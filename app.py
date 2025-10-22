# app.py — かなフォールディング＋軽量ファジー＋同義語CSV対応 / 日付ソート対応版 v5.1
# 変更概要（最小）:
#  1) 日付抽出の強化：開催日/発行日から最初の有効日付を抽出（注記・括弧・和暦の一部に対応）。年/年月は1日に補完。
#  2) 年フィルタのソース優先度：開催日＞本文/タイトル＞URL（本文/タイトル/URLは最“新”年を採用）。
#  3) 起動時KB取得の非同期化：Render再開を阻害しない（前回kb.jsonlを優先採用、外部取得はバックグラウンド）。
#  4) レスポンス仕様と既存スコアロジックは変更なし（UIは order=latest 固定のまま）。

import os, io, re, csv, json, hashlib, unicodedata, threading
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional, Set
from urllib.parse import urlparse, urlunparse, parse_qsl

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, PlainTextResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware

try:
    import requests
except Exception:
    requests = None

# ==================== 基本設定 ====================
KB_URL    = (os.getenv("KB_URL", "") or "").strip()
KB_PATH   = os.path.normpath((os.getenv("KB_PATH", "kb.jsonl") or "kb.jsonl").strip())
VERSION   = os.getenv("APP_VERSION", "jsonl-2025-10-22-v5.1")
SYN_CSV   = (os.getenv("SYNONYM_CSV", "") or "").strip()  # 例: "./synonyms.csv"

app = FastAPI(title="mini-rose-search-jsonl (v5.1)")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=False,
    allow_methods=["GET"], allow_headers=["*"],
)

KB_LINES: int = 0
KB_HASH:  str = ""
LAST_ERROR: str = ""
LAST_EVENT: str = ""
_KB_ROWS: Optional[List[Dict[str, Any]]] = None  # メモリ常駐

# ==================== 正規化ユーティリティ ====================
def _nfkc(s: Optional[str]) -> str:
    return unicodedata.normalize("NFKC", s or "")

def normalize_text(s: str) -> str:
    if not s: return ""
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("\u3000", " ")
    s = re.sub(r"[\r\n\t]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def textify(x: Any) -> str:
    if x is None: return ""
    if isinstance(x, str): return x
    try:
        return json.dumps(x, ensure_ascii=False)
    except Exception:
        return str(x)

# ==================== かなフォールディング ====================
KATA_TO_HIRA = str.maketrans({chr(k): chr(k - 0x60) for k in range(ord("ァ"), ord("ン") + 1)})
HIRA_SMALL2NORM = {
    "ぁ":"あ","ぃ":"い","ぅ":"う","ぇ":"え","ぉ":"お",
    "ゃ":"や","ゅ":"ゆ","ょ":"よ","っ":"つ","ゎ":"わ",
}
DAKUTEN = "\u3099"; HANDAKUTEN = "\u309A"
VOWELS = {"あ","い","う","え","お"}

def _strip_diacritics(hira: str) -> str:
    nfkd = unicodedata.normalize("NFD", hira)
    no_marks = "".join(ch for ch in nfkd if ch not in (DAKUTEN, HANDAKUTEN))
    return unicodedata.normalize("NFC", no_marks)

def _long_vowel_to_vowel(hira: str) -> str:
    out = []; prev = ""
    for ch in hira:
        if ch == "ー" and prev in VOWELS:
            out.append(prev)
        else:
            out.append(ch); prev = ch
    return "".join(out)

def fold_kana(s: str) -> str:
    if not s: return ""
    t = _nfkc(s)
    t = t.translate(KATA_TO_HIRA)
    t = "".join(HIRA_SMALL2NORM.get(ch, ch) for ch in t)
    t = _long_vowel_to_vowel(t)
    t = _strip_diacritics(t)
    return t

# ==================== 軽量ファジー（編集距離≤1） ====================
def _lev1_match(term: str, hay: str) -> bool:
    if not term or not hay: return False
    n, m = len(term), len(hay)
    if abs(n - m) > 1: return False
    if n == m:
        diff = 0
        for a, b in zip(term, hay):
            if a != b:
                diff += 1
                if diff > 1: return False
        return True
    if n > m: term, hay = hay, term; n, m = m, n
    i = j = diff = 0
    while i < n and j < m:
        if term[i] == hay[j]:
            i += 1; j += 1
        else:
            diff += 1
            if diff > 1: return False
            j += 1
    return True

def fuzzy_contains(term: str, text: str) -> bool:
    if not term or not text: return False
    n, m = len(term), len(text)
    if n == 1: return term in text
    if m < n - 1: return False
    lo = max(1, n - 1); hi = n + 1
    for L in (n, lo, hi):
        if L <= 0 or L > m: continue
        for i in range(0, m - L + 1):
            if _lev1_match(term, text[i:i+L]):
                return True
    return False

# ==================== 同義語CSV ====================
_syn_variant2canon: Dict[str, Set[str]] = {}
_syn_canon2variant: Dict[str, Set[str]] = {}

def _load_synonyms_from_csv(path: str):
    global _syn_variant2canon, _syn_canon2variant
    _syn_variant2canon = {}; _syn_canon2variant = {}
    if not path or not os.path.exists(path): return
    try:
        with io.open(path, "r", encoding="utf-8") as f:
            rdr = csv.reader(f); header = next(rdr, None)
            for row in rdr:
                if len(row) < 2: continue
                canon = normalize_text(row[0]); vari = normalize_text(row[1])
                if not canon or not vari: continue
                _syn_canon2variant.setdefault(canon, set()).add(vari)
                _syn_variant2canon.setdefault(vari, set()).add(canon)
    except Exception:
        pass

# ==================== KB 読み込み ====================
def _bytes_to_jsonl(blob: bytes) -> bytes:
    if not blob: return b""
    s = blob.decode("utf-8", errors="replace").strip()
    if not s: return b""
    if s.startswith("["):
        try:
            data = json.loads(s)
            if isinstance(data, list):
                lines = [json.dumps(obj, ensure_ascii=False, separators=(",", ":")) for obj in data]
                return ("\n".join(lines) + "\n").encode("utf-8")
        except Exception:
            return blob
    return blob

def _compute_lines_and_hash(path: str) -> Tuple[int, str]:
    cnt = 0; sha = hashlib.sha256()
    with open(path, "rb") as f:
        for line in f:
            sha.update(line)
            if line.strip(): cnt += 1
    return cnt, sha.hexdigest()

def _load_rows_into_memory(path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not os.path.exists(path): return rows
    with io.open(path, "r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln: continue
            try:
                rows.append(json.loads(ln))
            except Exception:
                continue
    return rows

def ensure_kb(fetch_now: bool = False) -> Tuple[int, str]:
    """fetch_now=False のときは外部取得を行わずローカル優先で読み込む。"""
    global LAST_ERROR, LAST_EVENT, _KB_ROWS
    LAST_ERROR = ""; LAST_EVENT = ""
    if fetch_now and KB_URL and requests is not None:
        try:
            r = requests.get(KB_URL, timeout=5)  # 短めのタイムアウト
            r.raise_for_status()
            blob = _bytes_to_jsonl(r.content)
            tmp = KB_PATH + ".tmp"
            os.makedirs(os.path.dirname(KB_PATH), exist_ok=True) if os.path.dirname(KB_PATH) else None
            with open(tmp, "wb") as wf: wf.write(blob)
            os.replace(tmp, KB_PATH)
            LAST_EVENT = "fetched"
        except Exception as e:
            LAST_ERROR = f"fetch_or_save_failed: {type(e).__name__}: {e}"
    if os.path.exists(KB_PATH):
        try:
            lines, sha = _compute_lines_and_hash(KB_PATH)
            _KB_ROWS = _load_rows_into_memory(KB_PATH)
            _load_synonyms_from_csv(SYN_CSV)
            return lines, sha
        except Exception as e:
            LAST_ERROR = f"hash_failed: {type(e).__name__}: {e}"
            _KB_ROWS = []
            return 0, ""
    else:
        LAST_EVENT = LAST_EVENT or "no_file"
        _KB_ROWS = []
        return 0, ""

def _refresh_kb_globals(fetch_now: bool = False):
    global KB_LINES, KB_HASH
    lines, sha = ensure_kb(fetch_now=fetch_now)
    KB_LINES, KB_HASH = lines, sha
    return lines, sha

def _bg_fetch_kb():
    """バックグラウンドでKBを取得して更新（起動直後のブロック回避）。"""
    try:
        _refresh_kb_globals(fetch_now=True)
    except Exception as e:
        pass  # 失敗しても静かに継続

@app.on_event("startup")
def _startup():
    # 起動は即応答：まずローカルKBを採用（外部取得はしない）
    try:
        _refresh_kb_globals(fetch_now=False)
    except Exception as e:
        pass
    # 起動後に非同期で外部取得（成功時のみ上書き）
    if KB_URL and requests is not None:
        th = threading.Thread(target=_bg_fetch_kb, daemon=True)
        th.start()

# ==================== フィールド抽出 ====================
TITLE_KEYS = ["title","Title","名前","タイトル","題名","見出し","subject","headline"]
TEXT_KEYS  = ["content","text","body","本文","内容","記事","description","summary","excerpt"]
DATE_KEYS  = ["開催日/発行日","date","Date","published_at","published","created_at","更新日","作成日","日付","開催日","発行日"]
URL_KEYS   = ["url","URL","link","permalink","出典URL","公開URL","source"]
ID_KEYS    = ["id","doc_id","record_id","ページID"]
AUTH_KEYS  = ["author","Author","writer","posted_by","著者","講師"]

def record_as_text(rec: Dict[str, Any], field: str) -> str:
    key_map = {
        "title": TITLE_KEYS, "text": TEXT_KEYS, "date": DATE_KEYS,
        "url": URL_KEYS, "id": ID_KEYS, "author": AUTH_KEYS
    }
    keys = key_map.get(field, [field])
    for k in keys:
        v = rec.get(k)
        if v:
            return textify(v)
    return ""

# ==================== URL正規化 & doc_id ====================
_DROP_QS = {"source","utm_source","utm_medium","utm_campaign","utm_term","utm_content"}
_NOTION_PAGEID_RE = re.compile(r"[0-9a-f]{32}", re.IGNORECASE)

def _extract_notion_page_id(path: str) -> Optional[str]:
    m = _NOTION_PAGEID_RE.search(path.replace("-", ""))
    return m.group(0).lower() if m else None

def canonical_url(url: str) -> str:
    u = (url or "").strip()
    if not u or u.lower() in {"notion","null","none","undefined"}: return ""
    try:
        p = urlparse(u)
        p = p._replace(fragment="")
        qs_pairs = [(k, v) for (k, v) in parse_qsl(p.query, keep_blank_values=True) if k not in _DROP_QS]
        qs = "&".join([f"{k}={v}" if v != "" else k for k, v in qs_pairs])
        p = p._replace(query=qs)
        p = p._replace(scheme=(p.scheme or "").lower(), netloc=(p.netloc or "").lower())
        if "notion.site" in p.netloc:
            pid = _extract_notion_page_id(p.path)
            if pid:
                return f"notion://{pid}"
        return urlunparse(p)
    except Exception:
        return u

def stable_hash(*parts: str) -> str:
    h = hashlib.sha256()
    for part in parts:
        h.update((part or "").encode("utf-8")); h.update(b"\x1e")
    return h.hexdigest()

def doc_id_for(rec: Dict[str, Any]) -> str:
    rid = record_as_text(rec, "id").strip()
    if rid: return f"id://{rid}"
    url_c = canonical_url(record_as_text(rec, "url"))
    if url_c: return f"url://{url_c}"
    title_n = normalize_text(record_as_text(rec, "title"))
    date_n  = normalize_text(record_as_text(rec, "date"))
    auth_n  = normalize_text(record_as_text(rec, "author"))
    return f"hash://{stable_hash(title_n, date_n, auth_n)}"

# ==================== 日付抽出（強化版） ====================
_DATE_RE = re.compile(
    r"(?P<y>(?:19|20|21)\d{2})[./\-年]?(?:(?P<m>0?[1-9]|1[0-2])[./\-月]?(?:(?P<d>0?[1-9]|[12]\d|3[01])日?)?)?",
    re.UNICODE
)
# 和暦の極小対応（令和/平成/昭和 → 西暦換算、月日任意）
_ERA_RE = re.compile(r"(令和|平成|昭和)\s*(\d{1,2})\s*年(?:\s*(\d{1,2})\s*月(?:\s*(\d{1,2})\s*日)?)?")

def _era_to_seireki(era: str, nen: int) -> int:
    base = {"令和":2018, "平成":1988, "昭和":1925}.get(era, None)
    if base is None: raise ValueError
    return base + nen

def _first_valid_date_from_string(s: str) -> Optional[datetime]:
    """テキストから最初の“有効”日付を抽出。年/年月のみは 1日補完。注記・括弧は無視。"""
    if not s: return None
    t = _nfkc(s)
    # 和暦（簡易）
    m = _ERA_RE.search(t)
    if m:
        try:
            y = _era_to_seireki(m.group(1), int(m.group(2)))
            mm = int(m.group(3)) if m.group(3) else 1
            dd = int(m.group(4)) if m.group(4) else 1
            return datetime(y, mm, dd)
        except Exception:
            pass
    # 西暦（最初の一致）
    m2 = _DATE_RE.search(t)
    if m2:
        y = int(m2.group("y"))
        m = int(m2.group("m")) if m2.group("m") else 1
        d = int(m2.group("d")) if m2.group("d") else 1
        try:
            return datetime(y, m, d)
        except Exception:
            return None
    return None

def record_date(rec: Dict[str, Any]) -> Optional[datetime]:
    # 1) 開催日/発行日など（最優先）
    for k in DATE_KEYS:
        v = rec.get(k)
        if not v: continue
        dt = _first_valid_date_from_string(textify(v))
        if dt: return dt
    # 2) 本文・タイトルから最“新”年（補助）
    cand_year = None
    for field in ("text","title"):
        v = record_as_text(rec, field)
        for y in re.findall(r"(19\d{2}|20\d{2}|21\d{2})", _nfkc(v)):
            cand_year = max(int(y), cand_year or 0)
    if cand_year:
        return datetime(cand_year, 1, 1)
    # 3) URLから年（最終補助）
    u = record_as_text(rec, "url")
    if u:
        y_url = None
        for y in re.findall(r"(19\d{2}|20\d{2}|21\d{2})", _nfkc(u)):
            y_val = int(y)
            y_url = max(y_val, y_url or 0)
        if y_url:
            return datetime(y_url, 1, 1)
    return None

# ==================== 年フィルタ（末尾年/範囲） ====================
RANGE_SEP = r"(?:-|–|—|~|〜|～|\.{2})"

def _parse_year_from_query(q_raw: str) -> Tuple[str, Optional[int], Optional[Tuple[int,int]]]:
    q = _nfkc(q_raw).strip()
    if not q: return "", None, None
    parts = q.replace("　"," ").split()
    last = parts[-1] if parts else ""
    if re.fullmatch(r"(19|20|21)\d{2}", last):
        return (" ".join(parts[:-1]).strip(), int(last), None)
    m_rng = re.fullmatch(rf"((?:19|20|21)\d{{2}})\s*{RANGE_SEP}\s*((?:19|20|21)\d{{2}})", last)
    if m_rng:
        y1, y2 = int(m_rng.group(1)), int(m_rng.group(2))
        if y1 > y2: y1, y2 = y2, y1
        return (" ".join(parts[:-1]).strip(), None, (y1, y2))
    m_suf = re.fullmatch(rf"^(.*?)(?:((?:19|20|21)\d{{2}}))$", last)
    if m_suf and m_suf.group(1):
        return (" ".join(parts[:-1] + [m_suf.group(1)]).strip(), int(m_suf.group(2)), None)
    return (q, None, None)

def _record_years(rec: Dict[str, Any]) -> List[int]:
    ys = set()
    d = record_date(rec)
    if d: ys.add(d.year)
    for field in ("title","text","url","author"):
        v = record_as_text(rec, field)
        for y in re.findall(r"(19\d{2}|20\d{2}|21\d{2})", _nfkc(v)):
            ys.add(int(y))
    return sorted(ys)

def _matches_year(rec: Dict[str, Any], year: Optional[int], yr: Optional[Tuple[int,int]]) -> bool:
    if year is None and yr is None: return True
    ys = _record_years(rec)
    if not ys: return False
    if year is not None: return year in ys
    lo, hi = yr
    return any(lo <= y <= hi for y in ys)

# ==================== 同義語展開 ====================
def expand_with_synonyms(term: str) -> Set[str]:
    t = normalize_text(term)
    out: Set[str] = {t}
    for canon in _syn_variant2canon.get(t, set()):
        out.add(canon); out.update(_syn_canon2variant.get(canon, set()))
    if t in _syn_canon2variant:
        out.update(_syn_canon2variant[t])
    return out

# ==================== スコア・ハイライト ====================
def _score_record(rec: Dict[str, Any], tokens: List[str]) -> int:
    title = normalize_text(record_as_text(rec, "title"))
    text  = normalize_text(record_as_text(rec, "text"))
    ftit  = fold_kana(title); ftxt = fold_kana(text)
    score = 0
    for raw in tokens:
        if not raw: continue
        exts = expand_with_synonyms(raw) or {raw}
        for t in exts:
            ft = fold_kana(t)
            if t and title.count(t) > 0: score += 3 * title.count(t)
            if t and text.count(t)  > 0: score += 1 * text.count(t)
            if ft and ftit.count(ft) > 0: score += 3 * ftit.count(ft)
            if ft and ftxt.count(ft) > 0: score += 1 * ftxt.count(ft)
            if len(t) >= 2 and fuzzy_contains(ft, ftit): score += 1
            if len(t) >= 2 and fuzzy_contains(ft, ftxt): score += 1
    return score

def html_escape(s: str) -> str:
    return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def highlight_simple(text: str, terms: List[str]) -> str:
    if not text: return ""
    esc = html_escape(text)
    hlset: Set[str] = set()
    for t in terms:
        hlset.add(normalize_text(t))
        hlset |= expand_with_synonyms(t)
    for t in sorted(hlset, key=len, reverse=True):
        if not t: continue
        et = html_escape(t)
        esc = re.sub(re.escape(et), lambda m: f"<mark>{m.group(0)}</mark>", esc)
    return esc

def build_item(rec: Dict[str, Any], terms: List[str], is_first_in_page: bool) -> Dict[str, Any]:
    title = record_as_text(rec, "title") or "(無題)"
    body  = record_as_text(rec, "text") or ""
    if is_first_in_page:
        snippet_src = body[:300]
    else:
        pos = -1
        for t in terms:
            t = normalize_text(t)
            if not t: continue
            p = body.find(t)
            if p >= 0: pos = p; break
        if pos < 0:
            snippet_src = body[:160]
        else:
            start = max(0, pos - 80); end = min(len(body), pos + 80)
            snippet_src = ("…" if start>0 else "") + body[start:end] + ("…" if end<len(body) else "")
    return {
        "title":   highlight_simple(title, terms),
        "content": highlight_simple(snippet_src, terms),
        "url":     record_as_text(rec, "url"),
        "rank":    None,
        "date":    record_as_text(rec, "date"),
    }

# ==================== クエリ処理 ====================
def tokenize_query(q: str) -> List[str]:
    TOKEN_RE = re.compile(r'"([^"]+)"|(\S+)')
    out: List[str] = []
    for m in TOKEN_RE.finditer(normalize_text(q)):
        tok = m.group(1) if m.group(1) is not None else m.group(2)
        if tok: out.append(tok)
    return out

# ==================== 並び ====================
def sort_key_relevance(entry: Tuple[int, Optional[datetime], str, Dict[str, Any]]) -> Tuple:
    score, d, did, _ = entry
    date_key = d or datetime.min
    return (-int(score), -int(date_key.strftime("%Y%m%d%H%M%S")), did)

def sort_key_latest(entry: Tuple[int, Optional[datetime], str, Dict[str, Any]]) -> Tuple:
    score, d, did, _ = entry
    date_key = d or datetime.min
    return (-int(date_key.strftime("%Y%m%d%H%M%S")), -int(score), did)

# ==================== 共通レスポンス ====================
def json_utf8(payload: Dict[str, Any], status: int = 200) -> JSONResponse:
    return JSONResponse(
        payload,
        status_code=status,
        media_type="application/json; charset=utf-8",
        headers={"Cache-Control":"no-store","Content-Type":"application/json; charset=utf-8"},
    )

# ==================== エンドポイント ====================
@app.get("/health")
def health():
    ok = os.path.exists(KB_PATH) and KB_LINES > 0
    return json_utf8({"ok": ok, "kb_url": KB_URL, "kb_size": KB_LINES, "kb_fingerprint": KB_HASH})

@app.get("/version")
def version():
    return json_utf8({"version": VERSION})

@app.get("/ui")
def ui():
    path = os.path.join("static", "ui.html")
    if os.path.exists(path):
        return FileResponse(path, media_type="text/html; charset=utf-8")
    return PlainTextResponse("static/ui.html not found", status_code=404)

@app.get("/admin/refresh")
def admin_refresh():
    lines, sha = _refresh_kb_globals(fetch_now=True)
    return json_utf8({"ok": lines>0, "kb_size": lines, "kb_fingerprint": sha,
                      "last_event": LAST_EVENT, "last_error": LAST_ERROR})

@app.get("/api/search")
def api_search(
    q: str = Query("", description="検索クエリ（末尾年/年範囲はフィルタ可：例『コンテスト2024』『剪定 1999〜2001』）"),
    page: int = Query(1, ge=1),
    page_size: int = Query(5, ge=1, le=50),
    order: str = Query("relevance", pattern="^(relevance|latest)$"),
    refresh: int = Query(0, description="1=kb.jsonl / 同義語CSV を再取得・再読み込み"),
):
    try:
        if refresh == 1:
            _refresh_kb_globals(fetch_now=True)

        if not os.path.exists(KB_PATH) or KB_LINES <= 0:
            return json_utf8({"items": [], "total_hits": 0, "page": page, "page_size": page_size,
                              "has_more": False, "next_page": None, "error": "kb_missing", "order_used": order})

        base_q, y_tail, yr_tail = _parse_year_from_query(q)
        tokens = tokenize_query(base_q)
        if not tokens:
            return json_utf8({"items": [], "total_hits": 0, "page": page, "page_size": page_size,
                              "has_more": False, "next_page": None, "error": None, "order_used": order})

        snapshot: List[Tuple[int, Optional[datetime], str, Dict[str, Any]]] = []
        for rec in (_KB_ROWS or []):
            if y_tail is not None or yr_tail is not None:
                if not _matches_year(rec, y_tail, yr_tail):
                    continue
            sc = _score_record(rec, tokens)
            if sc <= 0: continue
            d = record_date(rec)
            did = doc_id_for(rec)
            snapshot.append((sc, d, did, rec))

        if not snapshot:
            return json_utf8({"items": [], "total_hits": 0, "page": page, "page_size": page_size,
                              "has_more": False, "next_page": None, "error": None, "order_used": order})

        # doc_idでdedupe（ベストのみ）
        best_by_id: Dict[str, Tuple[int, Optional[datetime], str, Dict[str, Any]]] = {}
        for entry in snapshot:
            sc, d, did, rec = entry
            prev = best_by_id.get(did)
            if prev is None:
                best_by_id[did] = entry
            else:
                psc, pd, _, _ = prev
                if (sc > psc) or (sc == psc and (d or datetime.min) > (pd or datetime.min)):
                    best_by_id[did] = entry
        deduped = list(best_by_id.values())

        if order == "latest":
            deduped.sort(key=sort_key_latest)
            order_used = "latest"
        else:
            deduped.sort(key=sort_key_relevance)
            order_used = "relevance"

        total = len(deduped)
        start = (page - 1) * page_size
        end   = start + page_size
        page_slice = deduped[start:end]
        has_more = end < total
        next_page = page + 1 if has_more else None

        items: List[Dict[str, Any]] = []
        for i, (_sc, _d, _did, rec) in enumerate(page_slice):
            items.append(build_item(rec, tokens, is_first_in_page=(i == 0)))

        for idx, _ in enumerate(deduped, start=1):
            if start < idx <= end:
                items[idx - start - 1]["rank"] = idx

        return json_utf8({
            "items": items,
            "total_hits": total,
            "page": page,
            "page_size": page_size,
            "has_more": has_more,
            "next_page": next_page,
            "error": None,
            "order_used": order_used,
        })

    except Exception as e:
        return json_utf8({"items": [], "total_hits": 0, "page": 1, "page_size": page_size,
                          "has_more": False, "next_page": None, "error": "exception", "message": textify(e)})

# ==================== ローカル実行 ====================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
