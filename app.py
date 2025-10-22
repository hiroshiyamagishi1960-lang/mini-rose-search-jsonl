# app.py — かなフォールディング＋軽量ファジー＋同義語CSV対応 / 日付ソート対応版 v5.0
# 目的：
#  1) 一般的な検索の“多層のゆれ吸収”のうち、辞書いらずで効く２点を内蔵
#     - かなフォールディング（カタカナ⇔ひらがな/小書き→標準/長音→母音/濁点・半濁点の吸収）
#     - 軽量ファジー（編集距離≤1 を加点）
#  2) ドメイン固有の同義語は CSV（canonical,variant）で外部管理（任意）
#     - 環境変数 SYNONYM_CSV でパス指定（未設定でも動作）
#  3) ソートは relevance と latest（開催日/発行日など日付降順）を選択可能
#     - UI側で order=latest を指定すれば「開催日/発行日」降順で並びます
#
# API互換：
#   GET /api/search?q=...&page=1&page_size=5&order=(relevance|latest)&refresh=0
#   応答：{ items: [{title, content, url, rank, date}], total_hits, page, page_size, has_more, next_page, error, order_used }
#
# 注意：
#  - 検索用データは不変。ハイライトは表示直前のみ（検索順位に影響しない）
#  - doc_id でページング前に dedupe（完全順序：score↓,date↓,doc_id↑ / または date↓,score↓,doc_id↑）

import os, io, re, csv, json, hashlib, unicodedata
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
VERSION   = os.getenv("APP_VERSION", "jsonl-2025-10-22-v5.0")
SYN_CSV   = (os.getenv("SYNONYM_CSV", "") or "").strip()  # 例: "./synonyms.csv"

app = FastAPI(title="mini-rose-search-jsonl (kana-fold + fuzzy + synonym-csv)")
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

# ==================== かなフォールディング（辞書不要で揺れ吸収） ====================
# 目的：カタ→ひら、小書き→標準、長音→母音、濁点/半濁点を吸収して比較強化
KATA_TO_HIRA = str.maketrans({chr(k): chr(k - 0x60) for k in range(ord("ァ"), ord("ン") + 1)})
HIRA_SMALL2NORM = {
    "ぁ":"あ","ぃ":"い","ぅ":"う","ぇ":"え","ぉ":"お",
    "ゃ":"や","ゅ":"ゆ","ょ":"よ","っ":"つ","ゎ":"わ",
}
DAKUTEN = "\u3099"; HANDAKUTEN = "\u309A"  # 結合文字（濁点・半濁点）
VOWELS = {"あ","い","う","え","お"}

def _strip_diacritics(hira: str) -> str:
    # 濁点・半濁点を除去（NFD→結合記号除去→NFC）
    nfkd = unicodedata.normalize("NFD", hira)
    no_marks = "".join(ch for ch in nfkd if ch not in (DAKUTEN, HANDAKUTEN))
    return unicodedata.normalize("NFC", no_marks)

def _long_vowel_to_vowel(hira: str) -> str:
    # 長音「ー」を直前の母音に変換（直前が母音でない場合はそのまま）
    out = []
    prev = ""
    for ch in hira:
        if ch == "ー" and prev in VOWELS:
            out.append(prev)
        else:
            out.append(ch)
            prev = ch
    return "".join(out)

def fold_kana(s: str) -> str:
    if not s: return ""
    t = _nfkc(s)
    # カタカナ→ひらがな
    t = t.translate(KATA_TO_HIRA)
    # 小書き→標準
    t = "".join(HIRA_SMALL2NORM.get(ch, ch) for ch in t)
    # 長音→母音
    t = _long_vowel_to_vowel(t)
    # 濁点/半濁点を吸収（除去）
    t = _strip_diacritics(t)
    return t

# ==================== 軽量ファジー（編集距離≤1） ====================
def _lev1_match(term: str, hay: str) -> bool:
    """編集距離<=1 を高速近似。長さ差>1ならFalse。"""
    if not term or not hay: return False
    n, m = len(term), len(hay)
    if abs(n - m) > 1:
        return False
    # 1) 同長：1文字まで相違OK
    if n == m:
        diff = 0
        for a, b in zip(term, hay):
            if a != b:
                diff += 1
                if diff > 1: return False
        return True
    # 2) 長さ差=1：片方に1文字挿入で一致
    # ポインタで1回だけスキップ許容
    if n > m: term, hay = hay, term; n, m = m, n
    i = j = diff = 0
    while i < n and j < m:
        if term[i] == hay[j]:
            i += 1; j += 1
        else:
            diff += 1
            if diff > 1: return False
            j += 1
    return True  # 末尾1文字差はOK

def fuzzy_contains(term: str, text: str) -> bool:
    """text 内に、term と編集距離<=1の部分が存在するか。"""
    if not term or not text: return False
    n, m = len(term), len(text)
    if n == 1:
        return term in text
    if m < n - 1:
        return False
    lo = max(1, n - 1); hi = n + 1
    for L in (n, lo, hi):
        if L <= 0: continue
        if L > m: continue
        for i in range(0, m - L + 1):
            if _lev1_match(term, text[i:i+L]):
                return True
    return False

# ==================== 同義語CSVの読み込み（任意） ====================
# 形式：canonical,variant
# 例:
# canonical,variant
# 苔,こけ
# 苔,コケ
# 剪定,せん定
# ...
_syn_variant2canon: Dict[str, Set[str]] = {}
_syn_canon2variant: Dict[str, Set[str]] = {}

def _load_synonyms_from_csv(path: str):
    global _syn_variant2canon, _syn_canon2variant
    _syn_variant2canon = {}; _syn_canon2variant = {}
    if not path or not os.path.exists(path):
        return
    try:
        with io.open(path, "r", encoding="utf-8") as f:
            rdr = csv.reader(f)
            header = next(rdr, None)
            for row in rdr:
                if len(row) < 2: continue
                canon = normalize_text(row[0])
                vari  = normalize_text(row[1])
                if not canon or not vari: continue
                _syn_canon2variant.setdefault(canon, set()).add(vari)
                _syn_variant2canon.setdefault(vari, set()).add(canon)
    except Exception:
        # CSVが壊れていても致命傷にはしない
        pass

# ==================== kb.jsonl 読み込み ====================
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
    cnt = 0
    sha = hashlib.sha256()
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

def ensure_kb() -> Tuple[int, str]:
    global LAST_ERROR, LAST_EVENT, _KB_ROWS
    LAST_ERROR = ""; LAST_EVENT = ""
    if KB_URL:
        if requests is None:
            LAST_ERROR = "requests unavailable"
        else:
            try:
                r = requests.get(KB_URL, timeout=30)
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
            # 同義語CSVも読み込む
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

def _refresh_kb_globals():
    global KB_LINES, KB_HASH
    lines, sha = ensure_kb()
    KB_LINES, KB_HASH = lines, sha
    return lines, sha

@app.on_event("startup")
def _startup():
    try:
        _refresh_kb_globals()
    except Exception as e:
        global KB_LINES, KB_HASH, LAST_ERROR
        KB_LINES, KB_HASH = 0, ""
        LAST_ERROR = f"startup_failed: {type(e).__name__}: {e}"

# ==================== フィールド抽出 ====================
TITLE_KEYS = ["title","Title","名前","タイトル","題名","見出し","subject","headline"]
TEXT_KEYS  = ["content","text","body","本文","内容","記事","description","summary","excerpt"]
# 「開催日/発行日」を最優先で拾うよう、キー順序を工夫
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
    if not u or u.lower() in {"notion","null","none","undefined"}:
        return ""
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
    if rid:
        return f"id://{rid}"
    url_c = canonical_url(record_as_text(rec, "url"))
    if url_c:
        return f"url://{url_c}"
    title_n = normalize_text(record_as_text(rec, "title"))
    date_n  = normalize_text(record_as_text(rec, "date"))
    auth_n  = normalize_text(record_as_text(rec, "author"))
    return f"hash://{stable_hash(title_n, date_n, auth_n)}"

# ==================== 年フィルタ（末尾年/年範囲のみ） ====================
RANGE_SEP = r"(?:-|–|—|~|〜|～|\.{2})"

def _parse_year_from_query(q_raw: str) -> Tuple[str, Optional[int], Optional[Tuple[int,int]]]:
    q = _nfkc(q_raw).strip()
    if not q: return "", None, None
    parts = q.replace("　"," ").split()
    last = parts[-1] if parts else ""
    # YYYY
    if re.fullmatch(r"(19|20|21)\d{2}", last):
        return (" ".join(parts[:-1]).strip(), int(last), None)
    # YYYY-YYYY
    m_rng = re.fullmatch(rf"((?:19|20|21)\d{{2}})\s*{RANGE_SEP}\s*((?:19|20|21)\d{{2}})", last)
    if m_rng:
        y1, y2 = int(m_rng.group(1)), int(m_rng.group(2))
        if y1 > y2: y1, y2 = y2, y1
        return (" ".join(parts[:-1]).strip(), None, (y1, y2))
    # 末尾連結 …YYYY
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

def record_date(rec: Dict[str, Any]) -> Optional[datetime]:
    # 日本語日付も拾う
    s_raw = None
    for k in DATE_KEYS:
        v = rec.get(k)
        if not v: continue
        s_raw = normalize_text(textify(v))
        # まずISO等
        for fmt in ("%Y-%m-%d","%Y/%m/%d","%Y.%m.%d","%Y-%m","%Y/%m","%Y.%m","%Y"):
            try:
                return datetime.strptime(s_raw[:len(fmt)], fmt)
            except Exception:
                continue
        # 日本語
        m = re.match(r"^(\d{4})年(\d{1,2})月(\d{1,2})日?$", s_raw)
        if m:
            try: return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            except Exception: pass
        m = re.match(r"^(\d{4})年(\d{1,2})月$", s_raw)
        if m:
            try: return datetime(int(m.group(1)), int(m.group(2)), 1)
            except Exception: pass
        m = re.match(r"^(\d{4})年$", s_raw)
        if m:
            try: return datetime(int(m.group(1)), 1, 1)
            except Exception: pass
    return None

def _matches_year(rec: Dict[str, Any], year: Optional[int], yr: Optional[Tuple[int,int]]) -> bool:
    if year is None and yr is None: return True
    ys = _record_years(rec)
    if not ys: return False
    if year is not None: return year in ys
    lo, hi = yr
    return any(lo <= y <= hi for y in ys)

# ==================== 同義語展開ヘルパ ====================
def expand_with_synonyms(term: str) -> Set[str]:
    """term に対して、CSVで与えられた同義語をふくめた候補集合を返す。"""
    t = normalize_text(term)
    out: Set[str] = {t}
    # variant→canon
    for canon in _syn_variant2canon.get(t, set()):
        out.add(canon)
        out.update(_syn_canon2variant.get(canon, set()))
    # 直接canonだった場合
    if t in _syn_canon2variant:
        out.update(_syn_canon2variant[t])
    return out

# ==================== 検索（スコア：プレーン＋フォールド＋ファジー） ====================
def _score_record(rec: Dict[str, Any], tokens: List[str]) -> int:
    """
    最小だが実用的なスコア：
      - タイトル一致：3点/ヒット
      - 本文一致    ：1点/ヒット
      - ひらがなフォールド一致：同上加点
      - 軽量ファジー（編集距離≤1）一致：0.5点相当（整数化のため +1 だが重みを抑える）
    """
    title = normalize_text(record_as_text(rec, "title"))
    text  = normalize_text(record_as_text(rec, "text"))
    ftit  = fold_kana(title)
    ftxt  = fold_kana(text)

    score = 0
    for raw in tokens:
        if not raw: continue
        # 同義語展開
        exts = expand_with_synonyms(raw) or {raw}
        for t in exts:
            ft = fold_kana(t)
            # プレーン一致
            if t and title.count(t) > 0: score += 3 * title.count(t)
            if t and text.count(t)  > 0: score += 1 * text.count(t)
            # フォールド一致（ひらがな化等）
            if ft and ftit.count(ft) > 0: score += 3 * ftit.count(ft)
            if ft and ftxt.count(ft) > 0: score += 1 * ftxt.count(ft)
            # 低コストファジー（編集距離<=1）
            # タイトル
            if len(t) >= 2 and fuzzy_contains(fold_kana(t), ftit):
                score += 1  # 0.5点相当（整数で表現）
            # 本文
            if len(t) >= 2 and fuzzy_contains(fold_kana(t), ftxt):
                score += 1
    return score

# ==================== ハイライト（表示専用） ====================
def html_escape(s: str) -> str:
    return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def highlight_simple(text: str, terms: List[str]) -> str:
    if not text: return ""
    esc = html_escape(text)
    # オリジナル語＋同義語をハイライト（見栄え目的。フォールディングまではやり過ぎない）
    hlset: Set[str] = set()
    for t in terms:
        hlset.add(normalize_text(t))
        hlset |= expand_with_synonyms(t)
    # 長い語から置換
    for t in sorted(hlset, key=len, reverse=True):
        if not t: continue
        et = html_escape(t)
        esc = re.sub(re.escape(et), lambda m: f"<mark>{m.group(0)}</mark>", esc)
    return esc

def build_item(rec: Dict[str, Any], terms: List[str], is_first_in_page: bool) -> Dict[str, Any]:
    title = record_as_text(rec, "title") or "(無題)"
    body  = record_as_text(rec, "text") or ""
    # スニペット
    if is_first_in_page:
        snippet_src = body[:300]
    else:
        # 最初のヒット近傍（簡易）：最初の語で探す
        pos = -1
        base = None
        for t in terms:
            t = normalize_text(t)
            if t:
                p = body.find(t)
                if p >= 0:
                    pos = p; base = t; break
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
        "date":    record_as_text(rec, "date"),  # 「開催日/発行日」を優先抽出済み
    }

# ==================== クエリ処理 ====================
def tokenize_query(q: str) -> List[str]:
    # シンプル：ダブルクオート内はそのまま、それ以外は空白で分割
    TOKEN_RE = re.compile(r'"([^"]+)"|(\S+)')
    out: List[str] = []
    for m in TOKEN_RE.finditer(normalize_text(q)):
        tok = m.group(1) if m.group(1) is not None else m.group(2)
        if tok:
            out.append(tok)
    return out

# ==================== 並び（完全順序） ====================
def sort_key_relevance(entry: Tuple[int, Optional[datetime], str, Dict[str, Any]]) -> Tuple:
    score, d, did, _ = entry
    date_key = d or datetime.min
    return (-int(score), -int(date_key.strftime("%Y%m%d%H%M%S")), did)

def sort_key_latest(entry: Tuple[int, Optional[datetime], str, Dict[str, Any]]) -> Tuple:
    score, d, did, _ = entry
    date_key = d or datetime.min
    return (-int(date_key.strftime("%Y%m%d%H%M%S")), -int(score), did)

# ==================== 共通レスポンスヘルパ ====================
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
    lines, sha = _refresh_kb_globals()
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
            _refresh_kb_globals()

        if not os.path.exists(KB_PATH) or KB_LINES <= 0:
            return json_utf8({"items": [], "total_hits": 0, "page": page, "page_size": page_size,
                              "has_more": False, "next_page": None, "error": "kb_missing", "order_used": order})

        # クエリ解析（年尾抽出）
        base_q, y_tail, yr_tail = _parse_year_from_query(q)
        tokens = tokenize_query(base_q)
        if not tokens:
            return json_utf8({"items": [], "total_hits": 0, "page": page, "page_size": page_size,
                              "has_more": False, "next_page": None, "error": None, "order_used": order})

        # スナップショット（同一クエリ内の母集団固定）
        snapshot: List[Tuple[int, Optional[datetime], str, Dict[str, Any]]] = []
        for rec in (_KB_ROWS or []):
            if y_tail is not None or yr_tail is not None:
                if not _matches_year(rec, y_tail, yr_tail):
                    continue
            sc = _score_record(rec, tokens)
            if sc <= 0:
                continue
            d = record_date(rec)
            did = doc_id_for(rec)
            snapshot.append((sc, d, did, rec))

        if not snapshot:
            return json_utf8({"items": [], "total_hits": 0, "page": page, "page_size": page_size,
                              "has_more": False, "next_page": None, "error": None, "order_used": order})

        # ページング前 dedupe（doc_id単位で最良1件）
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

        # 決定的ソート
        if order == "latest":
            deduped.sort(key=sort_key_latest)
            order_used = "latest"
        else:
            deduped.sort(key=sort_key_relevance)
            order_used = "relevance"

        total = len(deduped)

        # ページ切り出し
        start = (page - 1) * page_size
        end   = start + page_size
        page_slice = deduped[start:end]
        has_more = end < total
        next_page = page + 1 if has_more else None

        # 表示加工（最後）
        items: List[Dict[str, Any]] = []
        for i, (_sc, _d, _did, rec) in enumerate(page_slice):
            items.append(build_item(rec, tokens, is_first_in_page=(i == 0)))

        # ランク（全体順位）
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
