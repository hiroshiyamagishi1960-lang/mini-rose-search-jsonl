# app.py — 起動安定化（/health&/ready）＋遅延ロード＋速度/順位の安定化 v6.0
# 目的：
#  1) /health（Liveness）と /ready（Readiness）を分離：Render が必ず外部ルートを開く
#  2) KB 読込を「起動時の重い同期処理」から「バックグラウンド遅延ロード」に変更（fail-open）
#  3) 検索速度と順位の安定化：
#     - 代表日：**「開催日/発行日」だけ**を採用 → date_primary にISOで付与
#     - 並び：latest固定 = date_primary↓ → score↓ → doc_id↑
#     - 前処理一回化（正規化・かなフォールドをロード時に前計算）、本文は先頭N文字だけをスコア対象
#     - 軽量ファジーの負荷制御（語長≥2、各語1回まで）
#
# API互換：
#   GET /api/search?q=...&page=1&page_size=5&order=(relevance|latest)&refresh=0
#   応答：{ items:[{title,content,url,rank,date,date_primary}], total_hits, page, page_size, has_more, next_page, error, order_used }
#   ※ date_primary を新規追加（UIは非依存のため互換維持）
#
# 注意：
#  - 起動は常に /health=200（生存） → 外部ルーティングは維持
#  - /ready は KB準備状況のみを返す（UIが準備中を案内可能）
#  - refresh=1 は非同期のバックグラウンド再ロード

import os, io, re, csv, json, hashlib, unicodedata, threading, time
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
VERSION   = os.getenv("APP_VERSION", "jsonl-2025-10-22-v6.0")
SYN_CSV   = (os.getenv("SYNONYM_CSV", "") or "").strip()  # 例: "./synonyms.csv"

# 速度チューニング（必要に応じて調整可）
TEXT_SCORE_LIMIT = int(os.getenv("TEXT_SCORE_LIMIT", "3000"))  # 本文の先頭N文字のみスコア対象
FUZZY_MIN_LEN    = 2                                          # 軽量ファジーの最短語長
CACHE_TTL_SEC    = int(os.getenv("CACHE_TTL_SEC", "90"))      # 同一クエリの短期キャッシュTTL

app = FastAPI(title="mini-rose-search-jsonl (ready-split + lazy-load + tuned)")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=False,
    allow_methods=["GET"], allow_headers=["*"],
)

# 全体状態
KB_LINES: int = 0
KB_HASH:  str = ""
LAST_ERROR: str = ""
LAST_EVENT: str = ""
_KB_ROWS: Optional[List[Dict[str, Any]]] = None  # オリジナル行（表示用の生テキスト保持）
_KB_IDX:  Optional[List[Dict[str, Any]]] = None  # 検索用の前処理済みインデックス
_READY: bool = False                               # /ready 用フラグ
_READY_DETAIL: str = "not_loaded"
_READY_AT: Optional[float] = None

# 簡易キャッシュ（同一クエリの結果）
_QUERY_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}

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
HIRA_SMALL2NORM = {"ぁ":"あ","ぃ":"い","ぅ":"う","ぇ":"え","ぉ":"お","ゃ":"や","ゅ":"ゆ","ょ":"よ","っ":"つ","ゎ":"わ"}
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

# ==================== 軽量ファジー ====================
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
    if n < FUZZY_MIN_LEN:  # 制御：短すぎる語は無効
        return term in text
    if m < n - 1:
        return False
    lo = max(1, n - 1); hi = n + 1
    for L in (n, lo, hi):
        if L <= 0 or L > m: continue
        # 各語1回で十分（負荷制御）
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
    if not path or not os.path.exists(path):
        return
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

# ==================== 代表日（「開催日/発行日」だけ） ====================
def record_date_primary(rec: Dict[str, Any]) -> Optional[datetime]:
    v = rec.get("開催日/発行日")
    if not v: return None
    s = normalize_text(textify(v))
    for fmt in ("%Y-%m-%d","%Y/%m/%d","%Y.%m.%d","%Y-%m","%Y/%m","%Y.%m","%Y"):
        try:
            return datetime.strptime(s[:len(fmt)], fmt)
        except Exception:
            continue
    m = re.match(r"^(\d{4})年(\d{1,2})月(\d{1,2})日?$", s)
    if m:
        try: return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except Exception: pass
    m = re.match(r"^(\d{4})年(\d{1,2})月$", s)
    if m:
        try: return datetime(int(m.group(1)), int(m.group(2)), 1)
        except Exception: pass
    m = re.match(r"^(\d{4})年$", s)
    if m:
        try: return datetime(int(m.group(1)), 1, 1)
        except Exception: pass
    return None

def date_to_iso(d: Optional[datetime]) -> Optional[str]:
    if not d: return None
    if d.day == 1 and d.hour == 0 and d.minute == 0 and d.second == 0:
        # 粗い日付だった場合でもISOとしては YYYY-MM-DD で返す
        return d.strftime("%Y-%m-%d")
    return d.strftime("%Y-%m-%d")

# ==================== URL正規化/ID ====================
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
        p = urlparse(u); p = p._replace(fragment="")
        qs_pairs = [(k, v) for (k, v) in parse_qsl(p.query, keep_blank_values=True) if k not in _DROP_QS]
        qs = "&".join([f"{k}={v}" if v != "" else k for k, v in qs_pairs])
        p = p._replace(query=qs)
        p = p._replace(scheme=(p.scheme or "").lower(), netloc=(p.netloc or "").lower())
        if "notion.site" in p.netloc:
            pid = _extract_notion_page_id(p.path)
            if pid: return f"notion://{pid}"
        return urlunparse(p)
    except Exception:
        return u

def stable_hash(*parts: str) -> str:
    h = hashlib.sha256()
    for part in parts:
        h.update((part or "").encode("utf-8")); h.update(b"\x1e")
    return h.hexdigest()

def doc_id_for(rec: Dict[str, Any]) -> str:
    rid = textify(rec.get("id") or rec.get("doc_id") or rec.get("record_id") or rec.get("ページID")).strip()
    if rid: return f"id://{rid}"
    url_c = canonical_url(textify(rec.get("url") or rec.get("URL") or rec.get("link") or rec.get("公開URL") or rec.get("出典URL")))
    if url_c: return f"url://{url_c}"
    title_n = normalize_text(textify(rec.get("title") or rec.get("Title") or rec.get("名前") or rec.get("題名") or rec.get("見出し") or rec.get("subject") or rec.get("headline")))
    date_n  = normalize_text(textify(rec.get("開催日/発行日") or ""))
    auth_n  = normalize_text(textify(rec.get("author") or rec.get("著者") or rec.get("講師") or ""))
    return f"hash://{stable_hash(title_n, date_n, auth_n)}"

# ==================== インデックス生成（起動時1回 or 遅延） ====================
TITLE_KEYS = ["title","Title","名前","タイトル","題名","見出し","subject","headline"]
TEXT_KEYS  = ["content","text","body","本文","内容","記事","description","summary","excerpt"]
URL_KEYS   = ["url","URL","link","permalink","出典URL","公開URL","source"]
AUTH_KEYS  = ["author","Author","writer","posted_by","著者","講師"]

def record_as_text(rec: Dict[str, Any], field: str) -> str:
    key_map = {"title": TITLE_KEYS, "text": TEXT_KEYS, "url": URL_KEYS, "author": AUTH_KEYS}
    keys = key_map.get(field, [field])
    for k in keys:
        v = rec.get(k)
        if v:
            return textify(v)
    return ""

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

def _build_index(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """検索専用の前処理済みインデックスを生成（本文先頭N文字のみ保持）。"""
    idx: List[Dict[str, Any]] = []
    for rec in rows:
        title_raw = record_as_text(rec, "title")
        text_raw  = record_as_text(rec, "text")
        url_raw   = record_as_text(rec, "url")
        d_primary = record_date_primary(rec)
        idx.append({
            "did": doc_id_for(rec),
            "title_raw": title_raw or "(無題)",
            "text_raw": text_raw or "",
            "url_raw": url_raw or "",
            "date": rec.get("開催日/発行日"),
            "date_primary": d_primary,                       # datetime
            # 検索用：正規化＆フォールド（本文は先頭N文字のみ）
            "title_norm": normalize_text(title_raw or ""),
            "text_norm": normalize_text((text_raw or "")[:TEXT_SCORE_LIMIT]),
            "title_fold": fold_kana(normalize_text(title_raw or "")),
            "text_fold": fold_kana(normalize_text((text_raw or "")[:TEXT_SCORE_LIMIT])),
        })
    return idx

def _download_kb_if_needed(url: str, path: str, timeout_sec: int = 5) -> Tuple[bool, str]:
    """外部から取得（任意）。失敗しても例外を投げず、ローカルを優先（fail-open）。"""
    if not url or requests is None:
        return False, "skip_download"
    try:
        r = requests.get(url, timeout=timeout_sec)
        r.raise_for_status()
        blob = _bytes_to_jsonl(r.content)
        tmp = path + ".tmp"
        os.makedirs(os.path.dirname(path), exist_ok=True) if os.path.dirname(path) else None
        with open(tmp, "wb") as wf: wf.write(blob)
        os.replace(tmp, path)
        return True, "downloaded"
    except Exception as e:
        return False, f"download_failed:{type(e).__name__}"

def _load_all():
    """バックグラウンドでKBと同義語を読み込み、前処理インデックスを構築。"""
    global _KB_ROWS, _KB_IDX, KB_LINES, KB_HASH, LAST_EVENT, LAST_ERROR, _READY, _READY_DETAIL, _READY_AT
    try:
        _READY = False; _READY_DETAIL = "loading"; _READY_AT = None
        # 外部KBがあれば短時間で試行（失敗しても続行）
        dl, ev = _download_kb_if_needed(KB_URL, KB_PATH, timeout_sec=5)
        LAST_EVENT = ev
        rows = _load_rows_into_memory(KB_PATH)
        KB_LINES, KB_HASH = _compute_lines_and_hash(KB_PATH) if os.path.exists(KB_PATH) else (0, "")
        _load_synonyms_from_csv(SYN_CSV)
        _KB_ROWS = rows
        _KB_IDX  = _build_index(rows)
        _READY = True; _READY_DETAIL = "ready"; _READY_AT = time.time()
        LAST_ERROR = ""
    except Exception as e:
        _KB_ROWS = []; _KB_IDX = []
        KB_LINES, KB_HASH = 0, ""
        LAST_ERROR = f"load_failed:{type(e).__name__}:{e}"
        _READY = False; _READY_DETAIL = "error"; _READY_AT = time.time()

def trigger_background_load():
    th = threading.Thread(target=_load_all, daemon=True)
    th.start()

# ==================== 起動時：重い処理はバックグラウンドへ ====================
@app.on_event("startup")
def _startup():
    # 起動直後は軽く：即座に /health=200 を返せる状態にしつつ、裏でロード
    trigger_background_load()

# ==================== ヘルス/レディ ====================
def json_utf8(payload: Dict[str, Any], status: int = 200) -> JSONResponse:
    return JSONResponse(payload, status_code=status, media_type="application/json; charset=utf-8",
                        headers={"Cache-Control":"no-store","Content-Type":"application/json; charset=utf-8"})

@app.get("/health")
def health():
    # Liveness 専用：常にOK（Renderのルーティングを塞がない）
    return json_utf8({"live": True, "kb_size": KB_LINES, "kb_fingerprint": KB_HASH, "last_event": LAST_EVENT, "last_error": LAST_ERROR})

@app.get("/ready")
def ready():
    return json_utf8({"ok": _READY, "detail": _READY_DETAIL, "kb_size": KB_LINES, "kb_fingerprint": KB_HASH, "at": _READY_AT})

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
    # 非同期でリロード（APIはブロックしない）
    trigger_background_load()
    return json_utf8({"ok": True, "message": "reload_started"})

# ==================== 検索ロジック ====================
def expand_with_synonyms(term: str) -> Set[str]:
    t = normalize_text(term)
    out: Set[str] = {t}
    for canon in _syn_variant2canon.get(t, set()):
        out.add(canon); out.update(_syn_canon2variant.get(canon, set()))
    if t in _syn_canon2variant:
        out.update(_syn_canon2variant[t])
    return out

def tokenize_query(q: str) -> List[str]:
    TOKEN_RE = re.compile(r'"([^"]+)"|(\S+)')
    out: List[str] = []
    for m in TOKEN_RE.finditer(normalize_text(q)):
        tok = m.group(1) if m.group(1) is not None else m.group(2)
        if tok: out.append(tok)
    return out

def _score_entry(entry: Dict[str, Any], tokens: List[str]) -> int:
    score = 0
    tit = entry["title_norm"]; txt = entry["text_norm"]
    ftit = entry["title_fold"]; ftxt = entry["text_fold"]
    for raw in tokens:
        if not raw: continue
        exts = expand_with_synonyms(raw) or {raw}
        used_fuzzy_title = False
        used_fuzzy_text  = False
        for t in exts:
            ft = fold_kana(normalize_text(t))
            # プレーン一致
            if t and tit.count(t) > 0: score += 3 * tit.count(t)
            if t and txt.count(t) > 0: score += 1 * txt.count(t)
            # フォールド一致
            if ft and ftit.count(ft) > 0: score += 3 * ftit.count(ft)
            if ft and ftxt.count(ft) > 0: score += 1 * ftxt.count(ft)
            # 軽量ファジー（語長≥2、各語1回）
            if len(t) >= FUZZY_MIN_LEN and not used_fuzzy_title and fuzzy_contains(ft, ftit):
                score += 1; used_fuzzy_title = True
            if len(t) >= FUZZY_MIN_LEN and not used_fuzzy_text and fuzzy_contains(ft, ftxt):
                score += 1; used_fuzzy_text = True
    return score

def sort_key_latest(sc:int, d:Optional[datetime], did:str) -> Tuple:
    # 完全順序：date_primary↓ → score↓ → doc_id↑
    date_key = d or datetime.min
    return (-int(date_key.strftime("%Y%m%d%H%M%S")), -int(sc), did)

def sort_key_relevance(sc:int, d:Optional[datetime], did:str) -> Tuple:
    date_key = d or datetime.min
    return (-int(sc), -int(date_key.strftime("%Y%m%d%H%M%S")), did)

def _slice_items(items: List[Tuple[int, Optional[datetime], str, Dict[str, Any]]], page:int, page_size:int):
    total = len(items); start = (page-1)*page_size; end = start+page_size
    page_slice = items[start:end]
    has_more = end < total; next_page = page+1 if has_more else None
    return total, page_slice, has_more, next_page, start, end

def html_escape(s: str) -> str:
    return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def highlight_simple(text: str, terms: List[str]) -> str:
    if not text: return ""
    esc = html_escape(text)
    hlset: Set[str] = set()
    for t in terms:
        tt = normalize_text(t)
        if tt: hlset.add(tt); hlset |= expand_with_synonyms(tt)
    for t in sorted(hlset, key=len, reverse=True):
        if not t: continue
        et = html_escape(t)
        esc = re.sub(re.escape(et), lambda m: f"<mark>{m.group(0)}</mark>", esc)
    return esc

def build_item(entry: Dict[str, Any], terms: List[str], is_first: bool) -> Dict[str, Any]:
    title = entry["title_raw"] or "(無題)"
    body  = entry["text_raw"] or ""
    if is_first:
        snippet_src = body[:300]
    else:
        # シンプル：最初の用語の周辺
        pos = -1; base = None
        for t in terms:
            t = normalize_text(t); 
            if t:
                p = body.find(t)
                if p >= 0: pos = p; base = t; break
        if pos < 0:
            snippet_src = body[:160]
        else:
            start = max(0, pos-80); end = min(len(body), pos+80)
            snippet_src = ("…" if start>0 else "") + body[start:end] + ("…" if end<len(body) else "")
    d = entry["date_primary"]
    return {
        "title":   highlight_simple(title, terms),
        "content": highlight_simple(snippet_src, terms),
        "url":     entry["url_raw"],
        "rank":    None,
        "date":    entry["date"],                      # 表示用（元のフィールド）
        "date_primary": date_to_iso(d) if d else None  # 並び用の代表日（新規）
    }

def _cache_key(q:str, page:int, page_size:int, order:str) -> str:
    return f"{q}||{page}||{page_size}||{order}"

def _get_cached(key:str) -> Optional[Dict[str, Any]]:
    now = time.time()
    hit = _QUERY_CACHE.get(key)
    if not hit: return None
    ts, payload = hit
    if now - ts > CACHE_TTL_SEC:
        _QUERY_CACHE.pop(key, None); return None
    return payload

def _set_cache(key:str, payload:Dict[str,Any]):
    _QUERY_CACHE[key] = (time.time(), payload)

# ==================== エンドポイント ====================
@app.get("/api/search")
def api_search(
    q: str = Query("", description="検索クエリ（末尾年/年範囲はUI仕様どおり。順位は latest 固定推奨）"),
    page: int = Query(1, ge=1),
    page_size: int = Query(5, ge=1, le=50),
    order: str = Query("latest", pattern="^(relevance|latest)$"),  # 既定：latest
    refresh: int = Query(0, description="1=KB/同義語をバックグラウンド再読込"),
):
    try:
        if refresh == 1:
            trigger_background_load()

        # 準備前でも検索は受け付ける（空返し）→ UIは準備中で案内可能
        if _KB_IDX is None or (_KB_IDX == [] and not _READY):
            return json_utf8({"items": [], "total_hits": 0, "page": page, "page_size": page_size,
                              "has_more": False, "next_page": None, "error": "not_ready", "order_used": order})

        # キャッシュ
        ck = _cache_key(q, page, page_size, order)
        cached = _get_cached(ck)
        if cached is not None:
            return json_utf8(cached)

        tokens = tokenize_query(q)
        if not tokens:
            payload = {"items": [], "total_hits": 0, "page": page, "page_size": page_size,
                       "has_more": False, "next_page": None, "error": None, "order_used": order}
            _set_cache(ck, payload); return json_utf8(payload)

        # スナップショット生成
        snap: List[Tuple[int, Optional[datetime], str, Dict[str, Any]]] = []
        for e in (_KB_IDX or []):
            sc = _score_entry(e, tokens)
            if sc <= 0: continue
            snap.append((sc, e["date_primary"], e["did"], e))

        if not snap:
            payload = {"items": [], "total_hits": 0, "page": page, "page_size": page_size,
                       "has_more": False, "next_page": None, "error": None, "order_used": order}
            _set_cache(ck, payload); return json_utf8(payload)

        # doc_id 単位で dedupe（best）
        best_by_id: Dict[str, Tuple[int, Optional[datetime], str, Dict[str, Any]]] = {}
        for sc, d, did, e in snap:
            prev = best_by_id.get(did)
            if prev is None:
                best_by_id[did] = (sc, d, did, e)
            else:
                psc, pd, _, _ = prev
                if (sc > psc) or (sc == psc and (d or datetime.min) > (pd or datetime.min)):
                    best_by_id[did] = (sc, d, did, e)
        deduped = list(best_by_id.values())

        # 並び（latest/relevance）
        if order == "latest":
            deduped.sort(key=lambda x: sort_key_latest(x[0], x[1], x[2]))
            order_used = "latest"
        else:
            deduped.sort(key=lambda x: sort_key_relevance(x[0], x[1], x[2]))
            order_used = "relevance"

        total, page_slice, has_more, next_page, start, end = _slice_items(deduped, page, page_size)

        # 表示加工（ページの5件だけ）
        items: List[Dict[str, Any]] = []
        for i, (_sc, _d, _did, e) in enumerate(page_slice):
            items.append(build_item(e, tokens, is_first=(i==0)))

        # ランク付与（全体順位）
        for idx, _ in enumerate(deduped, start=1):
            if start < idx <= end:
                items[idx - start - 1]["rank"] = idx

        payload = {
            "items": items,
            "total_hits": total,
            "page": page,
            "page_size": page_size,
            "has_more": has_more,
            "next_page": next_page,
            "error": None,
            "order_used": order_used,
        }
        _set_cache(ck, payload)
        return json_utf8(payload)

    except Exception as e:
        return json_utf8({"items": [], "total_hits": 0, "page": 1, "page_size": page_size,
                          "has_more": False, "next_page": None, "error": "exception", "message": textify(e)})

# ==================== ローカル実行 ====================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
@app.get("/dump")
def dump():
    import inspect
    src = inspect.getsourcefile(dump)
    with open(__file__, "r") as f:
        return PlainTextResponse(f.read())
