# app.py — slim版（UI/環境変数は維持｜検索ロジックだけシンプル版に刷新）
# 方針：
#  - 既存UI・PWA・環境変数・起動ゲートは v5.3.3 と同等に維持
#  - /api/search だけ全面作り直し
#  - 並び順は常に「日付優先（新しい順）＋同じ日付の中だけスコア順」
#  - 年フィルタは検索語の末尾（2025 / 2023-2025）だけを使い、record_date.year にだけ適用
#  - タイトルや本文に書かれた年（9999 など）はフィルタにもソートにも一切使わない

import os, io, re, json, hashlib, unicodedata, threading, tempfile
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional, Set
from urllib.parse import urlparse, urlunparse, parse_qsl
from collections import OrderedDict

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse, PlainTextResponse, FileResponse, HTMLResponse, RedirectResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

try:
    import requests
except Exception:
    requests = None

# ====== 設定（環境変数は既存どおり使用） ======
KB_URL    = (os.getenv("KB_URL", "") or "").strip()
KB_PATH   = os.path.normpath((os.getenv("KB_PATH", "kb.jsonl") or "kb.jsonl").strip())
VERSION   = os.getenv("APP_VERSION", "jsonl-2025-11-18-slim-search-v1")

CACHE_SIZE = int(os.getenv("CACHE_SIZE", "128"))
PREVIEW_LIMIT = 120000  # かなフォールドに使う本文の最大長

app = FastAPI(title="mini-rose-search-jsonl (slim-search-v1)")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=False,
    allow_methods=["GET"], allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory="static"), name="static")

# ====== KB 状態（実読込件数で判断） ======
KB_LINES: int = 0         # 行数（参考）
KB_HASH:  str = ""        # 生ファイルのSHA256
LAST_ERROR: str = ""
LAST_EVENT: str = ""
_KB_ROWS: Optional[List[Dict[str, Any]]] = None

# ====== 文字整形・かなフォールド ======
def _nfkc(s: Optional[str]) -> str:
    return unicodedata.normalize("NFKC", s or "")

def normalize_text(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("\u3000", " ")
    s = re.sub(r"[\r\n\t]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def textify(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, str):
        return x
    try:
        return json.dumps(x, ensure_ascii=False)
    except Exception:
        return str(x)

KATA_TO_HIRA = str.maketrans({chr(k): chr(k - 0x60) for k in range(ord("ァ"), ord("ン") + 1)})
HIRA_SMALL2NORM = {"ぁ":"あ","ぃ":"い","ぅ":"う","ぇ":"え","ぉ":"お","ゃ":"や","ゅ":"ゆ","ょ":"よ","っ":"つ","ゎ":"わ"}
DAKUTEN = "\u3099"
HANDAKUTEN = "\u309A"
VOWELS = {"あ","い","う","え","お"}

def _strip_diacritics(hira: str) -> str:
    nfkd = unicodedata.normalize("NFD", hira)
    no_marks = "".join(ch for ch in nfkd if ch not in (DAKUTEN, HANDAKUTEN))
    return unicodedata.normalize("NFC", no_marks)

def _long_vowel_to_vowel(hira: str) -> str:
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
    if not s:
        return ""
    t = _nfkc(s)
    t = t.translate(KATA_TO_HIRA)
    t = "".join(HIRA_SMALL2NORM.get(ch, ch) for ch in t)
    t = _long_vowel_to_vowel(t)
    t = _strip_diacritics(t)
    return t

# ====== レコード→文字列抽出（キー候補は既存踏襲） ======
TITLE_KEYS = ["title","Title","名前","タイトル","題名","見出し","subject","headline"]
TEXT_KEYS  = ["content","text","body","本文","内容","記事","description","summary","excerpt"]
DATE_KEYS  = ["開催日/発行日","date","Date","published_at","published","created_at","更新日","作成日","日付","開催日","発行日"]
URL_KEYS   = ["url","URL","link","permalink","出典URL","公開URL","source"]
ID_KEYS    = ["id","doc_id","record_id","ページID"]
AUTH_KEYS  = ["author","Author","writer","posted_by","著者","講師"]
TAG_KEYS   = ["tags","tag","タグ","区分","分類","カテゴリ","category","categories","keywords","キーワード"]

def record_as_text(rec: Dict[str, Any], field: str) -> str:
    key_map = {"title": TITLE_KEYS, "text": TEXT_KEYS, "date": DATE_KEYS,
               "url": URL_KEYS, "id": ID_KEYS, "author": AUTH_KEYS}
    keys = key_map.get(field, [field])
    for k in keys:
        v = rec.get(k)
        if v:
            return textify(v)
    return ""

def record_as_tags(rec: Dict[str, Any]) -> str:
    for k in TAG_KEYS:
        if k in rec and rec[k]:
            return textify(rec[k])
    return ""

# ====== URL正規化・ID ======
_DROP_QS = {"source","utm_source","utm_medium","utm_campaign","utm_term","utm_content"}

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
        return urlunparse(p)
    except Exception:
        return u

def stable_hash(*parts: str) -> str:
    h = hashlib.sha256()
    for part in parts:
        h.update((part or "").encode("utf-8"))
        h.update(b"\x1e")
    return h.hexdigest()

def doc_id_for(rec: Dict[str, Any]) -> str:
    rid = (record_as_text(rec, "id") or "").strip()
    if rid:
        return f"id://{rid}"
    url_c = canonical_url(record_as_text(rec, "url"))
    if url_c:
        return f"url://{url_c}"
    title_n = normalize_text(record_as_text(rec, "title"))
    date_n  = normalize_text(record_as_text(rec, "date"))
    auth_n  = normalize_text(record_as_text(rec, "author"))
    return f"hash://{stable_hash(title_n, date_n, auth_n)}"

# ====== 日付抽出（record_date は「日付列だけ」から決める） ======
_DATE_RE = re.compile(
    r"(?P<y>(?:19|20|21)\d{2})[./\-年]?(?:(?P<m>0?[1-9]|1[0-2])[./\-月]?(?:(?P<d>0?[1-9]|[12]\d|3[01])日?)?)?",
    re.UNICODE,
)
_ERA_RE  = re.compile(r"(令和|平成|昭和)\s*(\d{1,2})\s*年(?:\s*(\d{1,2})\s*月(?:\s*(\d{1,2})\s*日)?)?")

def _era_to_seireki(era: str, nen: int) -> int:
    base = {"令和":2018, "平成":1988, "昭和":1925}.get(era, None)
    return base + nen if base is not None else nen

def _first_valid_date_from_string(s: str) -> Optional[datetime]:
    if not s:
        return None
    t = _nfkc(s)

    m = _ERA_RE.search(t)
    if m:
        try:
            y = _era_to_seireki(m.group(1), int(m.group(2)))
            mm = int(m.group(3)) if m.group(3) else 1
            dd = int(m.group(4)) if m.group(4) else 1
            return datetime(y, mm, dd)
        except Exception:
            pass

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
    """
    日付列（DATE_KEYS）だけから日付を推定。
    タイトル・本文・URL 内の年は一切使わない。
    """
    for k in DATE_KEYS:
        v = rec.get(k)
        if not v:
            continue
        dt = _first_valid_date_from_string(textify(v))
        if dt:
            return dt
    return None

# ====== KB 読込/取得・診断 ======
def _bytes_to_jsonl(blob: bytes) -> bytes:
    if not blob:
        return b""
    s = blob.decode("utf-8", errors="replace").strip()
    if not s:
        return b""
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
            if line.strip():
                cnt += 1
    return cnt, sha.hexdigest()

def _load_rows_into_memory(path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not os.path.exists(path):
        return rows
    with io.open(path, "r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln:
                continue
            try:
                rows.append(json.loads(ln))
            except Exception:
                continue
    return rows

def _attach_precomputed_fields(rows: List[Dict[str, Any]]):
    for rec in rows:
        title = record_as_text(rec, "title") or ""
        text  = record_as_text(rec, "text") or ""
        tags  = record_as_tags(rec)

        ttl_norm = normalize_text(title)
        txt_norm = normalize_text(text)
        tag_norm = normalize_text(tags)

        rec["__ttl_norm"] = ttl_norm
        rec["__txt_norm"] = txt_norm
        rec["__tag_norm"] = tag_norm

        rec["__ttl_fold"] = fold_kana(ttl_norm) if ttl_norm else ""
        txt_for_fold = txt_norm[:PREVIEW_LIMIT]
        rec["__txt_fold"] = fold_kana(txt_for_fold) if txt_for_fold else ""
        rec["__tag_fold"] = fold_kana(tag_norm) if tag_norm else ""

        rec["__doc_id"]   = doc_id_for(rec)
        rec["__date_obj"] = record_date(rec)

def _fetch_and_save_kb(url: str, dst: str) -> Tuple[bool, str]:
    if not url or requests is None:
        return False, "no_url_or_requests"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        blob = _bytes_to_jsonl(r.content)
        tmp = dst + ".tmp"
        if os.path.dirname(dst):
            os.makedirs(os.path.dirname(dst), exist_ok=True)
        with open(tmp, "wb") as wf:
            wf.write(blob)
        os.replace(tmp, dst)
        return True, "fetched"
    except Exception as e:
        return False, f"fetch_failed:{type(e).__name__}:{e}"

def ensure_kb(fetch_now: bool = False) -> Tuple[int, str]:
    """kb.jsonlを確保→行数/ハッシュを返す（_KB_ROWSへロード）。"""
    global LAST_ERROR, LAST_EVENT, _KB_ROWS
    LAST_ERROR = ""
    LAST_EVENT = ""
    if (not os.path.exists(KB_PATH) or os.path.getsize(KB_PATH) == 0) and fetch_now:
        ok, ev = _fetch_and_save_kb(KB_URL, KB_PATH)
        LAST_EVENT = ev if ok else ""
        LAST_ERROR = "" if ok else ev

    try:
        if os.path.exists(KB_PATH) and os.path.getsize(KB_PATH) > 0:
            lines, sha = _compute_lines_and_hash(KB_PATH)
            rows = _load_rows_into_memory(KB_PATH)
            _attach_precomputed_fields(rows)
            _KB_ROWS = rows
            return lines, sha
        else:
            _KB_ROWS = []
            return 0, ""
    except Exception as e:
        LAST_ERROR = f"load_failed:{type(e).__name__}:{e}"
        _KB_ROWS = []
        return 0, ""

def _refresh_kb_globals(fetch_now: bool = False):
    global KB_LINES, KB_HASH
    lines, sha = ensure_kb(fetch_now=fetch_now)
    KB_LINES, KB_HASH = lines, sha
    _cache.clear()
    return lines, sha

def _bg_fetch_kb():
    try:
        _refresh_kb_globals(fetch_now=True)
    except Exception:
        pass

# ====== 起動ゲート：KB未読込なら起動を失敗させる ======
@app.on_event("startup")
def _startup():
    # 1) まずファイル同梱のkb.jsonlを読み込む
    _refresh_kb_globals(fetch_now=False)
    # 2) 無ければバックグラウンドで取得を試す
    if not _KB_ROWS and KB_URL and requests is not None:
        th = threading.Thread(target=_bg_fetch_kb, daemon=True)
        th.start()
    # 3) 最終チェック：少し待っても空なら起動失敗 → 旧安定版維持
    for _ in range(10):
        if _KB_ROWS:
            break
        threading.Event().wait(0.2)
    if not _KB_ROWS:
        raise RuntimeError("KB not loaded at startup; keep previous stable deployment.")

# ====== LRU キャッシュ ======
class LRU:
    def __init__(self, cap: int):
        self.cap = cap
        self._d: OrderedDict[Tuple, Dict[str, Any]] = OrderedDict()
        self._ver: str = ""
    def clear(self):
        self._d.clear()
        self._ver = KB_HASH
    def get(self, key: Tuple):
        if self._ver != KB_HASH:
            self.clear()
            return None
        v = self._d.get(key)
        if v is None:
            return None
        self._d.move_to_end(key)
        return v
    def set(self, key: Tuple, val: Dict[str, Any]):
        if self._ver != KB_HASH:
            self.clear()
        self._d[key] = val
        self._d.move_to_end(key)
        if len(self._d) > self.cap:
            self._d.popitem(last=False)

_cache = LRU(CACHE_SIZE)

def json_utf8(payload: Dict[str, Any], status: int = 200) -> JSONResponse:
    return JSONResponse(
        payload,
        status_code=status,
        media_type="application/json; charset=utf-8",
        headers={"Cache-Control":"no-store","Content-Type":"application/json; charset=utf-8"},
    )

# ====== 便利：トップは /ui へ ======
@app.get("/")
def root_redirect():
    return RedirectResponse(url="/ui", status_code=302)

# ====== /health・/version（ブラウザ=HTML、機械=JSON／戻るリンク付き） ======
def _wants_html(request: Request) -> bool:
    q = request.query_params
    if q.get("view") in {"html", "1"} or q.get("html") == "1":
        return True
    accept = (request.headers.get("accept") or "").lower()
    return "text/html" in accept

def _html_page(title: str, inner: str) -> HTMLResponse:
    html = f"""<!doctype html><html lang="ja"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<style>
  :root{{--b:#e5e7eb;--txt:#0f172a}}
  body{{margin:0;padding:16px;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Noto Sans JP","Yu Gothic",Meiryo,sans-serif;color:var(--txt)}}
  .bar{{position:sticky;top:0;background:#fff;padding:10px 0;border-bottom:1px solid var(--b);margin-bottom:16px}}
  a.btn{{display:inline-block;padding:8px 12px;border:1px solid var(--b);border-radius:10px;text-decoration:none}}
  pre{{white-space:pre-wrap;word-break:break-word;background:#fafafa;border:1px solid var(--b);border-radius:8px;padding:12px}}
</style></head><body>
  <div class="bar"><a class="btn" href="/ui">← 検索画面に戻る</a></div>
  {inner}
</body></html>"""
    return HTMLResponse(html)

@app.get("/health")
def health(request: Request):
    payload = {
        "ok": bool(_KB_ROWS),
        "kb_url": KB_URL,
        "kb_size": KB_LINES,
        "rows_loaded": len(_KB_ROWS or []),
        "kb_fingerprint": KB_HASH,
        "last_event": LAST_EVENT,
        "last_error": LAST_ERROR,
    }
    if _wants_html(request):
        pretty = json.dumps(payload, ensure_ascii=False, indent=2)
        return _html_page("Health", f"<h1>Health</h1><pre>{pretty}</pre>")
    return json_utf8(payload)

@app.get("/version")
def version(request: Request):
    payload = {"version": VERSION}
    if _wants_html(request):
        pretty = json.dumps(payload, ensure_ascii=False, indent=2)
        return _html_page("Version", f"<h1>Version</h1><pre>{pretty}</pre>")
    return json_utf8(payload)

@app.get("/health/ui")
def health_ui_redirect():
    return RedirectResponse(url="/ui", status_code=307)

@app.get("/version/ui")
def version_ui_redirect():
    return RedirectResponse(url="/ui", status_code=307)

# ====== 管理：手動リフレッシュ ======
@app.get("/admin/refresh")
def admin_refresh():
    lines, sha = _refresh_kb_globals(fetch_now=True)
    ok = bool(_KB_ROWS)
    return json_utf8({
        "ok": ok, "kb_size": lines, "rows_loaded": len(_KB_ROWS or []),
        "kb_fingerprint": sha, "last_event": LAST_EVENT, "last_error": LAST_ERROR
    })

# ====== ハイライトとスニペット ======
def html_escape(s: str) -> str:
    return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def highlight_simple(text: str, terms: List[str]) -> str:
    if not text:
        return ""
    esc = html_escape(text)
    hlset: Set[str] = set(normalize_text(t) for t in terms if t)
    for t in sorted(hlset, key=len, reverse=True):
        if not t:
            continue
        et = html_escape(t)
        esc = re.sub(re.escape(et), lambda m: f"<mark>{m.group(0)}</mark>", esc)
    return esc

def build_item(
    rec: Dict[str, Any],
    terms: List[str],
    is_first_in_page: bool,
    matches: Optional[Dict[str, List[str]]] = None,
    hit_field: Optional[str] = None,
) -> Dict[str, Any]:
    """
    1件目: 300文字 + 途中で切れている場合は末尾に「…」
    2件目以降:
      - ヒット位置が分かる場合: 185文字ぶん前後を抜き出し、前/後が切れていれば「…」
      - ヒット位置が分からない場合: 先頭185文字、途中で切れていれば末尾に「…」
    """
    FIRST_SNIPPET_LEN = 300
    OTHER_SNIPPET_LEN = 185

    title = record_as_text(rec, "title") or "(無題)"
    body  = record_as_text(rec, "text") or ""

    if is_first_in_page:
        if len(body) <= FIRST_SNIPPET_LEN:
            snippet_src = body
        else:
            snippet_src = body[:FIRST_SNIPPET_LEN] + "…"
    else:
        pos = -1
        body_norm = normalize_text(body)
        for t in terms:
            t_norm = normalize_text(t)
            if not t_norm:
                continue
            p = body_norm.find(t_norm)
            if p >= 0:
                pos = p
                break

        if pos < 0:
            if len(body) <= OTHER_SNIPPET_LEN:
                snippet_src = body
            else:
                snippet_src = body[:OTHER_SNIPPET_LEN] + "…"
        else:
            half = OTHER_SNIPPET_LEN // 2
            start = max(0, pos - half)
            end = start + OTHER_SNIPPET_LEN
            if end > len(body):
                end = len(body)
                start = max(0, end - OTHER_SNIPPET_LEN)
            core = body[start:end]
            prefix = "…" if start > 0 else ""
            suffix = "…" if end < len(body) else ""
            snippet_src = prefix + core + suffix

    item: Dict[str, Any] = {
        "title":   highlight_simple(title, terms),
        "content": highlight_simple(snippet_src, terms),
        "url":     record_as_text(rec, "url"),
        "rank":    None,
        "date":    record_as_text(rec, "date"),
    }
    if hit_field:
        item["hit_field"] = hit_field
    if matches is not None:
        item["matches"] = matches
    return item

# ====== クエリ解析と年フィルタ ======
TOKEN_RE = re.compile(r'"([^"]+)"|(\S+)')

def parse_query(q: str) -> Tuple[List[str], List[str], List[str]]:
    must: List[str] = []
    minus: List[str] = []
    raw: List[str] = []
    for m in TOKEN_RE.finditer(normalize_text(q)):
        tok = m.group(1) if m.group(1) is not None else m.group(2)
        if not tok:
            continue
        raw.append(tok)
        if tok.startswith("-") and len(tok) > 1:
            minus.append(tok[1:])
        else:
            must.append(tok)
    return must, minus, raw

RANGE_SEP = r"(?:-|–|—|~|〜|～|\.{2})"

def parse_year_filter(q_raw: str) -> Tuple[str, Optional[int], Optional[Tuple[int,int]]]:
    """
    末尾トークンが「2025」や「2023-2025」のときだけ年フィルタとして扱う。
    それ以外（タイトル・本文内の年）は一切使わない。
    戻り値: (年トークンを除いたクエリ, 単一年, 年範囲)
    """
    q = _nfkc(q_raw).strip()
    if not q:
        return "", None, None
    parts = q.replace("　", " ").split()
    if not parts:
        return "", None, None
    last = parts[-1]

    # 単一年 "2025"
    if re.fullmatch(r"(19|20|21)\d{2}", last):
        base = " ".join(parts[:-1]).strip()
        return base, int(last), None

    # 範囲 "2023-2025"
    m_rng = re.fullmatch(rf"((?:19|20|21)\d{{2}})\s*{RANGE_SEP}\s*((?:19|20|21)\d{{2}})", last)
    if m_rng:
        y1, y2 = int(m_rng.group(1)), int(m_rng.group(2))
        if y1 > y2:
            y1, y2 = y2, y1
        base = " ".join(parts[:-1]).strip()
        return base, None, (y1, y2)

    # それ以外は年フィルタなし
    return q_raw, None, None

def record_year(rec: Dict[str, Any]) -> Optional[int]:
    dt = rec.get("__date_obj")
    if not isinstance(dt, datetime):
        return None
    return dt.year

def matches_year_filter(rec: Dict[str, Any], year: Optional[int], yrange: Optional[Tuple[int,int]]) -> bool:
    if year is None and yrange is None:
        return True
    y = record_year(rec)
    if y is None:
        return False
    if year is not None:
        return y == year
    lo, hi = yrange
    return lo <= y <= hi

# ====== 検索の中身（常に「日付優先＋同日だけスコア」） ======
def contains_term(rec: Dict[str, Any], term: str) -> bool:
    if not term:
        return False
    t_norm = normalize_text(term)
    if not t_norm:
        return False
    t_fold = fold_kana(t_norm)

    ttl = rec.get("__ttl_norm", "")
    txt = rec.get("__txt_norm", "")
    tag = rec.get("__tag_norm", "")
    ftt = rec.get("__ttl_fold", "")
    ftx = rec.get("__txt_fold", "")
    ftg = rec.get("__tag_fold", "")

    if t_norm in ttl or t_norm in txt or t_norm in tag:
        return True
    if t_fold and (t_fold in ftt or t_fold in ftx or t_fold in ftg):
        return True
    return False

def score_record(rec: Dict[str, Any], terms: List[str]) -> int:
    """
    シンプルスコア：
      タイトル一致 3点／タグ一致 2点／本文一致 1点 を合計。
      同じ日付の中だけ、このスコアで順位を決める。
    """
    if not terms:
        return 0
    ttl = rec.get("__ttl_norm", "")
    txt = rec.get("__txt_norm", "")
    tag = rec.get("__tag_norm", "")

    score = 0
    for raw in terms:
        t_norm = normalize_text(raw)
        if not t_norm:
            continue
        if ttl.count(t_norm) > 0:
            score += 3 * ttl.count(t_norm)
        if tag.count(t_norm) > 0:
            score += 2 * tag.count(t_norm)
        if txt.count(t_norm) > 0:
            score += 1 * txt.count(t_norm)
    return score

def decide_hit_field(rec: Dict[str, Any], terms: List[str]) -> str:
    if not terms:
        return ""
    ttl = rec.get("__ttl_norm", "")
    txt = rec.get("__txt_norm", "")
    tag = rec.get("__tag_norm", "")

    for raw in terms:
        t = normalize_text(raw)
        if not t:
            continue
        if t in ttl:
            return "title"
    for raw in terms:
        t = normalize_text(raw)
        if not t:
            continue
        if t in tag:
            return "tag"
    for raw in terms:
        t = normalize_text(raw)
        if not t:
            continue
        if t in txt:
            return "body"
    return ""

def calc_matches_for_debug(rec: Dict[str, Any], terms: List[str]) -> Dict[str, List[str]]:
    ttl = rec.get("__ttl_norm","")
    txt = rec.get("__txt_norm","")
    tag = rec.get("__tag_norm","")
    hit_ttl: List[str] = []
    hit_tag: List[str] = []
    hit_txt: List[str] = []
    for t in terms:
        n = normalize_text(t)
        if not n:
            continue
        if n in ttl:
            hit_ttl.append(n)
        if n in tag:
            hit_tag.append(n)
        if n in txt:
            hit_txt.append(n)
    out: Dict[str, List[str]] = {}
    if hit_ttl:
        out["title"] = hit_ttl
    if hit_tag:
        out["tags"] = hit_tag
    if hit_txt:
        out["body"] = hit_txt
    return out

# ====== /api/search ======
@app.get("/api/search")
def api_search(
    q: str = Query("", description="検索クエリ（-語=除外）"),
    page: int = Query(1, ge=1),
    page_size: int = Query(5, ge=1, le=50),
    order: str = Query("latest", description="互換のためのダミー。常に日付優先で並べる。"),
    refresh: int = Query(0, description="1=kb.jsonl を再取得・再読み込み"),
    logic: str = Query("and", pattern="^(and|or)$", description="and=両語必須（既定）/ or=どれか一致"),
    debug: int = Query(0, description="1で各件のヒット内訳を返す（診断用）"),
):
    try:
        if refresh == 1:
            _refresh_kb_globals(fetch_now=True)
            _cache.clear()

        if not _KB_ROWS:
            return json_utf8(
                {"items": [], "total_hits": 0, "error": "kb_not_loaded", "order_used": "latest"},
                status=503,
            )

        cache_key = (q, page, page_size, order, logic, debug)
        cached = _cache.get(cache_key)
        if cached is not None:
            return json_utf8(cached)

        # 1) 年フィルタ（クエリ末尾の 2025 / 2023-2025 のみ）を解釈
        base_q, year, yrange = parse_year_filter(q)

        # 2) 残りのクエリから必須語・除外語を抽出
        must_terms, minus_terms, raw_terms = parse_query(base_q)

        # 何も指定がない場合は空結果（全部出すことはしない）
        if not must_terms and not minus_terms and year is None and yrange is None:
            payload = {
                "items": [],
                "total_hits": 0,
                "page": page,
                "page_size": page_size,
                "has_more": False,
                "next_page": None,
                "error": None,
                "order_used": "latest",
            }
            _cache.set(cache_key, payload)
            return json_utf8(payload)

        rows = _KB_ROWS or []
        candidates: List[Tuple[Optional[datetime], int, str, Dict[str, Any]]] = []

        # 3) レコードをスキャンして、フィルタ＆スコア計算
        for rec in rows:
            # 年フィルタ（record_date.year のみ使用）
            if not matches_year_filter(rec, year, yrange):
                continue

            # 除外語（-盆景 など）
            excluded = False
            for t in minus_terms:
                if contains_term(rec, t):
                    excluded = True
                    break
            if excluded:
                continue

            # 必須語（AND / OR）
            if must_terms:
                if logic == "or":
                    ok = any(contains_term(rec, t) for t in must_terms)
                else:  # AND
                    ok = all(contains_term(rec, t) for t in must_terms)
                if not ok:
                    continue

            dt = rec.get("__date_obj")
            score = score_record(rec, must_terms or raw_terms)
            doc_id = rec.get("__doc_id") or doc_id_for(rec)
            candidates.append((dt, score, doc_id, rec))

        if not candidates:
            payload = {
                "items": [],
                "total_hits": 0,
                "page": page,
                "page_size": page_size,
                "has_more": False,
                "next_page": None,
                "error": None,
                "order_used": "latest",
            }
            _cache.set(cache_key, payload)
            return json_utf8(payload)

        # 4) 並び順：
        #    まず「日付の新しい順」、同じ日付の中だけスコアの高い順。
        #    日付が無いものは最も古い日付として扱い、最後に回す。
        def sort_key(entry: Tuple[Optional[datetime], int, str, Dict[str, Any]]):
            dt, score, doc_id, _ = entry
            dkey = dt or datetime.min
            return (dkey, score, doc_id)

        candidates.sort(key=sort_key, reverse=True)

        total = len(candidates)
        start = (page - 1) * page_size
        end   = start + page_size
        page_slice = candidates[start:end]
        has_more = end < total
        next_page = page + 1 if has_more else None

        # 5) 出力用 items を構築
        terms_for_view = must_terms or raw_terms
        items: List[Dict[str, Any]] = []
        for i, (dt, score, doc_id, rec) in enumerate(page_slice):
            hf = decide_hit_field(rec, terms_for_view) or "body"
            m  = None if debug != 1 else calc_matches_for_debug(rec, terms_for_view)
            item = build_item(rec, terms_for_view, is_first_in_page=(i == 0), matches=m, hit_field=hf)
            items.append(item)

        # rank 付与（全体順位）
        for idx, _ in enumerate(candidates, start=1):
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
            "order_used": "latest",
        }
        _cache.set(cache_key, payload)
        return json_utf8(payload)

    except Exception as e:
        return json_utf8(
            {
                "items": [],
                "total_hits": 0,
                "page": 1,
                "page_size": page_size,
                "has_more": False,
                "next_page": None,
                "error": "exception",
                "message": textify(e),
            },
            status=500,
        )

# ====== Service Worker（単一路で配信） ======
@app.get("/service-worker.js")
def get_sw():
    sw_path = os.path.join("static", "service-worker.js")
    if not os.path.exists(sw_path):
        return Response("service-worker.js not found under /static/", status_code=404)
    return FileResponse(
        sw_path,
        media_type="application/javascript; charset=utf-8",
        headers={"Cache-Control": "no-cache, must-revalidate", "Service-Worker-Allowed": "/"},
    )

# ====== UIエンド ======
@app.get("/ui")
def ui():
    path = os.path.join("static", "ui.html")
    if os.path.exists(path):
        return FileResponse(path, media_type="text/html; charset=utf-8")
    return PlainTextResponse("static/ui.html not found", status_code=404)

# ====== エントリポイント ======
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
