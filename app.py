# app.py — 一般的な検索ロジック準拠（決定的ソート＋ページング前dedupe＋表示加工は最後）版 v4.1
# 目的:
# - 「次へ」を押しても同じ文書が再出現しない
# - 特別な検索ロジックは使わない。一般の定石のみ：
#   ①スナップショット作成 → ②完全順序（score↓, date↓, doc_id↑）→ ③dedupe（doc_id）→ ④ページ切り出し → ⑤表示加工（ハイライト等）
# - 既存API（/api/search）の出力形式は維持。UI変更不要。

import os, io, re, json, hashlib, unicodedata
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional
from urllib.parse import urlparse, urlunparse, parse_qsl

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, PlainTextResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware

try:
    import requests
except Exception:
    requests = None

# ==================== 基本設定 ====================
KB_URL   = (os.getenv("KB_URL", "") or "").strip()
KB_PATH  = os.path.normpath((os.getenv("KB_PATH", "kb.jsonl") or "kb.jsonl").strip())
VERSION  = os.getenv("APP_VERSION", "jsonl-2025-10-21-v4.1")

app = FastAPI(title="mini-rose-search-jsonl (general-logic v4.1)")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=False,
    allow_methods=["GET"], allow_headers=["*"],
)

KB_LINES: int = 0
KB_HASH: str = ""
LAST_ERROR: str = ""
LAST_EVENT: str = ""
_KB_ROWS: Optional[List[Dict[str, Any]]] = None  # メモリ常駐

# ==================== 文字・正規化ユーティリティ ====================
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

# ==================== フィールド抽出（必要最低限） ====================
TITLE_KEYS = ["title","Title","名前","タイトル","題名","見出し","subject","headline"]
TEXT_KEYS  = ["content","text","body","本文","内容","記事","description","summary","excerpt"]
DATE_KEYS  = ["date","Date","published_at","published","created_at","更新日","作成日","日付","開催日","発行日"]
URL_KEYS   = ["url","URL","link","permalink","出典URL","公開URL","source"]
ID_KEYS    = ["id","doc_id","record_id","ページID"]

def record_as_text(rec: Dict[str, Any], field: str) -> str:
    key_map = {
        "title": TITLE_KEYS,
        "text":  TEXT_KEYS,
        "date":  DATE_KEYS,
        "url":   URL_KEYS,
        "id":    ID_KEYS,
        "author": ["author","Author","writer","posted_by","著者","講師"],
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
    # タイトル等に年が含まれている場合も拾う（簡易）
    for field in ("title","text","url","author"):
        v = record_as_text(rec, field)
        for y in re.findall(r"(19\d{2}|20\d{2}|21\d{2})", _nfkc(v)):
            ys.add(int(y))
    return sorted(ys)

def record_date(rec: Dict[str, Any]) -> Optional[datetime]:
    for k in DATE_KEYS:
        v = rec.get(k)
        if not v: continue
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

def _matches_year(rec: Dict[str, Any], year: Optional[int], yr: Optional[Tuple[int,int]]) -> bool:
    if year is None and yr is None: return True
    ys = _record_years(rec)
    if not ys: return False
    if year is not None: return year in ys
    lo, hi = yr
    return any(lo <= y <= hi for y in ys)

# ==================== 検索（一般解：簡易スコア＋完全順序） ====================
def _score(rec: Dict[str, Any], q_base: str) -> int:
    """最小の関連度：タイトル部分一致3点＋本文部分一致1点×出現回数"""
    if not q_base: return 0
    t = normalize_text(record_as_text(rec, "title"))
    b = normalize_text(record_as_text(rec, "text"))
    s = 0
    if t:
        s += 3 * t.count(q_base)
    if b:
        s += 1 * b.count(q_base)
    return s

def build_item(rec: Dict[str, Any], q_base: str, is_first_in_page: bool) -> Dict[str, Any]:
    # 表示用（最後にだけ加工）——検索用データは汚さない
    title = record_as_text(rec, "title") or "(無題)"
    body  = record_as_text(rec, "text") or ""
    # ざっくりハイライト（表示専用）
    esc = lambda s: (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
    title_h = esc(title).replace(esc(q_base), f"<mark>{esc(q_base)}</mark>") if q_base else esc(title)
    if is_first_in_page:
        snippet_src = body[:300]
    else:
        pos = body.find(q_base) if q_base else -1
        if pos < 0:
            snippet_src = body[:160]
        else:
            start = max(0, pos - 80); end = min(len(body), pos + 80)
            snippet_src = ("…" if start>0 else "") + body[start:end] + ("…" if end<len(body) else "")
    content_h = esc(snippet_src).replace(esc(q_base), f"<mark>{esc(q_base)}</mark>") if q_base else esc(snippet_src)
    return {
        "title": title_h,
        "content": content_h,
        "url": record_as_text(rec, "url"),
        "rank": None,
        "date": record_as_text(rec, "date"),
    }

def sort_key_relevance(entry: Tuple[int, Optional[datetime], str, Dict[str, Any]]) -> Tuple:
    score, d, did, _ = entry
    date_key = d or datetime.min
    # 完全順序：score↓, date↓, doc_id↑
    return (-int(score), -int(date_key.strftime("%Y%m%d%H%M%S")), did)

def sort_key_latest(entry: Tuple[int, Optional[datetime], str, Dict[str, Any]]) -> Tuple:
    score, d, did, _ = entry
    date_key = d or datetime.min
    # 完全順序：date↓, score↓, doc_id↑
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
    refresh: int = Query(0, description="1=kb.jsonl を再取得・再読み込み"),
):
    try:
        if refresh == 1:
            _refresh_kb_globals()

        if not os.path.exists(KB_PATH) or KB_LINES <= 0:
            return json_utf8({"items": [], "total_hits": 0, "page": page, "page_size": page_size,
                              "has_more": False, "next_page": None, "error": "kb_missing", "order_used": order})

        # 1) クエリ処理（年末尾抽出／ベース語）
        base_q, y_tail, yr_tail = _parse_year_from_query(q)
        q_base = normalize_text(base_q)

        # 2) スナップショット（同一クエリで固定の母集団）
        snapshot: List[Tuple[int, Optional[datetime], str, Dict[str, Any]]] = []
        for rec in (_KB_ROWS or []):
            if y_tail is not None or yr_tail is not None:
                if not _matches_year(rec, y_tail, yr_tail):
                    continue
            sc = _score(rec, q_base)
            if sc <= 0:
                continue
            d = record_date(rec)
            did = doc_id_for(rec)  # 完全順序＆dedupeの核
            snapshot.append((sc, d, did, rec))

        if not snapshot:
            return json_utf8({"items": [], "total_hits": 0, "page": page, "page_size": page_size,
                              "has_more": False, "next_page": None, "error": None, "order_used": order})

        # 3) ページング前 dedupe（doc_idで1件化：score高→同点はdate新しい方）
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

        # 4) 決定的ソート（完全順序）：score↓,date↓,doc_id↑ or date↓,score↓,doc_id↑
        if order == "latest":
            deduped.sort(key=sort_key_latest)
            order_used = "latest"
        else:
            deduped.sort(key=sort_key_relevance)
            order_used = "relevance"

        total = len(deduped)

        # 5) ページ切り出し（同じ配列からスライス）
        start = (page - 1) * page_size
        end   = start + page_size
        page_slice = deduped[start:end]
        has_more = end < total
        next_page = page + 1 if has_more else None

        # 6) 表示加工（最後にだけ、検索用データは変更しない）
        items: List[Dict[str, Any]] = []
        for i, (_sc, _d, _did, rec) in enumerate(page_slice):
            items.append(build_item(rec, q_base, is_first_in_page=(i == 0)))

        # ランク（全体順位 1始まり）
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
    # PORT 環境変数があれば尊重
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
