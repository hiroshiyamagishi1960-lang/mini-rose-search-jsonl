# app.py — Mini Rose Search API（Notion直読みロールバック版）
# 版: notion-direct-rollback-2025-10-19
# 方針:
# - データ源を Notion DB へ切り戻し（kb.jsonl は参照しない）
# - 起動後は Notion をキャッシュ（メモリ常駐）。TTL内は再読込しない（高速化）
# - 検索対象は title + body（タイトルだけ一致も必ず拾う）
# - 表記ゆれ吸収: NFKC（全角/半角・記号・スペース）、苔/コケ/こけ、年+語の連結/分割
# - ハイライトは title/本文の両方に <mark>…</mark>
# - フレーズ(strict_phrase) と 分割一致（語単位）を両立。re は事前コンパイル
# - API: /version /health /diag /api/search /ui（既存UIをそのまま配信）

import os, re, json, unicodedata, time, datetime as dt
from typing import List, Dict, Any, Optional, Tuple
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, HTMLResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import requests

APP_VERSION = "notion-direct-rollback-2025-10-19"

# ==== 環境変数 ====
NOTION_TOKEN       = os.getenv("NOTION_TOKEN", "")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID", "")
FIELD_URL          = os.getenv("FIELD_URL", "出典URL")
CACHE_TTL_SECONDS  = int(os.getenv("CACHE_TTL_SECONDS", "600"))  # 10分

# ==== FastAPI 基本 ====
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# UI 配信（既存の static をそのまま）
if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

# ==== 内部キャッシュ ====
_CACHE: Dict[str, Any] = {
    "source": "notion",
    "rows": [],          # List[Dict]: {"id","title","body","url","date"}
    "last_sync": 0.0,    # epoch sec
    "last_count": 0,
}

# ==== 正規表現 / 正規化 ====
_WS_RE = re.compile(r"\s+")
# ひら/カタ/漢字の境界を厳密に取れない日本語の都合上、単純含有ベースで安全運用
def normalize_text(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s)
    # 制御文字系や過剰空白を整理
    s = s.replace("\u200b", "")
    s = _WS_RE.sub(" ", s).strip()
    return s

def expand_query_terms(q: str) -> Tuple[str, List[str]]:
    """
    入力クエリを NFKC 正規化し、
    ・フレーズ判定（ダブルクオート囲み） → phrase
    ・語単位（スペース区切り） → terms
    ・苔/コケ/こけ の表記ゆれ展開
    ・数値＋語の連結/分割（例: 2024コンテスト結果 / 2024 コンテスト結果）
    """
    q_raw = q or ""
    q_n = normalize_text(q_raw)

    phrase = ""
    # "..." フレーズ抽出（最初の "" ペアのみを扱う）
    if len(q_n) >= 2 and q_n[0] == '"' and q_n[-1] == '"':
        phrase = q_n.strip('"')
    # 語単位
    terms = [t for t in q_n.strip('"').split(" ") if t]

    # 表記ゆれ（苔）
    alt_map = {"苔": ["苔", "コケ", "こけ"], "コケ": ["苔", "コケ", "こけ"], "こけ": ["苔", "コケ", "こけ"]}
    expanded: List[str] = []
    for t in terms:
        if t in alt_map:
            expanded.extend(alt_map[t])
        else:
            expanded.append(t)

    # 数字＋語の連結/分割 同値扱い（例: "2024コンテスト結果" → ["2024 コンテスト結果", ...] も試す）
    # ここではシンプルに: 連続数字+非空白を1箇所だけ分割生成
    add_terms: List[str] = []
    for t in list(expanded):
        m = re.match(r"^(\d{4,})(.+)$", t)
        if m:
            add_terms.append(f"{m.group(1)} {m.group(2)}")
    expanded.extend(add_terms)

    # 重複削除
    seen = set()
    uniq = []
    for t in expanded:
        if t not in seen:
            uniq.append(t)
            seen.add(t)

    return phrase, uniq

def compile_patterns(phrase: str, terms: List[str]) -> Tuple[Optional[re.Pattern], List[re.Pattern]]:
    # フレーズ用
    phrase_re = re.compile(re.escape(phrase)) if phrase else None
    # 語単位（日本語なので単純含有）
    term_res = [re.compile(re.escape(t)) for t in terms if t]
    return phrase_re, term_res

def highlight_text(text: str, phrase_re: Optional[re.Pattern], term_res: List[re.Pattern]) -> str:
    if not text:
        return ""
    s = text
    # まずフレーズ（長いもの優先）
    if phrase_re:
        s = phrase_re.sub(lambda m: f"<mark>{m.group(0)}</mark>", s)
    # 次に語単位
    for r in term_res:
        s = r.sub(lambda m: f"<mark>{m.group(0)}</mark>", s)
    return s

# ==== Notion API ====
_NOTION_BASE = "https://api.notion.com/v1"
_NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

def _notion_ok() -> bool:
    return bool(NOTION_TOKEN and NOTION_DATABASE_ID)

def _extract_rich_text(prop: Dict[str, Any]) -> str:
    """title / rich_text の文字列を結合"""
    if not prop:
        return ""
    buf: List[str] = []
    if prop.get("type") == "title":
        for t in prop.get("title", []):
            buf.append(t.get("plain_text", ""))
    elif prop.get("type") == "rich_text":
        for t in prop.get("rich_text", []):
            buf.append(t.get("plain_text", ""))
    else:
        # その他タイプ（multi_select / number / date / select / url 等）は簡易に文字化
        typ = prop.get("type")
        if typ == "multi_select":
            vals = [x.get("name", "") for x in prop.get("multi_select", [])]
            buf.append(" ".join(vals))
        elif typ == "select":
            v = prop.get("select") or {}
            buf.append(v.get("name", ""))
        elif typ == "date":
            v = prop.get("date") or {}
            buf.append(v.get("start","") or "")
        elif typ == "url":
            buf.append(prop.get("url") or "")
        elif typ in ("number","checkbox","email","phone_number","people","files"):
            # 必要になれば拡張
            pass
    return normalize_text("".join(buf))

def _page_to_row(page: Dict[str, Any]) -> Dict[str, Any]:
    pid = page.get("id","")
    props = page.get("properties", {}) or {}
    title = ""
    body_parts: List[str] = []
    url = ""
    date_str = ""

    # 1) title プロパティを特定（type=title の最初）
    for name, p in props.items():
        if isinstance(p, dict) and p.get("type") == "title":
            title = _extract_rich_text(p)
            break

    # 2) body 候補（rich_text / その他文字化）をまとめて body に
    for name, p in props.items():
        if not isinstance(p, dict):
            continue
        txt = _extract_rich_text(p)
        if txt:
            body_parts.append(f"{name}: {txt}")
        # URL 候補
        if name == FIELD_URL and p.get("type") == "url":
            url = p.get("url") or url
        # date 候補（最初の date）
        if not date_str and p.get("type") == "date":
            v = p.get("date") or {}
            date_str = v.get("start","") or ""

    body = normalize_text("\n".join(body_parts))

    # URL フォールバック（Notion固有URL：共有設定がPublicでないと外部は見えない点に注意）
    if not url:
        # ハイフン除去IDで notion.so 直
        url = f"https://www.notion.so/{pid.replace('-','')}"

    # date 正規化（YYYY-MM-DD に揃える／無ければ空）
    if date_str:
        try:
            d = dt.datetime.fromisoformat(date_str.replace("Z","").split("T")[0])
            date_str = d.strftime("%Y-%m-%d")
        except Exception:
            pass

    return {"id": pid, "title": title or "(無題)", "body": body, "url": url, "date": date_str}

def fetch_notion_all_rows() -> List[Dict[str, Any]]:
    """Notion DB を全件取得 → rows へ変換（必要最小限の1パス）。"""
    if not _notion_ok():
        return []
    rows: List[Dict[str, Any]] = []
    payload = {"page_size": 100}
    next_cursor = None
    for _ in range(100):  # safety
        if next_cursor:
            payload["start_cursor"] = next_cursor
        resp = requests.post(f"{_NOTION_BASE}/databases/{NOTION_DATABASE_ID}/query",
                             headers=_NOTION_HEADERS, data=json.dumps(payload), timeout=30)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        for page in results:
            rows.append(_page_to_row(page))
        next_cursor = data.get("next_cursor")
        if not next_cursor:
            break
    return rows

def ensure_cache(force: bool=False) -> None:
    now = time.time()
    if force or (now - _CACHE["last_sync"] > CACHE_TTL_SECONDS) or not _CACHE["rows"]:
        rows = fetch_notion_all_rows()
        _CACHE["rows"] = rows
        _CACHE["last_sync"] = now
        _CACHE["last_count"] = len(rows)

# ==== 検索 ====
def match_score(row: Dict[str, Any], phrase_re: Optional[re.Pattern], term_res: List[re.Pattern]) -> Tuple[int,int]:
    """(score, flags) を返す。score: 大きいほど優先。flags: フレーズ一致、有無などのヒント。"""
    text = f"{row['title']}\n{row['body']}"
    score = 0
    flags = 0
    if phrase_re and phrase_re.search(text):
        score += 100
        flags |= 1
    # 語単位
    hits = 0
    for r in term_res:
        if r.search(text):
            hits += 1
    score += hits * 10
    # 日付が新しいほど少し優遇
    if row.get("date"):
        try:
            y,m,d = map(int, row["date"].split("-"))
            score += y  # 年で微加点（同点時の並びを安定化）
        except Exception:
            pass
    return score, flags

def search_rows(q: str, page: int, page_size: int, strict_phrase: bool) -> Dict[str, Any]:
    ensure_cache(force=False)
    rows = _CACHE["rows"]

    phrase, terms = expand_query_terms(q or "")
    if strict_phrase and not phrase:
        # ユーザーが strict_phrase=1 を指定したのに "..." でない場合は、フレーズ=全文で代替
        phrase = normalize_text(q or "")

    phrase_re, term_res = compile_patterns(phrase if strict_phrase else phrase, terms)

    scored: List[Tuple[int,int,Dict[str,Any]]] = []
    for r in rows:
        s, f = match_score(r, phrase_re, term_res)
        if s > 0:
            scored.append((s, f, r))

    # スコア降順→日付で暗黙加点しているので概ね新しい順が上に
    scored.sort(key=lambda x: x[0], reverse=True)

    total = len(scored)
    if page < 1: page = 1
    if page_size < 1: page_size = 5
    start = (page - 1) * page_size
    end = start + page_size
    page_items = [x[2] for x in scored[start:end]]

    # ハイライト生成（title / body 両方）
    phrase_re_h, term_res_h = compile_patterns(phrase, terms)
    items_out = []
    for r in page_items:
        title_h = highlight_text(r["title"], phrase_re_h, term_res_h)
        body_h  = highlight_text(r["body"],  phrase_re_h, term_res_h)
        # 短いスニペット（長すぎる本文を少しだけ）
        snippet = body_h
        if len(snippet) > 600:
            snippet = snippet[:600] + "…"
        items_out.append({
            "title": title_h or "(無題)",
            "content": snippet,
            "url": r["url"],
            "rank": 0,
            "date": r.get("date",""),
        })

    return {
        "items": items_out,
        "total_hits": total,
        "page": page,
        "page_size": page_size,
        "has_more": end < total,
        "next_page": (page + 1) if end < total else None,
        "error": None,
        "order_used": "relevance",
        "source": "notion",
    }

# ==== エンドポイント ====

@app.get("/version")
def version():
    return PlainTextResponse(APP_VERSION, media_type="text/plain; charset=utf-8")

@app.get("/health")
def health():
    ok = _notion_ok()
    ensure_cache(force=False)
    return JSONResponse({
        "ok": ok and bool(_CACHE["rows"]),
        "source": _CACHE["source"],
        "rows_cached": len(_CACHE["rows"]),
        "last_sync": _CACHE["last_sync"],
        "db_id": NOTION_DATABASE_ID[:6] + "…" if NOTION_DATABASE_ID else "",
        "env": {"APP_VERSION": APP_VERSION},
    })

@app.get("/diag")
def diag():
    ensure_cache(force=False)
    return JSONResponse({
        "source": _CACHE["source"],
        "rows": _CACHE["last_count"],
        "cache_ttl": CACHE_TTL_SECONDS,
        "has_token": bool(NOTION_TOKEN),
        "has_dbid": bool(NOTION_DATABASE_ID),
    })

@app.get("/api/search")
def api_search(
    q: str = Query("", description="検索クエリ。フレーズは \"...\" で指定"),
    page: int = Query(1, ge=1),
    page_size: int = Query(5, ge=1, le=50),
    strict_phrase: int = Query(0, description="1=フレーズのみで照合（分割一致なし）"),
    refresh: int = Query(0, description="1=Notionを強制再読込"),
):
    if refresh == 1:
        ensure_cache(force=True)
    data = search_rows(q=q, page=page, page_size=page_size, strict_phrase=bool(strict_phrase))
    return JSONResponse(data)

@app.get("/")
def root():
    return HTMLResponse("<!doctype html><html><head><meta charset='utf-8'><title>Mini Rose</title></head><body><p>OK</p></body></html>")

# 既存UIがある場合は /ui から参照（static/ui.html をそのまま使う）
@app.get("/ui")
def ui_entry():
    if os.path.exists("static/ui.html"):
        with open("static/ui.html", "r", encoding="utf-8") as f:
            html = f.read()
        return HTMLResponse(html)
    return HTMLResponse("<p>UI not found. Deploy static/ui.html</p>")
