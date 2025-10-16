# app.py — Mini Rose Search API
# 方針反映版：日本語短語ファジー抑止 / 代表日=開催日/発行日 / order=latest / HTMLタグ除去対応
# 版: ui-2025-10-16-fix-html-clean

import os, re, json, unicodedata, datetime as dt, hashlib, io, time
from typing import List, Dict, Any, Tuple, Optional
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, PlainTextResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import requests
from urllib.parse import urlparse, urlunparse
import httpx
from fastapi import APIRouter

# =========================================================
# 🔧 HTMLタグ除去関数（ここが今回の修正版のポイント）
# =========================================================
def _clean_html(text: str) -> str:
    """HTMLタグや余分な改行・空白を除去する"""
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", "", text)   # <mark>や<br>等を削除
    text = re.sub(r"\s+", " ", text)      # 改行・連続空白を1個に
    return text.strip()

# =========================================================
# 診断ルータ (既存)
# =========================================================
DIAG = {
    "kb_url": os.getenv("KB_URL", "").strip(),
    "has_kb": False,
    "kb_size": 0,
    "loaded_at": None,
    "last_error": None,
    "etag": None,
}

_kb_lines_cache: Optional[list[str]] = None
router_diag = APIRouter()

async def _fetch_kb_text(url: str) -> str:
    headers = {
        "User-Agent": "mini-rose-search-jsonl/diag",
        "Accept": "text/plain,*/*",
    }
    timeout = httpx.Timeout(20.0, connect=10.0)
    async with httpx.AsyncClient(follow_redirects=True, timeout=timeout) as client:
        r = await client.get(url, headers=headers)
        r.raise_for_status()
        DIAG["etag"] = r.headers.get("ETag")
        return r.text

async def _load_kb(force: bool = False) -> None:
    global _kb_lines_cache
    try:
        if not DIAG["kb_url"]:
            raise RuntimeError("KB_URL is empty")
        if _kb_lines_cache is not None and not force:
            return
        text = await _fetch_kb_text(DIAG["kb_url"])
        lines = [ln for ln in text.splitlines() if ln.strip()]
        _kb_lines_cache = lines
        DIAG["kb_size"] = len(lines)
        DIAG["has_kb"] = len(lines) > 0
        DIAG["loaded_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        DIAG["last_error"] = None
    except Exception as e:
        DIAG["has_kb"] = False
        DIAG["kb_size"] = 0
        DIAG["last_error"] = repr(e)

@router_diag.get("/kb/status")
async def kb_status():
    await _load_kb(force=False)
    return DIAG

@router_diag.post("/kb/reload")
async def kb_reload():
    await _load_kb(force=True)
    return DIAG

@router_diag.get("/health2")
async def health2_diag():
    await _load_kb(force=False)
    return {"ok": True, "has_kb": DIAG["has_kb"], "kb_size": DIAG["kb_size"], "kb_url": DIAG["kb_url"]}

@router_diag.get("/diag2")
async def diag2_diag():
    await _load_kb(force=False)
    return {"version_hint": "jsonl-diag2", **DIAG}

# =========================================================
# FastAPI 本体
# =========================================================
app = FastAPI(title="Mini Rose Search API", version="ui-2025-10-16-fix-html-clean")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"], allow_credentials=True
)
app.include_router(router_diag)

if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/ui", response_class=HTMLResponse)
def ui():
    path = "static/ui.html"
    if os.path.isfile(path):
        html = open(path, encoding="utf-8").read()
        return HTMLResponse(html, headers={"Cache-Control": "no-store"})
    return HTMLResponse("<h1>Not Found</h1>", status_code=404, headers={"Cache-Control": "no-store"})

# =========================================================
# ここから既存検索ロジック（HTML除去を統合済み）
# =========================================================

# ==== 環境変数 ====
NOTION_TOKEN       = os.getenv("NOTION_TOKEN", "")
NOTION_DATABASE_ID  = os.getenv("NOTION_DATABASE_ID", "")
KB_URL  = os.getenv("KB_URL", "").strip()
KB_PATH = os.getenv("KB_PATH", "kb.jsonl").strip() or "kb.jsonl"
JSON_HEADERS = {"content-type": "application/json; charset=utf-8"}

# ==== JSONLローダ ====
def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def load_kb(force: bool=False) -> Tuple[List[Dict[str,str]], str, int]:
    records=[]
    if KB_URL:
        try:
            r = requests.get(KB_URL, timeout=15)
            if r.status_code==200 and r.text:
                with open(KB_PATH,"w",encoding="utf-8") as f: f.write(r.text)
        except Exception: pass
    if os.path.exists(KB_PATH):
        with io.open(KB_PATH,"r",encoding="utf-8") as f:
            for line in f:
                if not line.strip(): continue
                try:
                    rec=json.loads(line)
                    rec["title"]=_clean_html(rec.get("title",""))
                    rec["text"]=_clean_html(rec.get("text",""))
                    records.append(rec)
                except: continue
    sha=_sha256_file(KB_PATH) if os.path.exists(KB_PATH) else ""
    return records, sha, len(records)

# ==== 正規化 ====
def fold_kana(s: str) -> str:
    s = unicodedata.normalize("NFKC", s or "")
    return s.lower().strip()

# ==== 検索 ====
def search_kb_advanced(q: str) -> Tuple[List[Dict[str,Any]], List[str]]:
    recs, _, _ = load_kb(False)
    if not recs or not q: return [], []
    qn = fold_kana(q)
    hits=[]
    for r in recs:
        title = fold_kana(r.get("title",""))
        text  = fold_kana(r.get("text",""))
        if qn in title or qn in text:
            hits.append(r)
    return hits, [q]

# =========================================================
# API群
# =========================================================
@app.get("/api/search")
def api_search(q:str=Query("")):
    if not q.strip():
        return JSONResponse({"items":[], "total_hits":0})
    ranked, _ = search_kb_advanced(q)
    items=[]
    for r in ranked[:5]:
        items.append({
            "title": r.get("title",""),
            "content": r.get("text",""),
            "url": r.get("url",""),
            "source": r.get("url","")
        })
    return JSONResponse({
        "items": items,
        "total_hits": len(ranked),
        "page": 1,
        "page_size": 5,
        "has_more": len(ranked)>5,
        "next_page": 2 if len(ranked)>5 else None,
        "error": None
    }, headers=JSON_HEADERS)

@app.get("/health2")
def health2():
    _, sha, lines = load_kb(False)
    return JSONResponse({
        "ok": True,
        "has_kb": bool(lines>0),
        "kb_size": lines,
        "kb_sha256": sha[:40] if sha else None,
        "kb_url": KB_URL,
        "version_hint": "jsonl-health2"
    }, headers=JSON_HEADERS)

@app.post("/kb/reload")
def kb_reload():
    _, sha, lines = load_kb(True)
    return JSONResponse({
        "reloaded": True,
        "has_kb": bool(lines>0),
        "kb_size": lines,
        "kb_sha256": sha[:40] if sha else None,
        "kb_url": KB_URL
    }, headers=JSON_HEADERS)

@app.get("/")
def root():
    return PlainTextResponse("Mini Rose Search API is running.\n", headers={"content-type":"text/plain; charset=utf-8"})
