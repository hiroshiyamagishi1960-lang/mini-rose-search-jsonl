# app.py — Mini Rose Search API
# 版: ui-2025-10-16-highlight-restore + longclip
# 検索精度・ハイライト復元・抜粋長拡大版

import os, re, json, unicodedata, datetime as dt
from typing import List, Dict, Any
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import requests

# ==== 環境変数 ====
KB_URL = os.getenv("KB_URL", "")
FIELD_TITLE = os.getenv("FIELD_TITLE", "title")
FIELD_CONTENT = os.getenv("FIELD_CONTENT", "content")
FIELD_URL = os.getenv("FIELD_URL", "url")

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory="static"), name="static")

# ==== 簡易キャッシュ ====
_kb_cache = {"data": [], "loaded": None}

# ==== 正規化関数 ====
def _normalize_text(s: str) -> str:
    s = unicodedata.normalize("NFKC", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()

# ==== KBロード ====
def _load_kb() -> List[Dict[str, Any]]:
    if _kb_cache["data"]:
        return _kb_cache["data"]
    if not KB_URL:
        return []
    r = requests.get(KB_URL, timeout=10)
    if r.status_code == 200:
        lines = [json.loads(line) for line in r.text.splitlines() if line.strip()]
        _kb_cache["data"] = lines
        _kb_cache["loaded"] = dt.datetime.now()
        return lines
    return []

# ==== 検索 ====
def _search(q: str, page: int = 1, page_size: int = 5):
    kb = _load_kb()
    qn = _normalize_text(q)
    items = []
    for rec in kb:
        text = f"{rec.get(FIELD_TITLE, '')} {rec.get(FIELD_CONTENT, '')}"
        if qn in _normalize_text(text):
            snippet = _highlight_and_clip(rec.get(FIELD_CONTENT, ""), qn)
            items.append({
                "title": rec.get(FIELD_TITLE, ""),
                "content": snippet,
                "url": rec.get(FIELD_URL, ""),
                "source": rec.get(FIELD_URL, "")
            })
    total = len(items)
    start = (page - 1) * page_size
    end = start + page_size
    return {
        "items": items[start:end],
        "total_hits": total,
        "page": page,
        "page_size": page_size,
        "has_more": end < total,
        "next_page": page + 1 if end < total else None,
        "error": None,
    }

# ==== 抜粋＋ハイライト ====
def _highlight_and_clip(text: str, q: str) -> str:
    if not text or not q:
        return text[:400]
    pattern = re.escape(q)
    match = re.search(pattern, text)
    if match:
        start = max(0, match.start() - 150)
        end = min(len(text), match.end() + 1200)
        clip = text[start:end]
        return re.sub(pattern, f"<mark>{q}</mark>", clip)
    return text[:1200]

# ==== ルート ====
@app.get("/api/search")
def api_search(q: str = Query(""), page: int = 1, page_size: int = 5):
    return JSONResponse(_search(q, page, page_size))

@app.get("/health2")
def health2():
    kb = _load_kb()
    return {"ok": True, "has_kb": bool(kb), "kb_size": len(kb), "kb_url": KB_URL}

@app.get("/diag2")
def diag2():
    kb = _load_kb()
    return {
        "version_hint": "jsonl-diag2",
        "kb_url": KB_URL,
        "has_kb": bool(kb),
        "kb_size": len(kb),
        "loaded_at": _kb_cache["loaded"].isoformat() if _kb_cache["loaded"] else None,
        "last_error": None
    }

@app.get("/ui")
def ui():
    with open("static/ui.html", encoding="utf-8") as f:
        return HTMLResponse(f.read())
