# app.py — 統合恒久版：UI安定＋検索機能（v5.2）
# 目的：
#  - /ui受け皿・入口統一・health/versionのHTML/JSON対応・迷子救済
#  - /api/search 含む全文検索エンジン（v5.2仕様）
# -------------------------------------------------------

import os, io, re, csv, json, hashlib, unicodedata, threading
from datetime import datetime
from typing import List, Dict, Any
from fastapi import FastAPI, Request, Query
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from functools import lru_cache

# =======================================================
# UI・導線部（恒久安定版）
# =======================================================
app = FastAPI(title="mini-rose-search-jsonl")

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
UI_FILE = os.path.join(STATIC_DIR, "ui.html")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

def html_shell(title: str, inner_html: str) -> str:
    return f"""<!doctype html><html lang="ja"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<link rel="manifest" href="/static/manifest.json"><meta name="theme-color" content="#AEE6B8">
<style>
  html,body{{margin:0;padding:0;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Noto Sans JP","Yu Gothic",Meiryo,sans-serif}}
  .bar{{position:sticky;top:0;display:flex;justify-content:space-between;align-items:center;gap:12px;
        padding:10px 14px;border-bottom:1px solid #e5e7eb;background:#fff}}
  .btn{{display:inline-block;padding:8px 12px;border:1px solid #e5e7eb;border-radius:10px;text-decoration:none;color:#0f172a}}
  .wrap{{max-width:960px;margin:16px auto;padding:0 12px}}
  pre{{background:#fff;border:1px solid #e5e7eb;border-radius:12px;padding:12px;overflow:auto;white-space:pre-wrap;word-break:break-word}}
</style></head><body>
  <div class="bar"><strong>{title}</strong><a class="btn" href="/ui">検索画面に戻る</a></div>
  <div class="wrap">{inner_html}</div>
</body></html>"""

def wants_html(req: Request) -> bool:
    return "text/html" in (req.headers.get("accept") or "").lower()

@app.get("/", include_in_schema=False)
def root_to_ui():
    return RedirectResponse("/ui", status_code=307)

@app.get("/static/ui.html", include_in_schema=False)
def canonicalize_static_ui(request: Request):
    from urllib.parse import urlsplit
    q = urlsplit(str(request.url)).query
    return RedirectResponse("/ui" + (("?" + q) if q else ""), status_code=307)

@app.get("/ui", include_in_schema=False)
def ui_entry():
    if os.path.exists(UI_FILE):
        return FileResponse(UI_FILE, media_type="text/html")
    return HTMLResponse(html_shell("UI not found", "<p>static/ui.html が見つかりません。</p>"), status_code=200)

@app.get("/health", include_in_schema=False)
def health(request: Request):
    payload = {"ok": True, "time": datetime.utcnow().isoformat(timespec="seconds") + "Z"}
    return HTMLResponse(html_shell("Health", f"<pre>{json.dumps(payload, ensure_ascii=False, indent=2)}</pre>")) \
        if wants_html(request) else JSONResponse(payload)

@app.get("/version", include_in_schema=False)
def version(request: Request):
    payload = {
        "version": os.getenv("APP_VERSION") or os.getenv("RENDER_GIT_COMMIT") or "jsonl-stable",
        "kb_url": os.getenv("KB_URL", "")
    }
    return HTMLResponse(html_shell("Version", f"<pre>{json.dumps(payload, ensure_ascii=False, indent=2)}</pre>")) \
        if wants_html(request) else JSONResponse(payload)

@app.exception_handler(404)
async def not_found_to_ui(request: Request, exc):
    path = (request.url.path or "").rstrip("/")
    if path.endswith("/ui"):
        return RedirectResponse("/ui", status_code=307)
    if "/health/" in path and path.split("/health/")[-1].endswith("ui"):
        return RedirectResponse("/ui", status_code=307)
    if "/version/" in path and path.split("/version/")[-1].endswith("ui"):
        return RedirectResponse("/ui", status_code=307)
    return JSONResponse({"detail": "Not Found", "path": request.url.path}, status_code=404)

# =======================================================
# 検索API部（v5.2 相当）
# =======================================================

KB_URL = os.getenv("KB_URL", "")
KB_PATH = os.getenv("KB_PATH", "kb.jsonl")

# ---------- KB ロード ----------
@lru_cache(maxsize=1)
def load_kb() -> List[Dict[str, Any]]:
    path = KB_PATH
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]

# ---------- 正規化 ----------
def normalize_text(text: str) -> str:
    if not text:
        return ""
    text = unicodedata.normalize("NFKC", text)
    text = text.lower()
    text = re.sub(r"\s+", " ", text)
    return text.strip()

# ---------- スコア算出 ----------
def compute_score(entry: Dict[str, Any], q_words: List[str]) -> float:
    text_all = " ".join([str(v) for v in entry.values() if isinstance(v, str)])
    text_norm = normalize_text(text_all)
    score = 0
    for qw in q_words:
        if qw in text_norm:
            score += 2
        elif len(qw) > 2 and qw[:-1] in text_norm:
            score += 1
    return score

# ---------- 検索 ----------
@app.get("/api/search")
def api_search(q: str = Query("", description="検索語"),
               order: str = Query("latest"),
               page: int = Query(1, ge=1),
               page_size: int = Query(20, ge=1, le=100)):
    docs = load_kb()
    if not q.strip():
        return {"total": len(docs), "results": []}

    qn = normalize_text(q)
    q_words = [w for w in qn.split(" ") if w]

    scored = []
    for entry in docs:
        sc = compute_score(entry, q_words)
        if sc > 0:
            scored.append((sc, entry))

    if order == "latest":
        scored.sort(key=lambda x: (x[1].get("date", ""), x[0]), reverse=True)
    else:
        scored.sort(key=lambda x: x[0], reverse=True)

    start = (page - 1) * page_size
    end = start + page_size
    results = [e for _, e in scored[start:end]]

    return {
        "total": len(scored),
        "count": len(results),
        "page": page,
        "results": results
    }

# =======================================================
# 終了
# =======================================================
