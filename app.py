# app.py — Mini Rose Search API（正式簡略版・JSONL専用）
# 版: stable-jsonl-2025-10-16-proper
# 特徴: 日本語検索最適化 / UI維持 / 診断機能完備 / 軽量高速版

import os, re, json, unicodedata, datetime as dt, hashlib
from typing import List, Dict, Any
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, HTMLResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import requests

# ==== 基本設定 ====
APP_VERSION = "stable-jsonl-2025-10-16-proper"
KB_URL = os.getenv("KB_URL", "").strip()
KB_PATH = "kb.jsonl"
CACHE_FILE = "kb_cache.jsonl"

# ==== FastAPI 初期化 ====
app = FastAPI(title="Mini Rose Search API", version=APP_VERSION)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory="static"), name="static")

# ==== 正規化関数 ====
def normalize(text: str) -> str:
    """日本語・英数字を統一的に正規化する"""
    if not text:
        return ""
    text = unicodedata.normalize("NFKC", text)
    text = text.lower()
    # 句読点・空白を整理
    text = re.sub(r"[、。,．，｡･・「」『』（）()［］\[\]{}<>〈〉【】!?！？…‥]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def tokenize_like_japanese(text: str) -> List[str]:
    """
    日本語用の簡易トークナイズ（分かち書き風）
    長い単語列を3〜5文字単位で分割して部分一致精度を向上させる
    """
    text = normalize(text)
    if not text:
        return []
    # 英数字と漢字・かなを分離
    parts = re.findall(r"[a-zA-Z0-9]+|[一-龥ぁ-んァ-ンー]+", text)
    tokens = []
    for p in parts:
        if len(p) > 5:
            # 長すぎる単語をスライス
            tokens += [p[i:i+3] for i in range(0, len(p), 3)]
        else:
            tokens.append(p)
    return tokens

# ==== KB 読み込み ====
def load_kb() -> List[Dict[str, Any]]:
    """KBをURLまたはローカルから読み込む"""
    if KB_URL:
        try:
            res = requests.get(KB_URL, timeout=10)
            if res.status_code == 200:
                with open(KB_PATH, "w", encoding="utf-8") as f:
                    f.write(res.text)
        except Exception as e:
            print("⚠️ KB_URL load failed:", e)
    path = KB_PATH if os.path.exists(KB_PATH) else CACHE_FILE
    if not os.path.exists(path):
        print("⚠️ No KB found.")
        return []
    items = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                items.append(json.loads(line))
            except Exception:
                continue
    print(f"📚 KB loaded: {len(items)} records")
    return items

KB_DATA = load_kb()
KB_HASH = hashlib.sha256(json.dumps(KB_DATA, ensure_ascii=False).encode()).hexdigest()[:12]

# ==== ハイライト ====
def highlight_text(text: str, query: str) -> str:
    """検索語句を <mark> でハイライト"""
    q = re.escape(normalize(query))
    if not q:
        return text
    return re.sub(q, lambda m: f"<mark>{m.group(0)}</mark>", text, flags=re.IGNORECASE)

# ==== 検索ロジック ====
def search_kb(query: str, top_k: int = 5) -> List[Dict[str, Any]]:
    q_norm = normalize(query)
    tokens = tokenize_like_japanese(q_norm)
    if not tokens:
        return []

    results = []
    for rec in KB_DATA:
        title = rec.get("title", "")
        content = rec.get("content", "")
        url = rec.get("url", "")
        full_text = normalize(title + " " + content)
        score = sum(1 for t in tokens if t in full_text)
        if score > 0:
            snippet = content[:1200]
            snippet = highlight_text(snippet, q_norm)
            results.append({
                "title": title or "(無題)",
                "content": snippet,
                "url": url,
                "source": url,
                "score": score
            })

    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:top_k]

# ==== API エンドポイント ====
@app.get("/health2")
def health2():
    """システムヘルス"""
    return {
        "ok": True,
        "has_kb": bool(KB_DATA),
        "kb_size": len(KB_DATA),
        "kb_url": KB_URL,
        "kb_hash": KB_HASH
    }

@app.get("/diag2")
def diag2():
    """詳細診断"""
    return {
        "version_hint": "jsonl-diag2",
        "kb_url": KB_URL,
        "has_kb": bool(KB_DATA),
        "kb_size": len(KB_DATA),
        "kb_sha": KB_HASH,
        "loaded_at": dt.datetime.now().isoformat(timespec="seconds")
    }

@app.get("/api/search")
def api_search(
    q: str = Query(..., description="検索語句"),
    page: int = 1,
    page_size: int = 5,
):
    """KB全文検索"""
    if not q.strip():
        return {"items": [], "total_hits": 0, "error": "empty query"}
    results = search_kb(q, top_k=page_size)
    total = len(results)
    return {
        "items": results,
        "total_hits": total,
        "page": page,
        "page_size": page_size,
        "has_more": total > page * page_size,
        "next_page": page + 1 if total > page * page_size else None
    }

@app.get("/ui")
def ui_page():
    """検索UI"""
    try:
        with open("static/ui.html", "r", encoding="utf-8") as f:
            return HTMLResponse(f.read())
    except FileNotFoundError:
        return HTMLResponse("<h2>UI not found</h2>", status_code=404)

@app.get("/version")
def version():
    """バージョン情報"""
    return PlainTextResponse(APP_VERSION)

@app.get("/")
def root():
    """ルート確認"""
    return {"message": "Mini Rose Search API running", "version": APP_VERSION}

# ==== ここまで ====
