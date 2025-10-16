# app.py — Mini Rose Search API（JSONL版：mini-rose-search-jsonl 用）
# 方針：UI変更なし / JSONLの全テキストをシンプル部分一致で横断検索 / 最新順
# 版: jsonl-2025-10-16-simple-contains

import os, re, json, unicodedata, datetime as dt
from typing import List, Dict, Any, Optional, Any
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, PlainTextResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import requests

# ==== 環境変数 ====
KB_URL = os.getenv("KB_URL", "https://raw.githubusercontent.com/hiroshiyamagishi1960-lang/mini-rose-search-jsonl/main/kb.jsonl")
JSON_HEADERS = {"content-type": "application/json; charset=utf-8"}

app = FastAPI(title="Mini Rose Search API", version="jsonl-2025-10-16-simple-contains")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"], allow_credentials=True
)

# ==== UI ====
if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/ui", response_class=HTMLResponse)
def ui():
    path = "static/ui.html"
    if os.path.isfile(path):
        with open(path, "r", encoding="utf-8") as f:
            html = f.read()
        return HTMLResponse(html, headers={"Cache-Control": "no-store"})
    return HTMLResponse("<h1>Not Found</h1>", status_code=404, headers={"Cache-Control": "no-store"})

# =============================
# 基本ユーティリティ
# =============================
def _nfkc(s: Optional[str]) -> str:
    return unicodedata.normalize("NFKC", s or "")

def _pick(*cands):
    for v in cands:
        if isinstance(v, str) and v.strip():
            return v
    return ""

# =============================
# KBロード（JSONL）
# =============================
def _load_kb(url:str)->List[Dict[str,Any]]:
    items=[]
    try:
        r=requests.get(url,timeout=10)
        r.raise_for_status()
        for line in r.text.splitlines():
            if line.strip():
                try:
                    items.append(json.loads(line))
                except:
                    pass
    except Exception as e:
        print("[WARN] KB load failed:", e)
    return items

KB_DATA=_load_kb(KB_URL)
print(f"[INIT] KB loaded: {len(KB_DATA)} records from {KB_URL}")

# =============================
# 年抽出・代表日（最新順ソート用）
# =============================
def _nfkcnum(s:str)->str: return unicodedata.normalize("NFKC", s or "")

def _years_from_text(s:str)->List[int]:
    ys=[int(y) for y in re.findall(r"(19|20|21)\d{2}", _nfkcnum(s))]
    return sorted(set(ys))

def _record_years(rec:Dict[str,Any])->List[int]:
    ys:set[int]=set()
    def _walk(x:Any):
        if isinstance(x, dict):
            for v in x.values(): _walk(v)
        elif isinstance(x, list):
            for v in x: _walk(v)
        elif isinstance(x, str):
            ys.update(_years_from_text(x))
    _walk(rec)
    return sorted(ys)

def _best_date(rec:Dict[str,Any])->dt.date:
    ys=_record_years(rec)
    return dt.date(max(ys),1,1) if ys else dt.date(1970,1,1)

# =============================
# 検索（全テキスト横断・シンプル部分一致）
# =============================
def _flatten_text(x:Any, buf:list):
    if isinstance(x, dict):
        for v in x.values(): _flatten_text(v, buf)
    elif isinstance(x, list):
        for v in x: _flatten_text(v, buf)
    elif isinstance(x, str):
        s=x.strip()
        if s: buf.append(s)

def _record_text_all(rec:Dict[str,Any])->str:
    buf:list[str]=[]
    _flatten_text(rec, buf)
    return " \n".join(buf)

def jp_terms(q:str)->List[str]:
    if not q: return []
    qn=_nfkc(q).replace("　"," ")
    toks=[t for t in qn.split() if t]
    # ここでは AND ではなく OR 的に「どれか1語でも含めばヒット」にする
    return list(dict.fromkeys(toks))

def _match_simple(text:str, terms:List[str])->bool:
    if not text: return False
    t_norm=_nfkc(text).lower()
    for term in terms:
        if _nfkc(term).lower() in t_norm:
            return True
    return False

def search_jsonl(q:str, year=None, year_from=None, year_to=None)->List[Dict[str,Any]]:
    if not KB_DATA: return []
    terms = jp_terms(q)
    if not terms: return []

    results=[]
    for rec in KB_DATA:
        title = str(rec.get("title",""))
        body  = _pick(rec.get("content",""), rec.get("text",""), rec.get("body",""), rec.get("description",""))
        alltxt = _record_text_all(rec)

        # タイトル / 代表本文 / 全テキストのいずれかに含まれればヒット
        if _match_simple(title, terms) or _match_simple(body, terms) or _match_simple(alltxt, terms):
            if year or year_from or year_to:
                ys=_record_years(rec)
                if not ys: continue
                lo=year_from or -10**9; hi=year_to or 10**9
                if year and year not in ys: continue
                if not any(lo<=y<=hi for y in ys): continue
            results.append(rec)

    results.sort(key=lambda r:_best_date(r), reverse=True)
    return results

# =============================
# API
# =============================
@app.get("/health")
def health():
    return JSONResponse({"ok":True,"kb_url":KB_URL,"kb_size":len(KB_DATA)}, headers=JSON_HEADERS)

@app.get("/version")
def version():
    return JSONResponse({"version":app.version}, headers=JSON_HEADERS)

@app.get("/diag")
def diag(q:str=Query("", description="確認")):
    return JSONResponse({"query":q,"years":_years_from_text(q)}, headers=JSON_HEADERS)

@app.get("/api/search")
def api_search(q:str=Query("", description="検索語"), page:int=1, page_size:int=5, order:str="latest"):
    try:
        if not q.strip():
            return JSONResponse({"items":[],"total_hits":0,"page":1,"page_size":page_size,"has_more":False,"next_page":None,"error":None,"order_used":order}, headers=JSON_HEADERS)

        ranked=search_jsonl(q)
        total=len(ranked)
        start=(page-1)*page_size; end=start+page_size
        slice_=ranked[start:end]

        def _best_title(r:Dict[str,Any])->str:
            return _pick(r.get("title",""), r.get("heading",""), r.get("tTitle",""))

        def _best_body(r:Dict[str,Any])->str:
            return _pick(r.get("content",""), r.get("text",""), r.get("body",""), r.get("description",""), _record_text_all(r))

        items=[]
        for i, r in enumerate(slice_):
            body=_best_body(r)
            items.append({
                "title": _best_title(r),
                "content": body[:900],
                "url": r.get("url",""),
                "source": r.get("url",""),
                "rank": start+i+1
            })

        return JSONResponse({
            "items": items,
            "total_hits": total,
            "page": page,
            "page_size": page_size,
            "has_more": end < total,
            "next_page": (page + 1) if end < total else None,
            "error": None,
            "order_used": order
        }, headers=JSON_HEADERS)
    except Exception as e:
        return JSONResponse({"items":[], "total_hits":0, "page":1, "page_size":page_size, "has_more":False, "next_page":None, "error":str(e), "order_used":order}, headers=JSON_HEADERS)

@app.get("/")
def root():
    return PlainTextResponse("Mini Rose Search JSONL API running.\n", headers={"content-type":"text/plain; charset=utf-8"})
