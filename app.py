# app.py — Mini Rose Search API（JSONL版：mini-rose-search-jsonl 用）
# 方針反映版：日本語短語ファジー抑止 / 代表日=開催日/発行日 / order=latest でページング前ソート / UI変更なし
# 版: jsonl-2025-10-16-stable+fix-content-text-body-description

import os, re, json, unicodedata, datetime as dt
from typing import List, Dict, Any, Tuple, Optional
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, PlainTextResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import requests
from urllib.parse import urlparse, urlunparse

# ==== 環境変数 ====
KB_URL = os.getenv("KB_URL", "https://raw.githubusercontent.com/hiroshiyamagishi1960-lang/mini-rose-search-jsonl/main/kb.jsonl")
JSON_HEADERS = {"content-type": "application/json; charset=utf-8"}

app = FastAPI(title="Mini Rose Search API", version="jsonl-2025-10-16-stable+fix")
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
# 日本語かなフォールディング／同義語辞書
# =============================
def _nfkc(s: Optional[str]) -> str:
    return unicodedata.normalize("NFKC", s or "")

_SMALL_TO_BASE = str.maketrans({"ぁ":"あ","ぃ":"い","ぅ":"う","ぇ":"え","ぉ":"お","ゃ":"や","ゅ":"ゆ","ょ":"よ","ゎ":"わ","っ":"つ","ゕ":"か","ゖ":"け"})

_A_SET=set("あかさたなはまやらわがざだばぱぁゃゎっ"); _I_SET=set("いきしちにひみりぎじぢびぴぃ")
_U_SET=set("うくすつぬふむゆるぐずづぶぷぅゅっ"); _E_SET=set("えけせてねへめれげぜでべぺぇ")
_O_SET=set("おこそとのほもよろをごぞどぼぽぉょ")

def _kana_to_hira(s:str)->str:
    out=[]
    for ch in s:
        code=ord(ch)
        if 0x30A1<=code<=0x30F6: out.append(chr(code-0x60))
        elif ch in("ヵ","ヶ"): out.append({"ヵ":"か","ヶ":"け"}[ch])
        else: out.append(ch)
    return "".join(out)

def _long_to_vowel(prev:str)->str:
    if not prev:return""
    if prev in _A_SET:return"あ"
    if prev in _I_SET:return"い"
    if prev in _U_SET:return"う"
    if prev in _E_SET:return"え"
    if prev in _O_SET:return"お"
    return""

def fold_kana(s:str)->str:
    if not s:return""
    s=unicodedata.normalize("NFKC",s); s=_kana_to_hira(s); s=s.translate(_SMALL_TO_BASE)
    buf=[]
    for ch in s:
        buf.append(_long_to_vowel(buf[-1]) if ch=="ー" else ch)
    d=unicodedata.normalize("NFD","".join(buf))
    d="".join(c for c in d if ord(c)not in(0x3099,0x309A))
    return unicodedata.normalize("NFC",d).lower().strip()

def hira_to_kata(s:str)->str:
    out=[]
    for ch in s:
        code=ord(ch)
        if 0x3041<=code<=0x3096: out.append(chr(code+0x60))
        elif ch in("ゕ","ゖ"): out.append({"ゕ":"ヵ","ゖ":"ヶ"}[ch])
        else: out.append(ch)
    return"".join(out)

KANJI_EQ={"苔":{"こけ","コケ"},"剪定":{"せん定","せんてい"},"施肥":{"肥料","追肥"},"用土":{"土","土の配合"},"挿し木":{"さし木","さし芽"},"接ぎ木":{"つぎ木","つぎき"},"植え替え":{"うえ替え","うえかえ"},"黒星病":{"クロボシ","黒点病"},"薔薇":{"バラ","ばら"},"ミニバラ":{"ミニ薔薇","みにばら"}}
REVERSE_EQ={v: {k} for k,vs in KANJI_EQ.items() for v in vs}
for k,vs in KANJI_EQ.items():
    for v in vs: REVERSE_EQ.setdefault(v,set()).add(k)
    REVERSE_EQ.setdefault(k,set()).add(k)

def expand_with_domain_dict(term:str)->set:
    out=set()
    if term in KANJI_EQ: out|=KANJI_EQ[term]
    if term in REVERSE_EQ: out|=REVERSE_EQ[term]
    return out

# =============================
# ユーティリティ
# =============================
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
                try: items.append(json.loads(line))
                except: pass
    except Exception as e:
        print("[WARN] KB load failed:",e)
    return items

KB_DATA=_load_kb(KB_URL)
print(f"[INIT] KB loaded: {len(KB_DATA)} records from {KB_URL}")

# =============================
# 年フィルタ／代表日抽出
# =============================
def _nfkcnum(s:str)->str: return unicodedata.normalize("NFKC",s or "")

def _parse_year_any(s:str)->Optional[int]:
    m=re.search(r"(19|20|21)\d{2}",_nfkcnum(s))
    return int(m.group(0)) if m else None

def _years_from_text(s:str)->List[int]:
    ys=[int(y)for y in re.findall(r"(19|20|21)\d{2}",_nfkcnum(s))]
    return sorted(set(ys))

def _record_years(r:Dict[str,Any])->List[int]:
    ys=set()
    for k in("issue","date_primary","title","text","content","body","description","url"):
        ys.update(_years_from_text(str(r.get(k,""))))
    return sorted(ys)

def _best_date(rec:Dict[str,Any])->dt.date:
    ys=_record_years(rec)
    return dt.date(max(ys),1,1) if ys else dt.date(1970,1,1)

# =============================
# 検索ロジック
# =============================
def jp_terms(q:str)->List[str]:
    if not q:return[]
    qn=_nfkc(q).replace("　"," ")
    toks=[t for t in qn.split() if t]
    return list(dict.fromkeys(toks))

def expand_terms(terms:List[str])->List[str]:
    out=set()
    for t in terms:
        out|={t,fold_kana(t),hira_to_kata(fold_kana(t))}
        out|=expand_with_domain_dict(t)
    return sorted(out)

def _match_text(text:str,terms:List[str])->bool:
    if not text:return False
    t_low=text.lower(); t_fold=fold_kana(text)
    for term in terms:
        if term.lower() in t_low or fold_kana(term) in t_fold:
            return True
    return False

def search_jsonl(q:str, year=None, year_from=None, year_to=None)->List[Dict[str,Any]]:
    if not KB_DATA:
        return []
    terms = expand_terms(jp_terms(q))
    if not terms:
        return []

    results = []
    for rec in KB_DATA:
        title = str(rec.get("title", ""))
        body  = str(_pick(rec.get("content",""), rec.get("text",""), rec.get("body",""), rec.get("description","")))

        # ★ 検索ヒット判定：title または 本文（content/text/body/description）
        if _match_text(title, terms) or _match_text(body, terms):
            # 年フィルタ（必要なときだけ適用）
            if year or year_from or year_to:
                ys = _record_years(rec)
                if not ys:
                    continue
                lo = year_from or -10**9
                hi = year_to   or  10**9
                if year and year not in ys:
                    continue
                if not any(lo <= y <= hi for y in ys):
                    continue
            results.append(rec)

    # 新しいもの順
    results.sort(key=lambda r:_best_date(r), reverse=True)
    return results

# =============================
# API
# =============================
@app.get("/health")
def health():
    return JSONResponse({"ok":True,"kb_url":KB_URL,"kb_size":len(KB_DATA)},headers=JSON_HEADERS)

@app.get("/version")
def version():
    return JSONResponse({"version":app.version},headers=JSON_HEADERS)

@app.get("/diag")
def diag(q:str=Query("",description="確認")):
    return JSONResponse({"query":q,"years":_years_from_text(q)},headers=JSON_HEADERS)

@app.get("/api/search")
def api_search(q:str=Query("",description="検索語"),page:int=1,page_size:int=5,order:str="latest"):
    try:
        if not q.strip():
            return JSONResponse({"items":[],"total_hits":0,"page":1,"page_size":page_size,"has_more":False,"next_page":None,"error":None,"order_used":order},headers=JSON_HEADERS)
        ranked=search_jsonl(q)
        total=len(ranked)
        start=(page-1)*page_size; end=start+page_size
        slice_=ranked[start:end]
        items=[]
        for i, r in enumerate(slice_):
            body = _pick(r.get("content",""), r.get("text",""), r.get("body",""), r.get("description",""))
            items.append({
                "title": r.get("title",""),
                "content": body[:900],  # 先頭だけ抜粋
                "url": r.get("url",""),
                "source": r.get("url",""),
                "rank": start + i + 1
            })
        return JSONResponse({
            "items":items,
            "total_hits":total,
            "page":page,
            "page_size":page_size,
            "has_more":end<total,
            "next_page":(page+1) if end<total else None,
            "error":None,
            "order_used":order
        },headers=JSON_HEADERS)
    except Exception as e:
        return JSONResponse({"items":[],"total_hits":0,"page":1,"page_size":page_size,"has_more":False,"next_page":None,"error":str(e),"order_used":order},headers=JSON_HEADERS)

@app.get("/")
def root():
    return PlainTextResponse("Mini Rose Search JSONL API running.\n",headers={"content-type":"text/plain; charset=utf-8"})
