# app.py — Mini Rose Search API（JSONL：フレーズ優先＋近接加点＋関連順を既定）
# 版: jsonl-2025-10-17-relevance-boost

import os, re, json, unicodedata, datetime as dt
from typing import List, Dict, Any, Tuple, Optional
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, PlainTextResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import requests

# ==== 設定 ====
KB_URL = os.getenv("KB_URL", "https://raw.githubusercontent.com/hiroshiyamagishi1960-lang/mini-rose-search-jsonl/main/kb.jsonl")
JSON_HEADERS = {"content-type": "application/json; charset=utf-8"}

app = FastAPI(title="Mini Rose Search API", version="jsonl-2025-10-17-relevance-boost")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"], allow_credentials=True)

# ==== UI（/ui は no-store で常に最新）====
if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/ui", response_class=HTMLResponse)
def ui():
    path = "static/ui.html"
    if os.path.isfile(path):
        with open(path, "r", encoding="utf-8") as f:
            html = f.read()
        return HTMLResponse(html, headers={"Cache-Control": "no-store"})
    return HTMLResponse("<h1>UI not found</h1>", headers={"Cache-Control": "no-store"})

# =============================
# かなフォールディング（表記ゆれ吸収）
# =============================
def _nfkc(s: Optional[str]) -> str: return unicodedata.normalize("NFKC", s or "")
_SMALL_TO_BASE = str.maketrans({"ぁ":"あ","ぃ":"い","ぅ":"う","ぇ":"え","ぉ":"お","ゃ":"や","ゅ":"ゆ","ょ":"よ","ゎ":"わ","っ":"つ","ゕ":"か","ゖ":"け"})
_A=set("あかさたなはまやらわがざだばぱぁゃゎっ"); _I=set("いきしちにひみりぎじぢびぴぃ")
_U=set("うくすつぬふむゆるぐずづぶぷぅゅっ"); _E=set("えけせてねへめれげぜでべぺぇ"); _O=set("おこそとのほもよろをごぞどぼぽぉょ")
def _kana_to_hira(s:str)->str:
    out=[]; 
    for ch in s:
        o=ord(ch)
        if 0x30A1<=o<=0x30F6: out.append(chr(o-0x60))
        elif ch in("ヵ","ヶ"): out.append({"ヵ":"か","ヶ":"け"}[ch])
        else: out.append(ch)
    return "".join(out)
def _long_to_vowel(prev:str)->str:
    if not prev:return""
    if prev in _A:return"あ"
    if prev in _I:return"い"
    if prev in _U:return"う"
    if prev in _E:return"え"
    if prev in _O:return"お"
    return""
def fold_kana(s:str)->str:
    if not s:return""
    s=unicodedata.normalize("NFKC",s); s=_kana_to_hira(s); s=s.translate(_SMALL_TO_BASE)
    buf=[]
    for ch in s: buf.append(_long_to_vowel(buf[-1]) if ch=="ー" else ch)
    d=unicodedata.normalize("NFD","".join(buf))
    d="".join(c for c in d if ord(c)not in(0x3099,0x309A))
    return unicodedata.normalize("NFC",d).lower().strip()
def hira_to_kata(s:str)->str:
    out=[]
    for ch in s:
        o=ord(ch)
        if 0x3041<=o<=0x3096: out.append(chr(o+0x60))
        elif ch in("ゕ","ゖ"): out.append({"ゕ":"ヵ","ゖ":"ヶ"}[ch])
        else: out.append(ch)
    return "".join(out)

# =============================
# 同義語辞書（ドメイン語彙）
# =============================
KANJI_EQ: Dict[str,set] = {
    "苔":{"こけ","コケ"},
    "剪定":{"せん定","せんてい"},
    "施肥":{"肥料","追肥"},
    "用土":{"土","土の配合"},
    "挿し木":{"さし木","さし芽"},
    "接ぎ木":{"つぎ木","つぎき","接木"},
    "植え替え":{"うえ替え","うえかえ"},
    "黒星病":{"クロボシ","黒点病"},
    "薔薇":{"バラ","ばら"},
    "ミニバラ":{"ミニ薔薇","みにばら"},
    # イベント関連
    "コンテスト":{"大会","表彰","コンクール"},
    "結果":{"発表","報告","結果発表"},
}
REVERSE_EQ: Dict[str,set] = {v:{k} for k,vs in KANJI_EQ.items() for v in vs}
for k,vs in KANJI_EQ.items():
    for v in vs: REVERSE_EQ.setdefault(v,set()).add(k)
    REVERSE_EQ.setdefault(k,set()).add(k)
def expand_with_domain_dict(term:str)->set:
    out=set()
    if term in KANJI_EQ: out|=KANJI_EQ[term]
    if term in REVERSE_EQ: out|=REVERSE_EQ[term]
    return out

# =============================
# KBロード（JSONL）
# =============================
def _load_kb(url:str)->List[Dict[str,Any]]:
    items=[]
    try:
        r=requests.get(url,timeout=15); r.raise_for_status()
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
# 年抽出・代表日（年フィルタ／latest用）
# =============================
def _nfkcnum(s:str)->str: return unicodedata.normalize("NFKC",s or "")
_RANGE_SEP = r"(?:-|–|—|~|〜|～|\.{2})"
def parse_year_from_query(q:str)->Tuple[str,Optional[int],Optional[int]]:
    qn=_nfkc(q).strip().replace("　"," ")
    if not qn:return "",None,None
    parts=qn.split(); last=parts[-1]
    m1=re.fullmatch(r"(19|20|21)\d{2}", last)
    if m1: return (" ".join(parts[:-1]).strip(), int(last), None)
    m2=re.fullmatch(rf"((?:19|20|21)\d{{2}})\s*{_RANGE_SEP}\s*((?:19|20|21)\d{{2}})", last)
    if m2:
        y1,y2=int(m2.group(1)),int(m2.group(2))
        if y1>y2:y1,y2=y2,y1
        return (" ".join(parts[:-1]).strip(), None, (y1,y2))
    return (qn, None, None)
def _years_from_text(s:str)->List[int]:
    return sorted({int(y) for y in re.findall(r"(19|20|21)\d{2}", _nfkcnum(s))})
def _record_years(r:Dict[str,Any])->List[int]:
    ys=set()
    def walk(x):
        if isinstance(x,dict):
            for v in x.values(): walk(v)
        elif isinstance(x,list):
            for v in x: walk(v)
        elif isinstance(x,str):
            ys.update(_years_from_text(x))
    walk(r); return sorted(ys)
def _best_date(rec:Dict[str,Any])->dt.date:
    ys=_record_years(rec)
    return dt.date(max(ys),1,1) if ys else dt.date(1970,1,1)

# =============================
# クエリ解析（語の抽出／複合語救済）
# =============================
_JP_WORDS = re.compile(r"[一-龥ぁ-んァ-ンー]{2,}|[A-Za-z0-9]{2,}")
SPLIT_TOKENS=("結果","発表","報告","案内","募集","開催","決定")
def normalize_query(q:str)->str:
    s=_nfkc(q or "")
    for a,b in [("接木","接ぎ木"),("つぎ木","接ぎ木"),("土作り","土の作り方"),("土づくり","土の作り方")]:
        s=s.replace(a,b)
    return s.strip()
def split_compound(term:str)->List[str]:
    outs=[term]
    for key in SPLIT_TOKENS:
        if key in term and term!=key:
            parts=term.split(key)
            left=parts[0].strip()
            if left: outs.append(left)
            outs.append(key)
    return list(dict.fromkeys([t for t in outs if t]))
def jp_terms(q:str)->List[str]:
    if not q:return []
    qn=normalize_query(q).replace("　"," ")
    toks=[t for t in qn.split() if t]
    toks+=_JP_WORDS.findall(qn)
    if len([t for t in qn.split() if t])==1 and len(toks)<=3:
        toks = list(dict.fromkeys(toks + split_compound(qn)))
    uniq=[]; seen=set()
    for t in sorted(set(toks), key=len, reverse=True):
        if t not in seen:
            uniq.append(t); seen.add(t)
        if len(uniq)>=5: break
    return uniq
def expand_terms(terms:List[str])->List[str]:
    out=set()
    for t in terms:
        ft=fold_kana(t)
        out|={t, ft, hira_to_kata(ft)}
        out|=expand_with_domain_dict(t)
    return sorted({s for s in out if s})

# =============================
# ハイライト・スニペット（当たり位置抜粋）
# =============================
def _record_text_all(rec:Dict[str,Any])->str:
    buf=[]
    def walk(x):
        if isinstance(x,dict):
            for v in x.values(): walk(v)
        elif isinstance(x,list):
            for v in x: walk(v)
        elif isinstance(x,str):
            s=x.strip()
            if s: buf.append(s)
    walk(rec); return "\n".join(buf)
def _html_escape(s:str)->str:
    return s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
def _find_first(text:str, keys:List[str])->Optional[int]:
    best=None
    for k in keys:
        if not k: continue
        m=re.search(re.escape(k), text or "", flags=re.IGNORECASE)
        if m:
            p=m.start()
            if best is None or p<best: best=p
    return best
def _build_hl_keys(terms:List[str])->List[str]:
    hs=set()
    for t in terms:
        hs.add(t); hs|=expand_with_domain_dict(t)
        ft=fold_kana(t); hs.add(ft); hs.add(hira_to_kata(ft))
    return sorted({h for h in hs if h}, key=len, reverse=True)
def _make_snippet_and_highlight(text:str, keys:List[str], ctx:int=90, maxlen:int=360)->Tuple[str,bool]:
    if not text: return ("", False)
    pos=_find_first(text, keys)
    if pos is None:
        raw=text[:maxlen]; return (_html_escape(raw), False)
    start=max(0,pos-ctx); end=min(len(text), pos+ctx)
    raw=text[start:end]
    esc=_html_escape(raw)
    for k in keys:
        if not k: continue
        esc=re.compile(re.escape(_html_escape(k)), re.IGNORECASE).sub(lambda m:f"<mark>{m.group(0)}</mark>", esc)
    prefix="…" if start>0 else ""; suffix="…" if end<len(text) else ""
    snip=prefix+esc+suffix
    return (snip if len(snip)<=maxlen+40 else snip[:maxlen]+"…", True)

# =============================
# スコアリング（関連度）＋フレーズ／近接加点
# =============================
W_TITLE, W_TEXT = 2.0, 1.4
PHRASE_TITLE_BONUS = 4.0
PHRASE_BODY_BONUS  = 2.2
COOCCUR_WINDOW = 30
COOCCUR_TITLE_BONUS = 1.5
COOCCUR_BODY_BONUS  = 1.2

def _near_cooccur(text:str, terms_base:List[str], w:int)->bool:
    t=text or ""
    base=[x for x in terms_base if x]
    for i in range(len(base)):
        for j in range(i+1,len(base)):
            a,b=base[i], base[j]
            pat=re.compile(re.escape(a)+r".{0,%d}"%w+re.escape(b)+"|"+re.escape(b)+r".{0,%d}"%w+re.escape(a))
            if pat.search(t): return True
    return False

def _phrase_candidates(q_raw:str, base_terms:List[str])->List[str]:
    c=set()
    qn=_nfkc(q_raw).strip()
    if qn: c.add(qn)
    if len(base_terms)>=2:
        for i in range(len(base_terms)):
            for j in range(i+1,len(base_terms)):
                c.add(base_terms[i]+base_terms[j])
                c.add(base_terms[j]+base_terms[i])
    return sorted({x for x in c if len(x)>=2}, key=len, reverse=True)

def _score_record(rec:Dict[str,Any], terms_all:List[str], base_terms:List[str], q_raw:str)->float:
    title=str(rec.get("title",""))
    body =str(rec.get("content","") or rec.get("text","") or rec.get("body",""))
    low=lambda s:(s or "").lower(); fk=lambda s:fold_kana(s or "")
    t_low, b_low = low(title), low(body)
    t_f, b_f = fk(title), fk(body)

    sc=0.0; matched=False
    for t in terms_all:
        tl=t.lower(); tf=fold_kana(t)
        if tl in t_low: sc+=W_TITLE; matched=True
        if tl in b_low: sc+=W_TEXT;  matched=True
        if tf in t_f:   sc+=W_TITLE*0.95; matched=True
        if tf in b_f:   sc+=W_TEXT*0.95;  matched=True

    # フレーズ一致（タイトル＞本文）
    for ph in _phrase_candidates(q_raw, base_terms):
        ph_f=fold_kana(ph)
        if ph.lower() in t_low or ph_f in t_f: sc+=PHRASE_TITLE_BONUS; matched=True
        if ph.lower() in b_low or ph_f in b_f: sc+=PHRASE_BODY_BONUS;  matched=True

    # 近接（“コンテスト”と“結果”が近い等）
    if _near_cooccur(title, base_terms, COOCCUR_WINDOW): sc+=COOCCUR_TITLE_BONUS; matched=True
    if _near_cooccur(body,  base_terms, COOCCUR_WINDOW): sc+=COOCCUR_BODY_BONUS;  matched=True

    # 取りこぼし救済（全フィールド走査）
    if not matched:
        alltxt=_record_text_all(rec); a_low=low(alltxt); a_f=fk(alltxt)
        for t in terms_all:
            tl=t.lower(); tf=fold_kana(t)
            if tl in a_low or tf in a_f:
                sc+=W_TEXT*0.7; matched=True; break
    return sc if matched else 0.0

def search_jsonl_scored(q:str, year=None, year_from=None, year_to=None)->Tuple[List[Tuple[float,dt.date,Dict[str,Any]]], List[str]]:
    if not KB_DATA: return [], []
    base_terms=jp_terms(q)
    terms_all=expand_terms(base_terms)
    if not terms_all: return [], []
    scored=[]
    for rec in KB_DATA:
        title=str(rec.get("title",""))
        body =str(rec.get("content","") or rec.get("text","") or rec.get("body",""))
        alltxt=_record_text_all(rec)

        # 粗フィルタ
        def _hit_any():
            for txt in (title, body, alltxt):
                t_low=txt.lower(); t_fold=fold_kana(txt)
                for term in terms_all:
                    if term.lower() in t_low or fold_kana(term) in t_fold:
                        return True
            return False
        if not _hit_any(): continue

        # 年フィルタ
        if year or year_from or year_to:
            ys=_record_years(rec)
            if not ys: 
                continue
            lo=year_from if year_from is not None else -10**9
            hi=year_to   if year_to   is not None else  10**9
            if year is not None and year not in ys: continue
            if year is None and not any(lo<=y<=hi for y in ys): continue

        score=_score_record(rec, terms_all, base_terms, q)
        if score<=0: continue
        scored.append((score, _best_date(rec), rec))
    return scored, base_terms

# =============================
# API
# =============================
@app.get("/health")
def health():
    return JSONResponse({"ok":True,"kb_url":KB_URL,"kb_size":len(KB_DATA)}, headers=JSON_HEADERS)

@app.get("/version")
def version():
    return JSONResponse({"version": app.version}, headers=JSON_HEADERS)

@app.get("/diag")
def diag(q:str=Query("", description="確認")):
    base,y,yr=parse_year_from_query(q); yf,yt=(None,None)
    if isinstance(yr,tuple): yf,yt=yr
    return JSONResponse({"query":q,"base":base,"year":y,"year_from":yf,"year_to":yt}, headers=JSON_HEADERS)

@app.get("/api/search")
def api_search(
    q:str=Query("", description="検索語（末尾に年/年範囲も可）"),
    page:int=1,
    page_size:int=5,
    order:str=Query("relevance", description="relevance（関連順） | latest（最新順）")
):
    try:
        q_raw=(q or "").strip()
        if not q_raw:
            return JSONResponse({"items":[],"total_hits":0,"page":1,"page_size":page_size,"has_more":False,"next_page":None,"error":None,"order_used":order}, headers=JSON_HEADERS)

        base_q, y, yr = parse_year_from_query(q_raw)
        y_from=y_to=None
        if isinstance(yr,tuple): y_from,y_to=yr

        scored, base_terms = search_jsonl_scored(base_q, year=y, year_from=y_from, year_to=y_to)
        total=len(scored)
        if total==0:
            return JSONResponse({"items":[],"total_hits":0,"page":1,"page_size":page_size,"has_more":False,"next_page":None,"error":None,"order_used":order}, headers=JSON_HEADERS)

        # 並べ替え：関連順（既定） or 最新順
        if order.lower()=="latest":
            scored.sort(key=lambda t:(t[1], t[0]), reverse=True)   # date → score
        else:
            scored.sort(key=lambda t:(t[0], t[1]), reverse=True)   # score → date
            order="relevance"

        # ページング
        if page<1: page=1
        if page_size<1: page_size=5
        start=(page-1)*page_size; end=start+page_size
        slice_=scored[start:end]

        # ハイライト用キー
        hl_keys=_build_hl_keys(expand_terms(base_terms))

        items=[]
        for i,(score, d, r) in enumerate(slice_):
            title=str(r.get("title","") or "(無題)")
            body =str(r.get("content","") or r.get("text","") or r.get("body",""))
            # ヒット箇所で抜粋＋<mark>
            title_pos=_find_first(title, hl_keys)
            body_pos =_find_first(body,  hl_keys)
            snippet,_=_make_snippet_and_highlight(body, hl_keys, ctx=90, maxlen=360)
            tag=""
            if body_pos is not None: tag="（本文にヒット）"
            elif title_pos is not None: tag="（タイトルにヒット）"
            items.append({
                "title": title + (tag or ""),
                "content": snippet,
                "url": r.get("url",""),
                "source": r.get("url",""),
                "rank": start+i+1
            })

        return JSONResponse({
            "items": items,
            "total_hits": total,
            "page": page,
            "page_size": page_size,
            "has_more": end<total,
            "next_page": (page+1) if end<total else None,
            "error": None,
            "order_used": order
        }, headers=JSON_HEADERS)

    except Exception as e:
        return JSONResponse({"items":[],"total_hits":0,"page":1,"page_size":page_size,"has_more":False,"next_page":None,"error":str(e),"order_used":order}, headers=JSON_HEADERS)

@app.get("/")
def root():
    return PlainTextResponse("Mini Rose Search JSONL API running.\n", headers={"content-type":"text/plain; charset=utf-8", "Cache-Control":"no-store"})
