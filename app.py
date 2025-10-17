# app.py — Mini Rose Search API（JSONL：タイトル補完＋合成ヘッダ＋フレーズ/近接＋関連順を既定）
# 版: jsonl-2025-10-17-relevance-solid

import os, re, json, unicodedata, datetime as dt, hashlib
from typing import List, Dict, Any, Tuple, Optional
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, PlainTextResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import requests

# ==== 設定 ====
KB_URL = os.getenv("KB_URL", "https://raw.githubusercontent.com/hiroshiyamagishi1960-lang/mini-rose-search-jsonl/main/kb.jsonl")
JSON_HEADERS = {"content-type": "application/json; charset=utf-8"}

app = FastAPI(title="Mini Rose Search API", version="jsonl-2025-10-17-relevance-solid")
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
# 正規化ユーティリティ
# =============================
def _nfkc(s: Optional[str]) -> str:
    return unicodedata.normalize("NFKC", s or "").strip()

def _lower(s: str) -> str:
    return (_nfkc(s)).lower()

def _first_non_empty(*vals: Any) -> str:
    for v in vals:
        t = _nfkc(v if isinstance(v, str) else (json.dumps(v, ensure_ascii=False) if v not in (None, "") else ""))
        if t:
            return t
    return ""

# =============================
# かなフォールディング（表記ゆれ吸収）
# =============================
_SMALL_TO_BASE = str.maketrans({"ぁ":"あ","ぃ":"い","ぅ":"う","ぇ":"え","ぉ":"お","ゃ":"や","ゅ":"ゆ","ょ":"よ","ゎ":"わ","っ":"つ","ゕ":"か","ゖ":"け"})
def _kana_to_hira(s:str)->str:
    out=[]
    for ch in s:
        o=ord(ch)
        if 0x30A1<=o<=0x30F6: out.append(chr(o-0x60))
        elif ch in("ヵ","ヶ"): out.append({"ヵ":"か","ヶ":"け"}[ch])
        else: out.append(ch)
    return "".join(out)

def _long_to_vowel(prev:str)->str:
    A=set("あかさたなはまやらわがざだばぱぁゃゎっ"); I=set("いきしちにひみりぎじぢびぴぃ")
    U=set("うくすつぬふむゆるぐずづぶぷぅゅっ"); E=set("えけせてねへめれげぜでべぺぇ")
    O=set("おこそとのほもよろをごぞどぼぽぉょ")
    if not prev: return ""
    if prev in A: return "あ"
    if prev in I: return "い"
    if prev in U: return "う"
    if prev in E: return "え"
    if prev in O: return "お"
    return ""

def fold_kana(s:str)->str:
    if not s: return ""
    s=unicodedata.normalize("NFKC",s)
    s=_kana_to_hira(s)
    s=s.translate(_SMALL_TO_BASE)
    buf=[]
    for ch in s:
        buf.append(_long_to_vowel(buf[-1]) if ch=="ー" else ch)
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
    # イベント
    "コンテスト":{"大会","表彰","コンクール"},
    "結果":{"発表","報告","結果発表"},
}
REVERSE_EQ: Dict[str,set] = {v:{k} for k,vs in KANJI_EQ.items() for v in vs}
for k,vs in KANJI_EQ.items():
    for v in vs:
        REVERSE_EQ.setdefault(v,set()).add(k)
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
        r=requests.get(url,timeout=20); r.raise_for_status()
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
# 年抽出・代表日（latest用）
# =============================
def _years_from_text(s:str)->List[int]:
    return sorted({int(y) for y in re.findall(r"(19|20|21)\d{2}", _nfkc(s))})

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
# クエリ解析
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
    if not q: return []
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
# タイトル補完・合成ヘッダ
# =============================
# よくある別名キー
TITLE_KEYS = ["title","見出し","タイトル","name","heading"]
BODY_KEYS  = ["content","text","body","本文","テキスト"]
ISSUE_KEYS = ["issue","会報号","号","会報"]
DATE_KEYS  = ["date","発行日","開催日","日付"]

def _get_by_keys(rec:Dict[str,Any], keys:List[str])->str:
    for k in keys:
        if k in rec and _nfkc(rec[k]):
            return _nfkc(rec[k])
    return ""

def _first_nonempty_line(text:str)->str:
    for ln in (text or "").splitlines():
        t=_nfkc(ln)
        if t:
            # 句点や読点で早めに切る（見出し風）
            m=re.split(r"[。．、,，/｜|／]", t, 1)
            return (m[0] or t)[:80]
    return ""

def synth_title(rec:Dict[str,Any])->str:
    # 元のtitle
    raw_title = _get_by_keys(rec, TITLE_KEYS)
    # 本文の先頭行
    body = _get_by_keys(rec, BODY_KEYS)
    firstline = _first_nonempty_line(body)
    # 号・日付（あれば）
    issue = _get_by_keys(rec, ISSUE_KEYS)
    date  = _get_by_keys(rec, DATE_KEYS)

    base = raw_title if raw_title and raw_title!="(無題)" else (firstline if firstline else "(無題)")
    parts=[]
    # "第NN号" 整形
    if issue:
        m=re.search(r"\d+", issue)
        parts.append(f"第{m.group(0)}号" if m else issue)
    # 年を抽出して付加（重複は避ける）
    yrs=_years_from_text(date or "")
    if yrs:
        parts.append(str(max(yrs)))
    # 最後に本文由来/元titleのベースを置く
    parts.append(base)
    # 合成
    title=" | ".join([p for p in parts if p])
    return title or "(無題)"

# =============================
# ハイライト・スニペット
# =============================
def _record_text_all(rec:Dict[str,Any])->str:
    buf=[]
    def walk(x):
        if isinstance(x,dict):
            for v in x.values(): walk(v)
        elif isinstance(x,list):
            for v in x: walk(v)
        elif isinstance(x,str):
            s=_nfkc(x)
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
    if not text: return False
    base=[x for x in terms_base if x]
    for i in range(len(base)):
        for j in range(i+1,len(base)):
            a,b=base[i], base[j]
            pat=re.compile(re.escape(a)+r".{0,%d}"%w+re.escape(b)+"|"+re.escape(b)+r".{0,%d}"%w+re.escape(a))
            if pat.search(text): return True
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

def _score_record(rec:Dict[str,Any], terms_all:List[str], base_terms:List[str], q_raw:str, title_for_score:str, body:str)->float:
    low=lambda s:(s or "").lower(); fk=lambda s:fold_kana(s or "")
    t_low, b_low = low(title_for_score), low(body)
    t_f,   b_f   = fk(title_for_score), fk(body)

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

    # 近接
    if _near_cooccur(title_for_score, base_terms, COOCCUR_WINDOW): sc+=COOCCUR_TITLE_BONUS; matched=True
    if _near_cooccur(body,           base_terms, COOCCUR_WINDOW): sc+=COOCCUR_BODY_BONUS;  matched=True

    # 「コンテスト結果」専用ブースト（直読み版体感の再現）
    if ("コンテスト結果" in title_for_score) or ("コンテスト結果" in body):
        sc += 1.8  # 軽ブースト（順位安定用）

    # 最後の救済（全フィールド走査）
    if not matched:
        alltxt=_record_text_all(rec); a_low=low(alltxt); a_f=fk(alltxt)
        for t in terms_all:
            tl=t.lower(); tf=fold_kana(t)
            if tl in a_low or tf in a_f:
                sc+=W_TEXT*0.7; matched=True; break
    return sc if matched else 0.0

# =============================
# 検索本体（JSONL）
# =============================
def parse_year_from_query(q:str)->Tuple[str,Optional[int],Optional[Tuple[int,int]]]:
    qn=_nfkc(q).replace("　"," ").strip()
    if not qn: return "", None, None
    parts=qn.split(); last=parts[-1]
    m1=re.fullmatch(r"(19|20|21)\d{2}", last)
    if m1: return (" ".join(parts[:-1]).strip(), int(last), None)
    m2=re.fullmatch(rf"((?:19|20|21)\d{{2}})\s*(?:-|–|—|~|〜|～|\.\.)\s*((?:19|20|21)\d{{2}})", last)
    if m2:
        y1,y2=int(m2.group(1)), int(m2.group(2))
        if y1>y2: y1,y2=y2,y1
        return (" ".join(parts[:-1]).strip(), None, (y1,y2))
    return (qn, None, None)

def search_jsonl_scored(q:str, year=None, year_from=None, year_to=None)->Tuple[List[Tuple[float,dt.date,Dict[str,Any],str,str]], List[str]]:
    if not KB_DATA: return [], []
    base_terms=jp_terms(q)
    terms_all=expand_terms(base_terms)
    if not terms_all: return [], []
    scored=[]
    for rec in KB_DATA:
        raw_title = _get_by_keys(rec, TITLE_KEYS)
        body      = _get_by_keys(rec, BODY_KEYS)
        # タイトル補完＋合成
        title_syn = synth_title(rec)

        # 粗フィルタ（どこかに当たれば候補へ）
        def _hit_any():
            for txt in (title_syn, body, _record_text_all(rec)):
                t_low=(_lower(txt)); t_fold=fold_kana(txt)
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

        score=_score_record(rec, terms_all, base_terms, q, title_syn, body)
        if score<=0: continue
        scored.append((score, _best_date(rec), rec, title_syn, body))
    return scored, base_terms

# =============================
# API
# =============================
@app.get("/health")
def health():
    # kbの簡易指紋も返す（行数＋SHA）
    sha=hashlib.sha256()
    for rec in KB_DATA:
        t=_nfkc(rec.get("title","")) + _nfkc(rec.get("url",""))
        sha.update(t.encode("utf-8"))
    return JSONResponse({"ok":True,"kb_url":KB_URL,"kb_size":len(KB_DATA),"kb_fingerprint":sha.hexdigest()}, headers=JSON_HEADERS)

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

        # 並べ替え：関連順（既定） or 最新順 —— ★ページング前で確定★
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
        for i,(score, d, rec, title_syn, body) in enumerate(slice_):
            # ヒット箇所で抜粋＋<mark>
            title_pos=_find_first(title_syn, hl_keys)
            body_pos =_find_first(body,      hl_keys)
            snippet,_=_make_snippet_and_highlight(body, hl_keys, ctx=90, maxlen=360)
            tag=""
            if title_pos is not None: tag="（タイトルにヒット）"
            elif body_pos is not None: tag="（本文にヒット）"
            items.append({
                "title": title_syn + (tag or ""),
                "content": snippet,
                "url": _first_non_empty(rec.get("url",""), rec.get("source","")),
                "source": _first_non_empty(rec.get("url",""), rec.get("source","")),
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
