# app.py — Mini Rose Search API（JSONL版）
# 目的: ハイライト＆ヒット位置抜粋（スニペット）、複合語「コンテスト結果」分割、
#       かなフォールディング、同義語辞書、年フィルタ、order=latest の前に並びを確定（ページング前ソート）、
#       UI互換（/ui）、/health /diag /version の維持
# 版: jsonl-2025-10-16-mark-snippet-solid

import os, re, json, unicodedata, datetime as dt
from typing import List, Dict, Any, Tuple, Optional
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, PlainTextResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import requests

# ========= 設定 =========
KB_URL = os.getenv("KB_URL", "https://raw.githubusercontent.com/hiroshiyamagishi1960-lang/mini-rose-search-jsonl/main/kb.jsonl")
JSON_HEADERS = {"content-type": "application/json; charset=utf-8"}

app = FastAPI(title="Mini Rose Search API", version="jsonl-2025-10-16-mark-snippet-solid")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"], allow_credentials=True
)

# ========= UI（旧UI互換：/ui は no-store） =========
if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/ui", response_class=HTMLResponse)
def ui():
    path = "static/ui.html"
    if os.path.isfile(path):
        with open(path, "r", encoding="utf-8") as f:
            html = f.read()
        return HTMLResponse(html, headers={"Cache-Control": "no-store"})
    return HTMLResponse("<h1>UI not found</h1><p>Put static/ui.html</p>", headers={"Cache-Control": "no-store"})

# ========= 日本語正規化・かなフォールディング =========
def _nfkc(s: Optional[str]) -> str:
    return unicodedata.normalize("NFKC", s or "")

_SMALL_TO_BASE = str.maketrans({"ぁ":"あ","ぃ":"い","ぅ":"う","ぇ":"え","ぉ":"お","ゃ":"や","ゅ":"ゆ","ょ":"よ","ゎ":"わ","っ":"つ","ゕ":"か","ゖ":"け"})
_A_SET=set("あかさたなはまやらわがざだばぱぁゃゎっ"); _I_SET=set("いきしちにひみりぎじぢびぴぃ")
_U_SET=set("うくすつぬふむゆるぐずづぶぷぅゅっ"); _E_SET=set("えけせてねへめれげぜでべぺぇ")
_O_SET=set("おこそとのほもよろをごぞどぼぽぉょ")

def _kana_to_hira(s:str)->str:
    out=[]
    for ch in s:
        o=ord(ch)
        if 0x30A1<=o<=0x30F6: out.append(chr(o-0x60))
        elif ch in("ヵ","ヶ"): out.append({"ヵ":"か","ヶ":"け"}[ch])
        else: out.append(ch)
    return "".join(out)

def _long_to_vowel(prev:str)->str:
    if not prev: return ""
    if prev in _A_SET: return "あ"
    if prev in _I_SET: return "い"
    if prev in _U_SET: return "う"
    if prev in _E_SET: return "え"
    if prev in _O_SET: return "お"
    return ""

def fold_kana(s:str)->str:
    if not s: return ""
    s=unicodedata.normalize("NFKC",s); s=_kana_to_hira(s); s=s.translate(_SMALL_TO_BASE)
    buf=[]
    for ch in s:
        buf.append(_long_to_vowel(buf[-1]) if ch=="ー" else ch)
    d=unicodedata.normalize("NFD","".join(buf))
    d="".join(c for c in d if ord(c)not in(0x3099,0x309A))  # 濁点除去
    return unicodedata.normalize("NFC",d).lower().strip()

def hira_to_kata(s:str)->str:
    out=[]
    for ch in s:
        o=ord(ch)
        if 0x3041<=o<=0x3096: out.append(chr(o+0x60))
        elif ch in("ゕ","ゖ"): out.append({"ゕ":"ヵ","ゖ":"ヶ"}[ch])
        else: out.append(ch)
    return "".join(out)

# ========= 同義語辞書（最小必要を厚めに） =========
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
    # 検索改善用（イベント系）
    "コンテスト":{"大会","表彰"},
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

# ========= KBロード（JSONL） =========
def _load_kb(url:str)->List[Dict[str,Any]]:
    items=[]
    try:
        r=requests.get(url,timeout=15)
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

# ========= 年フィルタ／代表日 =========
def _nfkcnum(s:str)->str: return unicodedata.normalize("NFKC",s or "")

_RANGE_SEP = r"(?:-|–|—|~|〜|～|\.{2})"

def parse_year_filter_from_query(q:str)->Tuple[str,Optional[int],Optional[int]]:
    """末尾に 'YYYY' または 'YYYY-YYYY' が付いたときに抽出"""
    qn=_nfkc(q).strip().replace("　"," ")
    if not qn: return "",None,None
    parts=qn.split()
    last=parts[-1]
    m1=re.fullmatch(r"(19|20|21)\d{2}", last)
    if m1:
        return (" ".join(parts[:-1]).strip(), int(last), None)
    m2=re.fullmatch(rf"((?:19|20|21)\d{{2}})\s*{_RANGE_SEP}\s*((?:19|20|21)\d{{2}})", last)
    if m2:
        y1,y2=int(m2.group(1)),int(m2.group(2))
        if y1>y2: y1,y2=y2,y1
        return (" ".join(parts[:-1]).strip(), None, (y1,y2))
    return (qn,None,None)

def _years_from_text(s:str)->List[int]:
    ys=[int(y)for y in re.findall(r"(19|20|21)\d{2}",_nfkcnum(s))]
    return sorted(set(ys))

def _record_years(r:Dict[str,Any])->List[int]:
    ys:set[int]=set()
    def walk(x):
        if isinstance(x,dict):
            for v in x.values(): walk(v)
        elif isinstance(x,list):
            for v in x: walk(v)
        elif isinstance(x,str):
            ys.update(_years_from_text(x))
    walk(r)
    return sorted(ys)

def _best_date(rec:Dict[str,Any])->dt.date:
    ys=_record_years(rec)
    return dt.date(max(ys),1,1) if ys else dt.date(1970,1,1)

# ========= クエリ処理（複合語分割＋正規表現抽出） =========
SPLIT_TOKENS=("結果","発表","報告","案内","募集","開催","中止","決定","概要","参加")
_JP_WORDS = re.compile(r"[一-龥ぁ-んァ-ンー]{2,}|[A-Za-z0-9]{2,}")

def split_compound(term:str)->List[str]:
    outs=[term]
    for key in SPLIT_TOKENS:
        if key in term and term!=key:
            parts=term.split(key)
            left=parts[0].strip()
            if left: outs.append(left)
            outs.append(key)
            right=key.join(parts[1:]).strip()
            if right and right!=key: outs.append(right)
    # unique
    seen=set(); out=[]
    for t in outs:
        if t and t not in seen:
            seen.add(t); out.append(t)
    return out

def normalize_query(q:str)->str:
    s=_nfkc(q or "")
    # 表記ゆれ救済（必要最小限）
    repl=[("接木","接ぎ木"),("つぎ木","接ぎ木"),("土作り","土の作り方"),("土づくり","土の作り方")]
    for a,b in repl: s=s.replace(a,b)
    return s.strip()

def jp_terms(q:str)->List[str]:
    if not q: return []
    qn=normalize_query(q).replace("　"," ")
    toks=[t for t in qn.split() if t]
    # 本文から語抽出を追加（2文字以上の日本語／英数）
    toks += _JP_WORDS.findall(qn)
    # スペース無し1語のみなら複合語分割も試す
    if len([t for t in qn.split() if t])==1 and len(toks)<=3:
        toks = list(dict.fromkeys(toks + split_compound(qn)))
    # 上限5語に丸める（ノイズ抑制）
    uniq=[]
    seen=set()
    for t in sorted(set(toks), key=len, reverse=True):
        if t not in seen:
            uniq.append(t); seen.add(t)
        if len(uniq)>=5: break
    return uniq

def expand_terms(terms:List[str])->List[str]:
    out:set[str]=set()
    for t in terms:
        ft=fold_kana(t)
        out|={t, ft, hira_to_kata(ft)}
        out|=expand_with_domain_dict(t)
    return sorted({s for s in out if s})

# ========= マッチング／ハイライト用ユーティリティ =========
def _pick(*cands)->str:
    for v in cands:
        if isinstance(v,str) and v.strip():
            return v
    return ""

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
    walk(rec)
    return "\n".join(buf)

def _match_text(text:str, terms:List[str])->bool:
    if not text: return False
    t_low=text.lower(); t_fold=fold_kana(text)
    for term in terms:
        if term.lower() in t_low or fold_kana(term) in t_fold:
            return True
    return False

def _build_highlight_variants(terms:List[str])->List[str]:
    hs:set[str]=set()
    for t in terms:
        hs.add(t)
        hs |= expand_with_domain_dict(t)
        ft=fold_kana(t)
        hs.add(ft)
        hs.add(hira_to_kata(ft))
    return sorted({s for s in hs if s}, key=lambda x: len(x), reverse=True)

def _html_escape(s:str)->str:
    return s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def _find_first(text:str, keys:List[str])->Optional[int]:
    best=None
    for k in keys:
        if not k: continue
        m=re.search(re.escape(k), text, flags=re.IGNORECASE)
        if m:
            pos=m.start()
            if best is None or pos<best: best=pos
    return best

def _make_snippet_and_highlight(text:str, keys:List[str], ctx:int=90, maxlen:int=360)->Tuple[str,bool]:
    if not text: return ("", False)
    pos=_find_first(text, keys)
    hit=pos is not None
    if not hit:
        raw=text[:maxlen]
        return (_html_escape(raw), False)
    start=max(0, pos-ctx); end=min(len(text), pos+ctx)
    raw=text[start:end]
    prefix="…" if start>0 else ""; suffix="…" if end<len(text) else ""
    esc=_html_escape(raw)
    # HTML上で置換（長い語→短い語の順）
    for k in keys:
        if not k: continue
        pattern=re.compile(re.escape(_html_escape(k)), flags=re.IGNORECASE)
        esc=pattern.sub(lambda m: f"<mark>{m.group(0)}</mark>", esc)
    snippet=prefix+esc+suffix
    if len(snippet) > maxlen+40:
        snippet = snippet[:maxlen] + "…"
    return (snippet, True)

# ========= スコアリング（軽量：タイトル重視＋本文＋フォールバック） =========
W_TITLE, W_TEXT = 2.0, 1.4

def _score_record(rec:Dict[str,Any], terms_all:List[str])->float:
    title=str(rec.get("title","")); body=_pick(rec.get("content",""), rec.get("text",""), rec.get("body",""))
    low=lambda s:(s or "").lower(); fk=lambda s:fold_kana(s or "")
    t_low, b_low = low(title), low(body)
    t_f, b_f = fk(title), fk(body)

    sc=0.0; matched=False
    for t in terms_all:
        tl, tf = t.lower(), fold_kana(t)
        if tl in t_low: sc+=W_TITLE; matched=True
        if tl in b_low: sc+=W_TEXT; matched=True
        if tf in t_f:   sc+=W_TITLE*0.95; matched=True
        if tf in b_f:   sc+=W_TEXT*0.95; matched=True
    if not matched:
        # レコード全体も一応見る（取りこぼし救済）
        alltxt=_record_text_all(rec)
        a_low=low(alltxt); a_f=fk(alltxt)
        for t in terms_all:
            tl, tf = t.lower(), fold_kana(t)
            if tl in a_low or tf in a_f:
                sc+=W_TEXT*0.7; matched=True; break
    return sc if matched else 0.0

# ========= 検索本体 =========
def search_jsonl(q:str, year=None, year_from=None, year_to=None)->Tuple[List[Dict[str,Any]], List[str]]:
    if not KB_DATA: return [], []
    base_terms=jp_terms(q)
    terms_all=expand_terms(base_terms)
    if not terms_all: return [], []

    ranked=[]
    for rec in KB_DATA:
        title=str(rec.get("title","")); body=_pick(rec.get("content",""), rec.get("text",""), rec.get("body",""))
        alltxt=_record_text_all(rec)

        if (_match_text(title, terms_all)
            or _match_text(body, terms_all)
            or _match_text(alltxt, terms_all)):
            # 年フィルタ
            if year or year_from or year_to:
                ys=_record_years(rec)
                if not ys: 
                    continue
                lo=year_from if year_from is not None else -10**9
                hi=year_to   if year_to   is not None else  10**9
                if year is not None and year not in ys:
                    continue
                if year is None and not any(lo<=y<=hi for y in ys):
                    continue
            ranked.append(rec)

    if not ranked: return [], []

    # スコア付け（関連度）→ 最新順（代表日）で安定化
    scored=[(_score_record(r, terms_all), r) for r in ranked]
    scored=[(s,r) for s,r in scored if s>0.0]
    if not scored: 
        # 全部0点ならとりあえず最新順だけで返す
        ranked.sort(key=lambda r:_best_date(r), reverse=True)
        return ranked, base_terms

    # まず関連度降順、次に代表日降順で安定化
    scored.sort(key=lambda x:x[0], reverse=True)
    scored.sort(key=lambda x:_best_date(x[1]), reverse=True)
    ranked=[r for _,r in scored]
    return ranked, base_terms

# ========= API =========
@app.get("/health")
def health():
    return JSONResponse({"ok":True,"kb_url":KB_URL,"kb_size":len(KB_DATA)}, headers=JSON_HEADERS)

@app.get("/version")
def version():
    return JSONResponse({"version": app.version}, headers=JSON_HEADERS)

@app.get("/diag")
def diag(q:str=Query("", description="確認")):
    base, y, yr = parse_year_filter_from_query(q)
    yf, yt = (None, None)
    if isinstance(yr, tuple): yf, yt = yr
    return JSONResponse({"query":q,"base":base,"year":y,"year_from":yf,"year_to":yt}, headers=JSON_HEADERS)

@app.get("/api/search")
def api_search(
    q:str=Query("", description="検索語（末尾に年/年範囲も可：例『苔 2001』『剪定 1999-2001』）"),
    page:int=1,
    page_size:int=5,
    order:str="latest"
):
    try:
        q_raw=(q or "").strip()
        if not q_raw:
            return JSONResponse({"items":[],"total_hits":0,"page":1,"page_size":page_size,"has_more":False,"next_page":None,"error":None,"order_used":order}, headers=JSON_HEADERS)

        # クエリから年/範囲を抽出
        base_q, y, yr = parse_year_filter_from_query(q_raw)
        y_from, y_to = (None, None)
        if isinstance(yr, tuple): y_from, y_to = yr

        ranked, base_terms = search_jsonl(base_q, year=y, year_from=y_from, year_to=y_to)
        total=len(ranked)

        # ページング
        if page<1: page=1
        if page_size<1: page_size=5
        start=(page-1)*page_size; end=start+page_size
        slice_=ranked[start:end]

        # ハイライトの準備（長い語から）
        keys_for_hl=_build_highlight_variants(expand_terms(base_terms))

        items=[]
        for i, r in enumerate(slice_):
            title_raw=_pick(r.get("title",""), r.get("heading",""), r.get("tTitle",""))
            body_raw =_pick(r.get("content",""), r.get("text",""), r.get("body",""), r.get("description",""))
            alltxt   =_record_text_all(r)

            # どこに当たったかでスニペット生成
            title_pos=_find_first(title_raw, keys_for_hl)
            body_pos =_find_first(body_raw,  keys_for_hl)
            all_pos  =_find_first(alltxt,    keys_for_hl) if body_pos is None else None

            if body_pos is not None:
                snippet, hit=_make_snippet_and_highlight(body_raw, keys_for_hl, ctx=90, maxlen=360)
                tag="（本文にヒット）"
            elif all_pos is not None:
                snippet, hit=_make_snippet_and_highlight(alltxt, keys_for_hl, ctx=90, maxlen=360)
                tag="（本文にヒット）"
            else:
                # タイトルにしか無い or 検出できない → 先頭抜粋＋タイトルタグ
                snippet, hit=_make_snippet_and_highlight(body_raw, keys_for_hl, ctx=90, maxlen=360)
                tag="" if title_pos is None else "（タイトルにヒット）"

            title_out=(title_raw or "(無題)") + (tag if tag else "")

            items.append({
                "title": title_out,
                "content": snippet,  # HTML（<mark>含む）
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
            "order_used": "latest"
        }, headers=JSON_HEADERS)

    except Exception as e:
        return JSONResponse({"items":[],"total_hits":0,"page":1,"page_size":page_size,"has_more":False,"next_page":None,"error":str(e),"order_used":order}, headers=JSON_HEADERS)

@app.get("/")
def root():
    return PlainTextResponse(
        "Mini Rose Search JSONL API running.\nEndpoints: /api/search, /health, /diag, /version, /ui\n",
        headers={"content-type":"text/plain; charset=utf-8", "Cache-Control":"no-store"}
    )
