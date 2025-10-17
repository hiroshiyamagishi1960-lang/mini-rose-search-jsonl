# app.py — 正規化内蔵・安定版（Notion直読み相当の構造に整えてから検索）
# 版: jsonl-2025-10-17-normalized-stable-v2
# 仕様:
#  - 取り込み時に JSONL を「直読み版と同等の構造」に正規化
#      (title / text / url / issue / date / author / section + 合成 header)
#  - 並び既定は relevance（関連順）; latest も指定可（?order=latest）
#  - 「コンテスト結果」などのフレーズ一致・近接を重視（header/title > text）
#  - 返却: title（<mark>でハイライト）, content(抜粋~500字), url, rank, date を含む
#  - タイトル末尾などに混入した「｜ UI-TEST-YYYYMMDD-HHMM」を除去
#  - CORS 全許可／UIは変更不要（/ui があれば配信）

import os, re, json, unicodedata, datetime as dt, hashlib
from typing import List, Dict, Any, Tuple, Optional
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, PlainTextResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import requests

# ====== 設定 ======
KB_URL = os.getenv("KB_URL", "https://raw.githubusercontent.com/hiroshiyamagishi1960-lang/mini-rose-search-jsonl/main/kb.jsonl")
JSON_HEADERS = {"content-type": "application/json; charset=utf-8"}

app = FastAPI(title="Mini Rose Search API", version="jsonl-2025-10-17-normalized-stable-v2")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"], allow_credentials=True)

# ====== UI（任意。同梱されていれば no-store で配信） ======
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

# ====== 正規化ユーティリティ ======
def _nfkc(s: Optional[str]) -> str:
    return unicodedata.normalize("NFKC", s or "").strip()

def _lower(s: str) -> str:
    return _nfkc(s).lower()

# かなフォールディング（濁点・長音・カナ/かな差を吸収）
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

# ====== フィールド別名（JSONLの揺れに対応） ======
TITLE_KEYS = ["title","見出し","タイトル","name","heading"]
TEXT_KEYS  = ["content","text","body","本文","テキスト"]
ISSUE_KEYS = ["issue","会報号","号","会報"]
DATE_KEYS  = ["date","date_primary","発行日","開催日","日付"]
AUTH_KEYS  = ["author","著者","執筆","作成者"]
SECT_KEYS  = ["section","カテゴリ","category","セクション"]
URL_KEYS   = ["url","source","リンク","参照","出典URL"]

def _pick(rec:Dict[str,Any], keys:List[str])->str:
    for k in keys:
        if k in rec and _nfkc(rec[k]): return _nfkc(rec[k])
    return ""

def _first_nonempty_line(text:str)->str:
    for ln in (text or "").splitlines():
        t=_nfkc(ln)
        if t:
            m=re.split(r"[。．、,，/｜|／]", t, 1)
            return (m[0] or t)[:80]
    return ""

# 年・号の抽出
def _years_from_text(s:str)->List[int]:
    return sorted({int(y) for y in re.findall(r"(19|20|21)\d{2}", _nfkc(s))})

def _parse_issue(s:str)->str:
    if not s: return ""
    m=re.search(r"\d+", s)
    return f"第{m.group(0)}号" if m else _nfkc(s)

# UI テスト用の後置ラベル除去（｜ UI-TEST-YYYYMMDD-HHMM）
UI_TEST_PAT = re.compile(r"\s*[|｜]\s*UI-TEST-[0-9]{8}-[0-9]{4}\s*$", re.IGNORECASE)
def _strip_ui_test_label(s:str)->str:
    return UI_TEST_PAT.sub("", _nfkc(s))

# ====== 正規化（直読み版と同等の論理構造へ） ======
RAW_DATA: List[Dict[str,Any]] = []
NORM_DATA: List[Dict[str,Any]] = []

def _load_raw(url:str)->List[Dict[str,Any]]:
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

def _normalize_record(rec:Dict[str,Any])->Dict[str,Any]:
    title_raw = _pick(rec, TITLE_KEYS)
    text      = _pick(rec, TEXT_KEYS)
    issue     = _parse_issue(_pick(rec, ISSUES_KEYS)) if (ISSUES_KEYS:=[k for k in ISSUE_KEYS]) else _parse_issue(_pick(rec, ISSUE_KEYS))  # safety
    date      = _pick(rec, DATE_KEYS)   # 開催日/発行日優先
    author    = _pick(rec, AUTH_KEYS)
    section   = _pick(rec, SECT_KEYS)
    url       = _pick(rec, URL_KEYS)

    # タイトル補完（空なら本文先頭行）＋ UI-TEST ラベル除去
    title_base = title_raw if title_raw and title_raw!="(無題)" else (_first_nonempty_line(text) or "(無題)")
    title_base = _strip_ui_test_label(title_base)

    # 合成ヘッダ（号・年・タイトル）＋ UI-TEST ラベル除去
    years = _years_from_text(date)
    year_str = str(max(years)) if years else ""
    header = " | ".join([p for p in [_parse_issue(_pick(rec, ISSUE_KEYS)), year_str, title_base] if p])
    header = _strip_ui_test_label(header)

    return {
        "title": title_base,
        "text": _nfkc(text),
        "url": url,
        "issue": _parse_issue(_pick(rec, ISSUE_KEYS)),
        "date": date,
        "author": author,
        "section": section,
        "header": header or title_base
    }

def _normalize_all():
    global RAW_DATA, NORM_DATA
    RAW_DATA = _load_raw(KB_URL)
    NORM_DATA = [_normalize_record(r) for r in RAW_DATA]

_normalize_all()

# ====== クエリ解析・ハイライト ======
_JP_WORDS = re.compile(r"[一-龥ぁ-んァ-ンー]{2,}|[A-Za-z0-9]{2,}")
SPLIT_TOKENS=("結果","発表","報告","案内","募集","開催","決定")

def normalize_query(q:str)->str:
    return _nfkc(q or "")

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
        ft=fold_kana(t); out|={t, ft, hira_to_kata(ft)}
        # 必要最低限の同義語（“結果発表”等）は本文で拾えるのでここは簡素に
    return sorted({s for s in out if s})

def _record_text_all_norm(rec:Dict[str,Any])->str:
    parts=[rec.get("header",""), rec.get("title",""), rec.get("section",""), rec.get("author",""),
           rec.get("issue",""), rec.get("date",""), rec.get("text","")]
    return "\n".join([_nfkc(p) for p in parts if _nfkc(p)])

def _html_escape(s:str)->str:
    return s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def _build_hl_keys(terms:List[str])->List[str]:
    hs=set()
    for t in terms:
        hs.add(t)
        ft=fold_kana(t); hs.add(ft); hs.add(hira_to_kata(ft))
    return sorted({h for h in hs if h}, key=len, reverse=True)

def _find_first(text:str, keys:List[str])->Optional[int]:
    best=None
    for k in keys:
        if not k: continue
        m=re.search(re.escape(k), text or "", flags=re.IGNORECASE)
        if m:
            p=m.start()
            if best is None or p<best: best=p
    return best

def _highlight_text(text:str, keys:List[str])->str:
    esc=_html_escape(text or "")
    for k in keys:
        if not k: continue
        esc=re.compile(re.escape(_html_escape(k)), re.IGNORECASE).sub(lambda m:f"<mark>{m.group(0)}</mark>", esc)
    return esc

def _make_snippet_and_highlight(text:str, keys:List[str], ctx:int=120, maxlen:int=500)->Tuple[str,bool]:
    if not text: return ("", False)
    pos=_find_first(text, keys)
    if pos is None:
        raw=text[:maxlen]; return (_html_escape(raw), False)
    start=max(0,pos-ctx); end=min(len(text), pos+ctx)
    raw=text[start:end]
    esc=_highlight_text(raw, keys)
    prefix="…" if start>0 else ""; suffix="…" if end<len(text) else ""
    snip=prefix+esc+suffix
    return (snip if len(snip)<=maxlen+40 else snip[:maxlen]+"…", True)

# ====== スコアリング（header/title を重視。フレーズ・近接も加点） ======
W_HEAD, W_TITLE, W_SECT, W_TEXT = 2.6, 2.2, 1.2, 1.1
PHRASE_HEAD_BONUS, PHRASE_TITLE_BONUS, PHRASE_TEXT_BONUS = 4.6, 4.2, 2.2
COOCCUR_WINDOW = 30
COOCCUR_HEAD_BONUS, COOCCUR_TEXT_BONUS = 1.7, 1.2

PHRASE_PAT_RESULTS = re.compile(r"コンテスト(?:\s|　|の|:|：|・|,|，|。|．|-|–|—|~|〜|～|/|／){0,2}結果")

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
    # ベース語を連結したフレーズも候補に
    if len(base_terms)>=2:
        for i in range(len(base_terms)):
            for j in range(i+1,len(base_terms)):
                c.add(base_terms[i]+base_terms[j])
                c.add(base_terms[j]+base_terms[i])
    # 「コンテスト結果」ゆるい表記
    c.add("コンテスト結果")
    return sorted({x for x in c if len(x)>=2}, key=len, reverse=True)

def _best_date(rec:Dict[str,Any])->dt.date:
    yset=set(_years_from_text(rec.get("date",""))+
             _years_from_text(rec.get("text",""))+
             _years_from_text(rec.get("title",""))+
             _years_from_text(rec.get("header","")))
    return dt.date(max(yset),1,1) if yset else dt.date(1970,1,1)

def _score(rec:Dict[str,Any], terms_all:List[str], base_terms:List[str], q_raw:str)->Tuple[float, dt.date]:
    head = _nfkc(rec.get("header",""))
    title= _nfkc(rec.get("title",""))
    sect = _nfkc(rec.get("section",""))
    text = _nfkc(rec.get("text",""))

    low=lambda s:(s or "").lower(); fk=lambda s:fold_kana(s or "")
    h_low,t_low,s_low,x_low = low(head),low(title),low(sect),low(text)
    h_f,  t_f,  s_f,  x_f  = fk(head), fk(title), fk(sect), fk(text)

    sc=0.0; matched=False
    for t in terms_all:
        tl=t.lower(); tf=fold_kana(t)
        if tl in h_low or tf in h_f: sc+=W_HEAD;  matched=True
        if tl in t_low or tf in t_f: sc+=W_TITLE; matched=True
        if tl in s_low or tf in s_f: sc+=W_SECT;  matched=True
        if tl in x_low or tf in x_f: sc+=W_TEXT;  matched=True

    # フレーズ一致（header>title>text）
    for ph in _phrase_candidates(q_raw, base_terms):
        ph_f=fold_kana(ph)
        if ph.lower() in h_low or ph_f in h_f: sc+=PHRASE_HEAD_BONUS;  matched=True
        if ph.lower() in t_low or ph_f in t_f: sc+=PHRASE_TITLE_BONUS; matched=True
        if ph.lower() in x_low or ph_f in x_f: sc+=PHRASE_TEXT_BONUS;  matched=True

    # 「コンテスト結果」ゆるい表記にも加点
    if PHRASE_PAT_RESULTS.search(head) or PHRASE_PAT_RESULTS.search(title):
        sc += 1.5

    # 近接
    if _near_cooccur(head, base_terms, COOCCUR_WINDOW): sc+=COOCCUR_HEAD_BONUS; matched=True
    if _near_cooccur(text, base_terms, COOCCUR_WINDOW): sc+=COOCCUR_TEXT_BONUS; matched=True

    return (sc if matched else 0.0, _best_date(rec))

# ====== 検索本体 ======
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

def _years_of_norm(rec:Dict[str,Any])->List[int]:
    return _years_from_text(_record_text_all_norm(rec))

def search_scored(q:str, year=None, year_from=None, year_to=None)->Tuple[List[Tuple[float,dt.date,Dict[str,Any]]], List[str]]:
    if not NORM_DATA: return [], []
    base_terms=jp_terms(q)
    terms_all=expand_terms(base_terms)
    if not terms_all: return [], []
    scored=[]
    for rec in NORM_DATA:
        # 粗フィルタ（正規化後の全体テキストで1語でも一致）
        alltxt=_record_text_all_norm(rec)
        t_low=alltxt.lower(); t_fold=fold_kana(alltxt)
        if not any((term.lower() in t_low) or (fold_kana(term) in t_fold) for term in terms_all):
            continue

        # 年フィルタ
        if year or year_from or year_to:
            ys=_years_of_norm(rec)
            if not ys: continue
            lo=year_from if year_from is not None else -10**9
            hi=year_to   if year_to   is not None else  10**9
            if year is not None and year not in ys: continue
            if year is None and not any(lo<=y<=hi for y in ys): continue

        sc, d = _score(rec, terms_all, base_terms, q)
        if sc<=0: continue
        scored.append((sc, d, rec))
    return scored, base_terms

# ====== API ======
@app.get("/health")
def health():
    sha=hashlib.sha256()
    for rec in NORM_DATA:
        sha.update((_nfkc(rec.get("title","")) + _nfkc(rec.get("url",""))).encode("utf-8"))
    return JSONResponse({
        "ok": True,
        "kb_url": KB_URL,
        "kb_size_raw": len(RAW_DATA),
        "kb_size_norm": len(NORM_DATA),
        "kb_fingerprint": sha.hexdigest()
    }, headers=JSON_HEADERS)

@app.get("/version")
def version():
    return JSONResponse({"version": app.version}, headers=JSON_HEADERS)

@app.get("/api/search")
def api_search(
    q:str=Query("", description="検索語（末尾に年/年範囲も可）"),
    page:int=1,
    page_size:int=5,
    order:str=Query("relevance", description="relevance（関連順: 既定） | latest（最新順）")
):
    try:
        q_raw=(q or "").strip()
        if not q_raw:
            return JSONResponse({"items":[],"total_hits":0,"page":1,"page_size":page_size,"has_more":False,"next_page":None,"error":None,"order_used":order}, headers=JSON_HEADERS)

        base_q, y, yr = parse_year_from_query(q_raw)
        y_from=y_to=None
        if isinstance(yr,tuple): y_from,y_to=yr

        scored, base_terms = search_scored(base_q, year=y, year_from=y_from, year_to=y_to)
        total=len(scored)
        if total==0:
            return JSONResponse({"items":[],"total_hits":0,"page":1,"page_size":page_size,"has_more":False,"next_page":None,"error":None,"order_used":order}, headers=JSON_HEADERS)

        # 並び: ページング前に確定
        if order.lower()=="latest":
            scored.sort(key=lambda t:(t[1], t[0]), reverse=True)   # date→score
        else:
            scored.sort(key=lambda t:(t[0], t[1]), reverse=True)   # score→date
            order="relevance"

        # ページング
        if page<1: page=1
        if page_size<1: page_size=5
        start=(page-1)*page_size; end=start+page_size
        slice_=scored[start:end]

        # ハイライト用キー（タイトルにも<mark>適用）
        hl_keys=_build_hl_keys(expand_terms(base_terms))

        items=[]
        for i,(score, d, r) in enumerate(slice_):
            head  = _strip_ui_test_label(_nfkc(r.get("header","")))
            title = _strip_ui_test_label(_nfkc(r.get("title","")))
            text  = _nfkc(r.get("text",""))
            date  = _nfkc(r.get("date",""))  # ← 返却に必ず含める

            # タイトルは合成ヘッダ優先で表示し、<mark>でハイライト
            display_title_source = head or title or "(無題)"
            title_highlighted = _highlight_text(display_title_source, hl_keys)

            # 本文は ~500字の抜粋（ヒット周辺に<mark>）
            snippet,_ = _make_snippet_and_highlight(text, hl_keys, ctx=120, maxlen=500)

            items.append({
                "title": title_highlighted,     # ← タイトルにもハイライトを反映
                "content": snippet,             # ← 抜粋 ~500字
                "url": _nfkc(r.get("url","")),
                "rank": start+i+1,
                "date": date                    # ← 日付（開催日/発行日）を返却
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
