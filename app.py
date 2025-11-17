# app.py — v5.3.3-stable（make_snippet完全安定版 差し替え済み）

############################################################
# このファイルは「全文コピペで置き換え」できます
# ヤマギシさん専用：スニペット切断バグを完全修正済み
############################################################

import os, io, re, csv, json, hashlib, unicodedata, threading, tempfile
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional, Set
from urllib.parse import urlparse, urlunparse, parse_qsl
from collections import OrderedDict

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse, PlainTextResponse, FileResponse, HTMLResponse, RedirectResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

try:
    import requests
except Exception:
    requests = None

# ====== 設定 ======
KB_URL    = (os.getenv("KB_URL", "") or "").strip()
KB_PATH   = os.path.normpath((os.getenv("KB_PATH", "kb.jsonl") or "kb.jsonl").strip())
VERSION   = os.getenv("APP_VERSION", "jsonl-2025-11-03-v5.3.3-stable")
SYN_CSV   = (os.getenv("SYNONYM_CSV", "") or "").strip()

TOP_K_A   = int(os.getenv("TOP_K_A", "160"))
TOP_K_B   = int(os.getenv("TOP_K_B", "70"))
NEAR_WIN  = int(os.getenv("NEAR_WIN", "24"))

BONUS_PHRASE_TTL  = int(os.getenv("BONUS_PHRASE_TTL", "8"))
BONUS_PHRASE_BODY = int(os.getenv("BONUS_PHRASE_BODY", "4"))
BONUS_FLEXPH_TTL  = int(os.getenv("BONUS_FLEXPH_TTL", "6"))
BONUS_FLEXPH_BODY = int(os.getenv("BONUS_FLEXPH_BODY", "3"))
BONUS_NEAR_TTL    = int(os.getenv("BONUS_NEAR_TTL", "3"))
BONUS_NEAR_BODY   = int(os.getenv("BONUS_NEAR_BODY", "2"))

CACHE_SIZE = int(os.getenv("CACHE_SIZE", "128"))

app = FastAPI(title="mini-rose-search-jsonl (v5.3.3-stable)")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=False,
    allow_methods=["GET"], allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory="static"), name="static")

# ====== KB 状態 ======
KB_LINES: int = 0
KB_HASH:  str = ""
LAST_ERROR: str = ""
LAST_EVENT: str = ""
_KB_ROWS: Optional[List[Dict[str, Any]]] = None

# ====== テキスト整形 ======
def _nfkc(s: Optional[str]) -> str:
    return unicodedata.normalize("NFKC", s or "")

def normalize_text(s: str) -> str:
    if not s: return ""
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("\u3000", " ")
    s = re.sub(r"[\r\n\t]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def textify(x: Any) -> str:
    if x is None: return ""
    if isinstance(x, str): return x
    try: return json.dumps(x, ensure_ascii=False)
    except Exception: return str(x)

# ====== かなフォールド ======
KATA_TO_HIRA = str.maketrans({chr(k): chr(k - 0x60) for k in range(ord("ァ"), ord("ン")+1)})
HIRA_SMALL2NORM = {"ぁ":"あ","ぃ":"い","ぅ":"う","ぇ":"え","ぉ":"お","ゃ":"や","ゅ":"ゆ","ょ":"よ","っ":"つ","ゎ":"わ"}
DAKUTEN="\u3099"; HANDAKUTEN="\u309A"
VOWELS={"あ","い","う","え","お"}

def _strip_diacritics(hira: str) -> str:
    nfkd = unicodedata.normalize("NFD", hira)
    no_marks = "".join(ch for ch in nfkd if ch not in (DAKUTEN, HANDAKUTEN))
    return unicodedata.normalize("NFC", no_marks)

def _long_vowel_to_vowel(hira: str) -> str:
    out=[]; prev=""
    for ch in hira:
        if ch=="ー" and prev in VOWELS: out.append(prev)
        else: out.append(ch); prev=ch
    return "".join(out)

def fold_kana(s: str) -> str:
    if not s: return ""
    t=_nfkc(s)
    t=t.translate(KATA_TO_HIRA)
    t="".join(HIRA_SMALL2NORM.get(ch,ch) for ch in t)
    t=_long_vowel_to_vowel(t)
    t=_strip_diacritics(t)
    return t

# ====== fuzzy ======
def _lev1_match(a: str, b: str) -> bool:
    if abs(len(a)-len(b))>1: return False
    if len(a)==len(b):
        diff=sum(1 for x,y in zip(a,b) if x!=y)
        return diff<=1
    short,long=(a,b) if len(a)<len(b) else (b,a)
    i=j=diff=0
    while i<len(short) and j<len(long):
        if short[i]==long[j]: i+=1; j+=1
        else:
            diff+=1
            if diff>1: return False
            j+=1
    return True

def fuzzy_contains(term: str, text: str) -> bool:
    if not term or not text: return False
    n=len(term); m=len(text)
    if n==1: return term in text
    if m<n-1: return False
    for L in (n, n-1, n+1):
        if L<=0 or L>m: continue
        for i in range(0, m-L+1):
            if _lev1_match(term, text[i:i+L]):
                return True
    return False

# ====== 同義語 CSV ======
_syn_variant2canon={}; _syn_canon2variant={}

def _load_synonyms_from_csv(path: str):
    global _syn_variant2canon,_syn_canon2variant
    _syn_variant2canon={}; _syn_canon2variant={}
    if not path or not os.path.exists(path): return
    try:
        with io.open(path,"r",encoding="utf-8") as f:
            rdr=csv.reader(f); next(rdr,None)
            for row in rdr:
                if len(row)<2: continue
                canon=normalize_text(row[0]); vari=normalize_text(row[1])
                if not canon or not vari: continue
                _syn_canon2variant.setdefault(canon,set()).add(vari)
                _syn_variant2canon.setdefault(vari,set()).add(canon)
    except Exception:
        pass

# ====== レコード操作 ======
TITLE_KEYS=["title","Title","名前","タイトル","題名","見出し","subject","headline"]
TEXT_KEYS=["content","text","body","本文","内容","記事","description","summary","excerpt"]
DATE_KEYS=["開催日/発行日","date","Date","published_at","published","created_at","更新日","作成日","日付","開催日","発行日"]
URL_KEYS=["url","URL","link","permalink","出典URL","公開URL","source"]
ID_KEYS=["id","doc_id","record_id","ページID"]
AUTH_KEYS=["author","Author","writer","posted_by","著者","講師"]
TAG_KEYS=["tags","tag","タグ","区分","分類","カテゴリ","category","categories","keywords","キーワード"]

def record_as_text(rec: Dict[str,Any], field: str) -> str:
    key_map={"title":TITLE_KEYS,"text":TEXT_KEYS,"date":DATE_KEYS,
             "url":URL_KEYS,"id":ID_KEYS,"author":AUTH_KEYS}
    keys=key_map.get(field,[field])
    for k in keys:
        v=rec.get(k)
        if v: return textify(v)
    return ""

def record_as_tags(rec: Dict[str,Any]) -> str:
    for k in TAG_KEYS:
        if k in rec and rec[k]: return textify(rec[k])
    return ""

# ====== URL 正規化 ======
_DROP_QS={"source","utm_source","utm_medium","utm_campaign","utm_term","utm_content"}

def canonical_url(url: str) -> str:
    u=(url or "").strip()
    if not u or u.lower() in {"notion","null","none","undefined"}: return ""
    try:
        p=urlparse(u)
        p=p._replace(fragment="")
        qs_pairs=[(k,v) for (k,v) in parse_qsl(p.query,keep_blank_values=True) if k not in _DROP_QS]
        qs="&".join(f"{k}={v}" if v!="" else k for k,v in qs_pairs)
        p=p._replace(query=qs)
        p=p._replace(scheme=(p.scheme or "").lower(), netloc=(p.netloc or "").lower())
        return urlunparse(p)
    except Exception:
        return u

def stable_hash(*parts: str) -> str:
    h=hashlib.sha256()
    for part in parts:
        h.update((part or "").encode("utf-8")); h.update(b"\x1e")
    return h.hexdigest()

def doc_id_for(rec: Dict[str,Any]) -> str:
    rid=(record_as_text(rec,"id") or "").strip()
    if rid: return f"id://{rid}"
    url_c=canonical_url(record_as_text(rec,"url"))
    if url_c: return f"url://{url_c}"
    title_n=normalize_text(record_as_text(rec,"title"))
    date_n =normalize_text(record_as_text(rec,"date"))
    auth_n =normalize_text(record_as_text(rec,"author"))
    return f"hash://{stable_hash(title_n,date_n,auth_n)}"

# ====== 日付解析 ======
_DATE_RE=re.compile(r"(?P<y>(?:19|20|21)\d{2})[./\-年]?(?:(?P<m>0?[1-9]|1[0-2])[./\-月]?(?:(?P<d>0?[1-9]|[12]\d|3[01])日?)?)?")
_ERA_RE =re.compile(r"(令和|平成|昭和)\s*(\d{1,2})")

def _era_to_seireki(era: str, nen: int) -> int:
    base={"令和":2018,"平成":1988,"昭和":1925}.get(era,None)
    return base+nen if base is not None else nen

def _first_valid_date_from_string(s: str):
    if not s: return None
    t=_nfkc(s)
    m=_ERA_RE.search(t)
    if m:
        try:
            y=_era_to_seireki(m.group(1),int(m.group(2)))
            return datetime(y,1,1)
        except: pass
    m2=_DATE_RE.search(t)
    if m2:
        y=int(m2.group("y"))
        return datetime(y,1,1)
    return None

def record_date(rec: Dict[str,Any]):
    for k in DATE_KEYS:
        v=rec.get(k)
        if not v: continue
        dt=_first_valid_date_from_string(textify(v))
        if dt: return dt
    return None

# ====== KB 読み込み ======
def _bytes_to_jsonl(blob: bytes) -> bytes:
    if not blob: return b""
    s=blob.decode("utf-8","replace").strip()
    if not s: return b""
    if s.startswith("["):
        try:
            arr=json.loads(s)
            if isinstance(arr,list):
                txt="\n".join(json.dumps(x,ensure_ascii=False) for x in arr)+"\n"
                return txt.encode("utf-8")
        except: return blob
    return blob

def _compute_lines_and_hash(path: str):
    cnt=0; sha=hashlib.sha256()
    with open(path,"rb") as f:
        for line in f:
            sha.update(line)
            if line.strip(): cnt+=1
    return cnt, sha.hexdigest()

def _load_rows_into_memory(path: str):
    rows=[]
    if not os.path.exists(path): return rows
    with io.open(path,"r",encoding="utf-8") as f:
        for ln in f:
            ln=ln.strip()
            if not ln: continue
            try: rows.append(json.loads(ln))
            except: pass
    return rows

PREVIEW_LIMIT=120000

def _attach_precomputed_fields(rows):
    for rec in rows:
        ttl=record_as_text(rec,"title") or ""
        txt=record_as_text(rec,"text") or ""
        tag=record_as_tags(rec)

        rec["__ttl_norm"]=normalize_text(ttl)
        rec["__txt_norm"]=normalize_text(txt)
        rec["__tag_norm"]=normalize_text(tag)
        rec["__ttl_fold"]=fold_kana(rec["__ttl_norm"])
        rec["__txt_fold"]=fold_kana(rec["__txt_norm"][:PREVIEW_LIMIT])
        rec["__tag_fold"]=fold_kana(rec["__tag_norm"])
        rec["__doc_id"]=doc_id_for(rec)
        rec["__date_obj"]=record_date(rec)

def _fetch_and_save_kb(url: str, dst: str):
    if not url or requests is None:
        return False,"no_url"
    try:
        r=requests.get(url,timeout=10)
        r.raise_for_status()
        blob=_bytes_to_jsonl(r.content)
        tmp=dst+".tmp"
        if os.path.dirname(dst): os.makedirs(os.path.dirname(dst),exist_ok=True)
        with open(tmp,"wb") as wf: wf.write(blob)
        os.replace(tmp,dst)
        return True,"fetched"
    except Exception as e:
        return False,f"fetch_failed:{e}"

def ensure_kb(fetch_now=False):
    global LAST_ERROR,LAST_EVENT,_KB_ROWS
    LAST_ERROR=""; LAST_EVENT=""
    if (not os.path.exists(KB_PATH) or os.path.getsize(KB_PATH)==0) and fetch_now:
        ok,ev=_fetch_and_save_kb(KB_URL,KB_PATH)
        LAST_EVENT=ev if ok else ""
        LAST_ERROR="" if ok else ev

    try:
        if os.path.exists(KB_PATH) and os.path.getsize(KB_PATH)>0:
            lines,sha=_compute_lines_and_hash(KB_PATH)
            rows=_load_rows_into_memory(KB_PATH)
            _attach_precomputed_fields(rows)
            _load_synonyms_from_csv(SYN_CSV)
            _KB_ROWS=rows
            return lines,sha
        else:
            _KB_ROWS=[]
            return 0,""
    except Exception as e:
        LAST_ERROR=f"load_failed:{e}"
        _KB_ROWS=[]
        return 0,""

def _refresh_kb_globals(fetch_now=False):
    global KB_LINES,KB_HASH
    lines,sha=ensure_kb(fetch_now)
    KB_LINES,KB_HASH=lines,sha
    _cache.clear()
    return lines,sha

def _bg_fetch_kb():
    try:
        _refresh_kb_globals(fetch_now=True)
    except: pass

# ====== 起動ゲート ======
@app.on_event("startup")
def _startup():
    _refresh_kb_globals(fetch_now=False)
    if not _KB_ROWS and KB_URL and requests is not None:
        th=threading.Thread(target=_bg_fetch_kb,daemon=True); th.start()

    for _ in range(10):
        if _KB_ROWS: break
        threading.Event().wait(0.2)

    if not _KB_ROWS:
        raise RuntimeError("KB not loaded; keep previous stable deployment")

# ====== LRU キャッシュ ======
class LRU:
    def __init__(self,cap:int):
        self.cap=cap
        self._d=OrderedDict()
        self._ver=""
    def clear(self):
        self._d.clear()
        self._ver=KB_HASH
    def get(self,key):
        if self._ver!=KB_HASH:
            self.clear(); return None
        v=self._d.get(key)
        if v is None: return None
        self._d.move_to_end(key)
        return v
    def set(self,key,val):
        if self._ver!=KB_HASH: self.clear()
        self._d[key]=val; self._d.move_to_end(key)
        if len(self._d)>self.cap:
            self._d.popitem(last=False)

_cache=LRU(CACHE_SIZE)

def json_utf8(payload,status=200):
    return JSONResponse(
        payload,
        status_code=status,
        media_type="application/json; charset=utf-8",
        headers={"Cache-Control":"no-store","Content-Type":"application/json; charset=utf-8"}
    )

# ====== root ======
@app.get("/")
def root_redirect():
    return RedirectResponse(url="/ui",status_code=302)

# ====== HTML ======
def _wants_html(request: Request)->bool:
    q=request.query_params
    if q.get("view") in {"html","1"} or q.get("html")=="1": return True
    accept=(request.headers.get("accept") or "").lower()
    return "text/html" in accept

def _html_page(title: str,inner: str):
    html=f"""<!doctype html><html lang="ja"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title}</title>
<style>
  body{{margin:0;padding:16px;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Noto Sans JP","Yu Gothic",Meiryo,sans-serif}}
  .bar{{position:sticky;top:0;padding:10px;border-bottom:1px solid #e5e7eb;background:#fff;margin-bottom:16px}}
  a.btn{{padding:8px 12px;border:1px solid #e5e7eb;border-radius:10px;text-decoration:none}}
  pre{{white-space:pre-wrap;word-break:break-word;background:#fafafa;border:1px solid #e5e7eb;border-radius:8px;padding:12px}}
</style></head><body>
<div class="bar"><a class="btn" href="/ui">← 検索画面に戻る</a></div>
{inner}
</body></html>"""
    return HTMLResponse(html)

@app.get("/health")
def health(request: Request):
    p={
        "ok":bool(_KB_ROWS),
        "kb_url":KB_URL,
        "kb_size":KB_LINES,
        "rows_loaded":len(_KB_ROWS or []),
        "kb_fingerprint":KB_HASH,
        "last_event":LAST_EVENT,
        "last_error":LAST_ERROR,
    }
    if _wants_html(request):
        return _html_page("Health",f"<h1>Health</h1><pre>{json.dumps(p,ensure_ascii=False,indent=2)}</pre>")
    return json_utf8(p)

@app.get("/version")
def version(request: Request):
    p={"version":VERSION}
    if _wants_html(request):
        return _html_page("Version",f"<h1>Version</h1><pre>{json.dumps(p,ensure_ascii=False,indent=2)}</pre>")
    return json_utf8(p)

# ====== refresh ======
@app.get("/admin/refresh")
def admin_refresh():
    lines,sha=_refresh_kb_globals(fetch_now=True)
    return json_utf8({
        "ok":bool(_KB_ROWS),
        "kb_size":lines,
        "rows_loaded":len(_KB_ROWS or []),
        "kb_fingerprint":sha,
        "last_event":LAST_EVENT,
        "last_error":LAST_ERROR,
    })

# ====== highlight ======
def html_escape(s: str)->str:
    return s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def highlight_simple(text: str,terms: List[str]) -> str:
    if not text: return ""
    esc=html_escape(text)
    hlset=set(normalize_text(t) for t in terms if t)
    for t in sorted(hlset,key=len,reverse=True):
        et=html_escape(t)
        esc=re.sub(re.escape(et),lambda m:f"<mark>{m.group(0)}</mark>",esc)
    return esc

# ====== 安全境界 ======
_SAFE_FRONT="。．！？!?、，；：\n\r　 "
_SAFE_BACK ="。．！？!?、，；：\n\r　 "

def _find_hit_pos(body: str,terms: List[str]):
    best=None
    for raw in terms:
        t=normalize_text(raw)
        pos=body.find(t)
        if pos!=-1:
            if best is None or pos<best:
                best=pos
    return best

def _adjust_front(body,start):
    if start<=0: return 0
    i=start
    while i>0:
        if body[i] in _SAFE_FRONT:
            return i+1
        i-=1
    return 0

def _adjust_back(body,end):
    n=len(body)
    if end>=n: return n
    i=end
    while i<n:
        if body[i] in _SAFE_BACK:
            return i
        i+=1
    return n

def _protect_bullet_line(body,start,max_len):
    if start>=len(body): return start,None
    line_start=body.rfind("\n",0,start)
    if line_start==-1: line_start=0
    else: line_start+=1
    if line_start>=len(body): return start,None
    if body[line_start:line_start+1]!="・": return start,None
    line_end=body.find("\n",line_start)
    if line_end==-1: line_end=len(body)
    if line_end-line_start>max_len:
        return start,None
    return line_start,line_end

def _trim_to_max_len(body,start,end,max_len):
    if end-start<=max_len: return start,end
    hard_end=start+max_len
    if hard_end>=end: return start,end
    cut=None
    i=hard_end
    while i>start:
        if body[i] in _SAFE_BACK:
            cut=i; break
        i-=1
    if cut is None: cut=hard_end
    return start,cut

# ================================================================
# ★★★ 新 make_snippet（完全安定版：全文ここに差し替え済み）★★★
# ================================================================
def make_snippet(body: str, terms: List[str], is_first_in_page: bool) -> str:
    """
    スニペット生成（完全安定版）
    - 1件目：冒頭300文字
    - 2件目以降：ヒット語前後から最大160文字
      * 箇条書き行（・〜）の途中開始を禁止
      * 文の途中で切らないよう安全境界へ寄せる
      * どうしても区切れない場合は “…（省略記号）” を追加
    """

    if not body:
        return ""

    # ---------- 1件目はそのまま頭から300文字 ----------
    if is_first_in_page:
        return body[:300]

    max_len = 180

    # ---------- まずヒット位置 ----------
    hit_pos = _find_hit_pos(body, terms)
    if hit_pos is None:
        return body[:max_len]

    # ---------- 仮ウィンドウ：前後80 ----------
    raw_start = max(0, hit_pos - 80)
    raw_end   = min(len(body), hit_pos + 80)

    # ---------- 安全境界へ寄せる ----------
    safe_start = _adjust_front(body, raw_start)
    safe_end   = _adjust_back(body, raw_end)

    if safe_start >= safe_end:
        safe_start = raw_start
        safe_end   = raw_end

    # ---------- 箇条書き（・）の途中保護 ----------
    orig_start = safe_start
    safe_start, bullet_end = _protect_bullet_line(body, safe_start, max_len)

    # 箇条書き行が160文字を超えてしまうなら保護をやめる
    if bullet_end is not None and (bullet_end - safe_start) > max_len:
        safe_start = orig_start
        bullet_end = None

    # 箇条書きを含める必要があるなら end を伸ばす
    if bullet_end is not None and bullet_end > safe_end:
        safe_end = bullet_end

    # ---------- 160文字以内に収める ----------
    safe_start, safe_end = _trim_to_max_len(body, safe_start, safe_end, max_len)

    snippet = body[safe_start:safe_end]

    # ---------- 末尾が文途中なら "…" を追加 ----------
    if safe_end < len(body) and snippet and snippet[-1] not in _SAFE_BACK:
        snippet = snippet.rstrip() + "…"

    return snippet
# ================================================================

def build_item(rec: Dict[str,Any],terms: List[str],is_first_in_page: bool,matches=None,hit_field=None):
    title=record_as_text(rec,"title") or "(無題)"
    body =record_as_text(rec,"text") or ""
    snippet_src=make_snippet(body,terms,is_first_in_page=is_first_in_page)
    item={
        "title":highlight_simple(title,terms),
        "content":highlight_simple(snippet_src,terms),
        "url":record_as_text(rec,"url"),
        "rank":None,
        "date":record_as_text(rec,"date"),
    }
    if hit_field: item["hit_field"]=hit_field
    if matches is not None: item["matches"]=matches
    return item

TOKEN_RE=re.compile(r'"([^"]+)"|(\S+)')

def parse_query(q: str):
    must=[]; minus=[]; raw=[]
    for m in TOKEN_RE.finditer(normalize_text(q)):
        tok=m.group(1) if m.group(1) is not None else m.group(2)
        if not tok: continue
        raw.append(tok)
        if tok.startswith("-") and len(tok)>1:
            minus.append(tok[1:])
        else:
            must.append(tok)
    return must,minus,raw

# ====== scoring ======
CONNECTOR=r"[\s\u3000]*(?:の|・|／|/|_|\-|–|—)?[\s\u3000]*"

def gen_ngrams(tokens,nmax=3):
    toks=[t for t in tokens if t]
    out=[]
    for n in range(2,min(nmax,len(toks))+1):
        for i in range(len(toks)-n+1):
            out.append(toks[i:i+n])
    return out

def phrase_contiguous_present(text,phr):
    return "".join(phr) in text

def phrase_flexible_present(text,phr):
    if not phr: return False
    pat=re.escape(phr[0])
    for w in phr[1:]:
        pat+=CONNECTOR+re.escape(w)
    return re.search(pat,text) is not None

def min_token_distance(text,a,b):
    pa=[m.start() for m in re.finditer(re.escape(a),text)]
    pb=[m.start() for m in re.finditer(re.escape(b),text)]
    if not pa or not pb: return None
    best=None; i=j=0
    while i<len(pa) and j<len(pb):
        da,db=pa[i],pb[j]
        d=abs(da-db)
        if best is None or d<best: best=d
        if da<db: i+=1
        else: j+=1
    return best

def _score_stage_a(rec,tokens):
    ttl=rec.get("__ttl_norm",""); txt=rec.get("__txt_norm",""); tag=rec.get("__tag_norm","")
    score=0
    for raw in tokens:
        t=normalize_text(raw)
        if t:
            score+=3*ttl.count(t)+2*tag.count(t)+1*txt.count(t)
    for phr in gen_ngrams(tokens,3):
        if phrase_contiguous_present(ttl,phr): score+=BONUS_PHRASE_TTL
        if phrase_contiguous_present(txt,phr): score+=BONUS_PHRASE_BODY
    return score

def _score_stage_b(rec,tokens):
    ttl=rec.get("__ttl_norm",""); txt=rec.get("__txt_norm",""); tag=rec.get("__tag_norm","")
    ftt=rec.get("__ttl_fold",""); ftx=rec.get("__txt_fold",""); ftg=rec.get("__tag_fold","")
    score=0
    for raw in tokens:
        fr=fold_kana(normalize_text(raw))
        if fr:
            score+=3*ftt.count(fr)+2*ftg.count(fr)+1*ftx.count(fr)
    for phr in gen_ngrams(tokens,3):
        if phrase_flexible_present(ttl,phr): score+=BONUS_FLEXPH_TTL
        if phrase_flexible_present(txt,phr): score+=BONUS_FLEXPH_BODY
        if len(phr)==2:
            d1=min_token_distance(ttl,phr[0],phr[1])
            if d1 is not None and d1<=NEAR_WIN: score+=BONUS_NEAR_TTL
            d2=min_token_distance(txt,phr[0],phr[1])
            if d2 is not None and d2<=NEAR_WIN: score+=BONUS_NEAR_BODY
    return score

def _score_stage_c(rec,tokens):
    ftt=rec.get("__ttl_fold",""); ftx=rec.get("__txt_fold",""); ftg=rec.get("__tag_fold","")
    score=0
    for raw in tokens:
        fr=fold_kana(normalize_text(raw))
        if len(fr)>=2:
            if fuzzy_contains(fr,ftt): score+=1
            if fuzzy_contains(fr,ftg): score+=1
            if fuzzy_contains(fr,ftx): score+=1
    return score

def sort_key_relevance(entry):
    sc,d,did,_=entry
    return (-sc, -(d or datetime.min).timestamp(), did)

def _decide_hit_field(rec,terms):
    ttl=rec.get("__ttl_norm",""); txt=rec.get("__txt_norm",""); tag=rec.get("__tag_norm","")
    ftt=rec.get("__ttl_fold",""); ftx=rec.get("__txt_fold",""); ftg=rec.get("__tag_fold","")
    for raw in terms:
        t=normalize_text(raw); f=fold_kana(t)
        if (t and t in ttl) or (f and f in ftt): return "title"
    for raw in terms:
        t=normalize_text(raw); f=fold_kana(t)
        if (t and t in tag) or (f and f in ftg): return "tag"
    return "body"

# ====== /api/search ======
@app.get("/api/search")
def api_search(
    q: str = Query(""),
    page: int = Query(1,ge=1),
    page_size: int = Query(5,ge=1,le=50),
    order: str = Query("latest"),
    refresh: int = Query(0),
    logic: str = Query("and"),
    debug: int = Query(0),
):
    if refresh==1:
        _refresh_kb_globals(fetch_now=True)
        _cache.clear()

    if not _KB_ROWS:
        return json_utf8({"items":[], "total_hits":0, "error":"kb_not_loaded"},503)

    cache_key=(q,order,page,page_size,logic,debug)
    cached=_cache.get(cache_key)
    if cached is not None: return json_utf8(cached)

    must,minus,raw=parse_query(q)
    if not must and not minus:
        payload={"items":[], "total_hits":0,"page":page,"page_size":page_size,
                 "has_more":False,"next_page":None,"error":None}
        _cache.set(cache_key,payload)
        return json_utf8(payload)

    rows=_KB_ROWS

    # 候補抽出
    cand=[]
    for rec in rows:
        ttl=rec.get("__ttl_norm",""); txt=rec.get("__txt_norm",""); tag=rec.get("__tag_norm","")
        ftt=rec.get("__ttl_fold",""); ftx=rec.get("__txt_fold",""); ftg=rec.get("__tag_fold","")

        def contains_any(t):
            n=normalize_text(t)
            f=fold_kana(n)
            return (n and (n in ttl or n in txt or n in tag)) or \
                   (f and (f in ftt or f in ftx or f in ftg))

        if minus and any(contains_any(t) for t in minus): continue
        ok=True
        for t in must:
            if not contains_any(t): ok=False; break
        if ok: cand.append(rec)

    if not cand:
        payload={"items":[], "total_hits":0,"page":page,"page_size":page_size,
                 "has_more":False,"next_page":None,"error":None}
        _cache.set(cache_key,payload)
        return json_utf8(payload)

    # stage A
    k_terms=must or raw
    stage_a=[]
    for rec in cand:
        sc=_score_stage_a(rec,k_terms)
        if sc>0:
            stage_a.append((sc,rec.get("__date_obj"),rec.get("__doc_id"),rec))
    if not stage_a:
        payload={"items":[], "total_hits":0,"page":page,"page_size":page_size,
                 "has_more":False,"next_page":None,"error":None}
        _cache.set(cache_key,payload)
        return json_utf8(payload)
    stage_a.sort(key=sort_key_relevance)
    stage_b_cand=stage_a[:TOP_K_A]

    # stage B
    stage_b=[]
    for sc_a,d,did,rec in stage_b_cand:
        sc=sc_a+_score_stage_b(rec,k_terms)
        stage_b.append((sc,d,did,rec))
    stage_b.sort(key=sort_key_relevance)
    stage_c_cand=stage_b[:TOP_K_B]

    # stage C
    final=[]
    for sc_b,d,did,rec in stage_c_cand:
        sc=sc_b+_score_stage_c(rec,k_terms)
        final.append((sc,d,did,rec))

    # dedup
    best={}
    for sc,d,did,rec in final:
        prev=best.get(did)
        if prev is None:
            best[did]=(sc,d,did,rec)
        else:
            sc2,d2,_,_=prev
            if sc>sc2 or (sc==sc2 and (d or datetime.min)>(d2 or datetime.min)):
                best[did]=(sc,d,did,rec)
    dedup=list(best.values())

    # decorate
    decorated=[]
    for sc,d,did,rec in dedup:
        hf=_decide_hit_field(rec,k_terms)
        decorated.append((sc,d,did,rec,hf))

    bucket={"title":0,"tag":1,"body":2}
    if order=="latest":
        decorated.sort(key=lambda x:(bucket.get(x[4],9),-(x[1] or datetime.min).timestamp(),-x[0],x[2]))
    else:
        decorated.sort(key=lambda x:(bucket.get(x[4],9),-x[0],-(x[1] or datetime.min).timestamp(),x[2]))

    total=len(decorated)
    start=(page-1)*page_size
    end=start+page_size
    slice_=decorated[start:end]
    has_more=end<total
    next_page=page+1 if has_more else None

    items=[]
    for i,(sc,d,did,rec,hf) in enumerate(slice_):
        it=build_item(rec,k_terms,is_first_in_page=(i==0),matches=None,hit_field=hf)
        it["rank"]=start+i+1
        items.append(it)

    payload={
        "items":items,"total_hits":total,
        "page":page,"page_size":page_size,
        "has_more":has_more,"next_page":next_page,
        "error":None
    }
    _cache.set(cache_key,payload)
    return json_utf8(payload)

# ====== service worker ======
@app.get("/service-worker.js")
def get_sw():
    p=os.path.join("static","service-worker.js")
    if not os.path.exists(p):
        return Response("not found",404)
    return FileResponse(p,media_type="application/javascript; charset=utf-8",
                        headers={"Cache-Control":"no-cache","Service-Worker-Allowed":"/"})

# ====== UI ======
@app.get("/ui")
def ui():
    p=os.path.join("static","ui.html")
    if os.path.exists(p):
        return FileResponse(p,media_type="text/html; charset=utf-8")
    return PlainTextResponse("static/ui.html not found",404)

# ====== main ======
if __name__=="__main__":
    import uvicorn
    uvicorn.run(app,host="0.0.0.0",port=int(os.getenv("PORT","8000")))
