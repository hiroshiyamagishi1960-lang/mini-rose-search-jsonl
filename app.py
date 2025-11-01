# app.py — v5.3.2 (AND前提・除外語・診断debug、関連度バケット＋hit_field返却／UI変更なし)

import os, io, re, csv, json, hashlib, unicodedata, threading
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional, Set
from urllib.parse import urlparse, urlunparse, parse_qsl
from collections import OrderedDict

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, PlainTextResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

try:
    import requests
except Exception:
    requests = None

KB_URL    = (os.getenv("KB_URL", "") or "").strip()
KB_PATH   = os.path.normpath((os.getenv("KB_PATH", "kb.jsonl") or "kb.jsonl").strip())
VERSION   = os.getenv("APP_VERSION", "jsonl-2025-11-01-v5.3.2")  # ★ バージョン表記更新
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

app = FastAPI(title="mini-rose-search-jsonl (v5.3.2)")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=False,
    allow_methods=["GET"], allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="static"), name="static")

KB_LINES: int = 0
KB_HASH:  str = ""
LAST_ERROR: str = ""
LAST_EVENT: str = ""
_KB_ROWS: Optional[List[Dict[str, Any]]] = None

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
    try:
        return json.dumps(x, ensure_ascii=False)
    except Exception:
        return str(x)

KATA_TO_HIRA = str.maketrans({chr(k): chr(k - 0x60) for k in range(ord("ァ"), ord("ン") + 1)})
HIRA_SMALL2NORM = {"ぁ":"あ","ぃ":"い","ぅ":"う","ぇ":"え","ぉ":"お","ゃ":"や","ゅ":"ゆ","ょ":"よ","っ":"つ","ゎ":"わ"}
DAKUTEN = "\u3099"; HANDAKUTEN = "\u309A"
VOWELS = {"あ","い","う","え","お"}

def _strip_diacritics(hira: str) -> str:
    nfkd = unicodedata.normalize("NFD", hira)
    no_marks = "".join(ch for ch in nfkd if ch not in (DAKUTEN, HANDAKUTEN))
    return unicodedata.normalize("NFC", no_marks)

def _long_vowel_to_vowel(hira: str) -> str:
    out = []; prev = ""
    for ch in hira:
        if ch == "ー" and prev in VOWELS:
            out.append(prev)
        else:
            out.append(ch); prev = ch
    return "".join(out)

def fold_kana(s: str) -> str:
    if not s: return ""
    t = _nfkc(s)
    t = t.translate(KATA_TO_HIRA)
    t = "".join(HIRA_SMALL2NORM.get(ch, ch) for ch in t)
    t = _long_vowel_to_vowel(t)
    t = _strip_diacritics(t)
    return t

def _lev1_match(term: str, hay: str) -> bool:
    if not term or not hay: return False
    n, m = len(term), len(hay)
    if abs(n - m) > 1: return False
    if n == m:
        diff = 0
        for a, b in zip(term, hay):
            if a != b:
                diff += 1
                if diff > 1: return False
        return True
    if n > m: term, hay = hay, term; n, m = m, n
    i = j = diff = 0
    while i < n and j < m:
        if term[i] == hay[j]:
            i += 1; j += 1
        else:
            diff += 1
            if diff > 1: return False
            j += 1
    return True

def fuzzy_contains(term: str, text: str) -> bool:
    if not term or not text: return False
    n, m = len(term), len(text)
    if n == 1: return term in text
    if m < n - 1: return False
    lo = max(1, n - 1); hi = n + 1
    for L in (n, lo, hi):
        if L <= 0 or L > m: continue
        for i in range(0, m - L + 1):
            if _lev1_match(term, text[i:i+L]):
                return True
    return False

_syn_variant2canon: Dict[str, Set[str]] = {}
_syn_canon2variant: Dict[str, Set[str]] = {}

def _load_synonyms_from_csv(path: str):
    global _syn_variant2canon, _syn_canon2variant
    _syn_variant2canon = {}; _syn_canon2variant = {}
    if not path or not os.path.exists(path): return
    try:
        with io.open(path, "r", encoding="utf-8") as f:
            rdr = csv.reader(f); _ = next(rdr, None)
            for row in rdr:
                if len(row) < 2: continue
                canon = normalize_text(row[0]); vari = normalize_text(row[1])
                if not canon or not vari: continue
                _syn_canon2variant.setdefault(canon, set()).add(vari)
                _syn_variant2canon.setdefault(vari, set()).add(canon)
    except Exception:
        pass

TITLE_KEYS = ["title","Title","名前","タイトル","題名","見出し","subject","headline"]
TEXT_KEYS  = ["content","text","body","本文","内容","記事","description","summary","excerpt"]
DATE_KEYS  = ["開催日/発行日","date","Date","published_at","published","created_at","更新日","作成日","日付","開催日","発行日"]
URL_KEYS   = ["url","URL","link","permalink","出典URL","公開URL","source"]
ID_KEYS    = ["id","doc_id","record_id","ページID"]
AUTH_KEYS  = ["author","Author","writer","posted_by","著者","講師"]
TAG_KEYS   = ["tags","tag","タグ","区分","分類","カテゴリ","category","categories","keywords","キーワード"]

def record_as_text(rec: Dict[str, Any], field: str) -> str:
    key_map = {
        "title": TITLE_KEYS, "text": TEXT_KEYS, "date": DATE_KEYS,
        "url": URL_KEYS, "id": ID_KEYS, "author": AUTH_KEYS
    }
    keys = key_map.get(field, [field])
    for k in keys:
        v = rec.get(k)
        if v:
            return textify(v)
    return ""

def record_as_tags(rec: Dict[str, Any]) -> str:
    for k in TAG_KEYS:
        if k in rec and rec[k]:
            return textify(rec[k])
    return ""

_DROP_QS = {"source","utm_source","utm_medium","utm_campaign","utm_term","utm_content"}
_NOTION_PAGEID_RE = re.compile(r"[0-9a-f]{32}", re.IGNORECASE)

def _extract_notion_page_id(path: str) -> Optional[str]:
    m = _NOTION_PAGEID_RE.search(path.replace("-", ""))
    return m.group(0).lower() if m else None

def canonical_url(url: str) -> str:
    u = (url or "").strip()
    if not u or u.lower() in {"notion","null","none","undefined"}: return ""
    try:
        p = urlparse(u)
        p = p._replace(fragment="")
        qs_pairs = [(k, v) for (k, v) in parse_qsl(p.query, keep_blank_values=True) if k not in _DROP_QS]
        qs = "&".join([f"{k}={v}" if v != "" else k for k, v in qs_pairs])
        p = p._replace(query=qs)
        p = p._replace(scheme=(p.scheme or "").lower(), netloc=(p.netloc or "").lower())
        if "notion.site" in p.netloc:
            pid = _extract_notion_page_id(p.path)
            if pid:
                return f"notion://{pid}"
        return urlunparse(p)
    except Exception:
        return u

def stable_hash(*parts: str) -> str:
    h = hashlib.sha256()
    for part in parts:
        h.update((part or "").encode("utf-8")); h.update(b"\x1e")
    return h.hexdigest()

def doc_id_for(rec: Dict[str, Any]) -> str:
    rid = record_as_text(rec, "id").strip()
    if rid: return f"id://{rid}"
    url_c = canonical_url(record_as_text(rec, "url"))
    if url_c: return f"url://{url_c}"
    title_n = normalize_text(record_as_text(rec, "title"))
    date_n  = normalize_text(record_as_text(rec, "date"))
    auth_n  = normalize_text(record_as_text(rec, "author"))
    return f"hash://{stable_hash(title_n, date_n, auth_n)}"

_DATE_RE = re.compile(r"(?P<y>(?:19|20|21)\d{2})[./\-年]?(?:(?P<m>0?[1-9]|1[0-2])[./\-月]?(?:(?P<d>0?[1-9]|[12]\d|3[01])日?)?)?", re.UNICODE)
_ERA_RE  = re.compile(r"(令和|平成|昭和)\s*(\d{1,2})\s*年(?:\s*(\d{1,2})\s*月(?:\s*(\d{1,2})\s*日)?)?")

def _era_to_seireki(era: str, nen: int) -> int:
    base = {"令和":2018, "平成":1988, "昭和":1925}.get(era, None)
    return base + nen if base is not None else nen

def _first_valid_date_from_string(s: str) -> Optional[datetime]:
    if not s: return None
    t = _nfkc(s)
    m = _ERA_RE.search(t)
    if m:
        try:
            y = _era_to_seireki(m.group(1), int(m.group(2)))
            mm = int(m.group(3)) if m.group(3) else 1
            dd = int(m.group(4)) if m.group(4) else 1
            return datetime(y, mm, dd)
        except Exception:
            pass
    m2 = _DATE_RE.search(t)
    if m2:
        y = int(m2.group("y"))
        m = int(m2.group("m")) if m2.group("m") else 1
        d = int(m2.group("d")) if m2.group("d") else 1
        try:
            return datetime(y, m, d)
        except Exception:
            return None
    return None

def record_date(rec: Dict[str, Any]) -> Optional[datetime]:
    for k in DATE_KEYS:
        v = rec.get(k)
        if not v: continue
        dt = _first_valid_date_from_string(textify(v))
        if dt: return dt
    cand_year = None
    for field in ("text","title"):
        v = record_as_text(rec, field)
        for y in re.findall(r"(19\d{2}|20\d{2}|21\d{2})", _nfkc(v)):
            cand_year = max(int(y), cand_year or 0)
    if cand_year:
        return datetime(cand_year, 1, 1)
    u = record_as_text(rec, "url")
    if u:
        y_url = None
        for y in re.findall(r"(19\d{2}|20\d{2}|21\d{2})", _nfkc(u)):
            y_val = int(y)
            y_url = max(y_val, y_url or 0)
        if y_url:
            return datetime(y_url, 1, 1)
    return None

def _bytes_to_jsonl(blob: bytes) -> bytes:
    if not blob: return b""
    s = blob.decode("utf-8", errors="replace").strip()
    if not s: return b""
    if s.startswith("["):
        try:
            data = json.loads(s)
            if isinstance(data, list):
                lines = [json.dumps(obj, ensure_ascii=False, separators=(",", ":")) for obj in data]
                return ("\n".join(lines) + "\n").encode("utf-8")
        except Exception:
            return blob
    return blob

def _compute_lines_and_hash(path: str) -> Tuple[int, str]:
    cnt = 0; sha = hashlib.sha256()
    with open(path, "rb") as f:
        for line in f:
            sha.update(line)
            if line.strip(): cnt += 1
    return cnt, sha.hexdigest()

def _load_rows_into_memory(path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not os.path.exists(path): return rows
    with io.open(path, "r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln: continue
            try:
                rows.append(json.loads(ln))
            except Exception:
                continue
    return rows

PREVIEW_LIMIT = 120000

def _attach_precomputed_fields(rows: List[Dict[str, Any]]):
    for rec in rows:
        title = record_as_text(rec, "title") or ""
        text  = record_as_text(rec, "text") or ""
        tags  = record_as_tags(rec)
        rec["__ttl_norm"]  = normalize_text(title)
        rec["__txt_norm"]  = normalize_text(text)
        rec["__tag_norm"]  = normalize_text(tags)
        rec["__ttl_fold"]  = fold_kana(rec["__ttl_norm"]) if rec["__ttl_norm"] else ""
        txt_for_fold = rec["__txt_norm"][:PREVIEW_LIMIT]
        rec["__txt_fold"]  = fold_kana(txt_for_fold) if txt_for_fold else ""
        rec["__tag_fold"]  = fold_kana(rec["__tag_norm"]) if rec["__tag_norm"] else ""
        rec["__doc_id"]    = doc_id_for(rec)
        rec["__date_obj"]  = record_date(rec)

def ensure_kb(fetch_now: bool = False) -> Tuple[int, str]:
    global LAST_ERROR, LAST_EVENT, _KB_ROWS
    LAST_ERROR = ""; LAST_EVENT = ""
    if fetch_now and KB_URL and requests is not None:
        try:
            r = requests.get(KB_URL, timeout=5)
            r.raise_for_status()
            blob = _bytes_to_jsonl(r.content)
            tmp = KB_PATH + ".tmp"
            os.makedirs(os.path.dirname(KB_PATH), exist_ok=True) if os.path.dirname(KB_PATH) else None
            with open(tmp, "wb") as wf: wf.write(blob)
            os.replace(tmp, KB_PATH)
            LAST_EVENT = "fetched"
        except Exception as e:
            LAST_ERROR = f"fetch_or_save_failed: {type(e).__name__}: {e}"
    if os.path.exists(KB_PATH):
        try:
            lines, sha = _compute_lines_and_hash(KB_PATH)
            rows = _load_rows_into_memory(KB_PATH)
            _attach_precomputed_fields(rows)
            _load_synonyms_from_csv(SYN_CSV)
            _KB_ROWS = rows
            return lines, sha
        except Exception as e:
            LAST_ERROR = f"hash_failed: {type(e).__name__}: {e}"
            _KB_ROWS = []
            return 0, ""
    else:
        LAST_EVENT = LAST_EVENT or "no_file"
        _KB_ROWS = []
        return 0, ""

def _refresh_kb_globals(fetch_now: bool = False):
    global KB_LINES, KB_HASH
    lines, sha = ensure_kb(fetch_now=fetch_now)
    KB_LINES, KB_HASH = lines, sha
    _cache.clear()
    return lines, sha

def _bg_fetch_kb():
    try:
        _refresh_kb_globals(fetch_now=True)
    except Exception:
        pass

@app.on_event("startup")
def _startup():
    try:
        _refresh_kb_globals(fetch_now=False)
    except Exception:
        pass
    if KB_URL and requests is not None:
        th = threading.Thread(target=_bg_fetch_kb, daemon=True)
        th.start()

RANGE_SEP = r"(?:-|–|—|~|〜|～|\.{2})"

def _parse_year_from_query(q_raw: str) -> Tuple[str, Optional[int], Optional[Tuple[int,int]]]:
    q = _nfkc(q_raw).strip()
    if not q: return "", None, None
    parts = q.replace("　"," ").split()
    last = parts[-1] if parts else ""
    if re.fullmatch(r"(19|20|21)\d{2}", last):
        return (" ".join(parts[:-1]).strip(), int(last), None)
    m_rng = re.fullmatch(rf"((?:19|20|21)\d{{2}})\s*{RANGE_SEP}\s*((?:19|20|21)\d{{2}})", last)
    if m_rng:
        y1, y2 = int(m_rng.group(1)), int(m_rng.group(2))
        if y1 > y2: y1, y2 = y2, y1
        return (" ".join(parts[:-1]).strip(), None, (y1, y2))
    m_suf = re.fullmatch(rf"^(.*?)(?:((?:19|20|21)\d{{2}}))$", last)
    if m_suf and m_suf.group(1):
        return (" ".join(parts[:-1] + [m_suf.group(1)]).strip(), int(m_suf.group(2)), None)
    return (q, None, None)

def _record_years(rec: Dict[str, Any]) -> List[int]:
    ys = set()
    d = rec.get("__date_obj") or record_date(rec)
    if d: ys.add(d.year)
    for field in ("text","title","url","author"):
        v = record_as_text(rec, field)
        for y in re.findall(r"(19\d{2}|20\d{2}|21\d{2})", _nfkc(v)):
            ys.add(int(y))
    return sorted(ys)

def _matches_year(rec: Dict[str, Any], year: Optional[int], yr: Optional[Tuple[int,int]]) -> bool:
    if year is None and yr is None: return True
    ys = _record_years(rec)
    if not ys: return False
    if year is not None: return year in ys
    lo, hi = yr
    return any(lo <= y <= hi for y in ys)

def expand_with_synonyms(term: str) -> Set[str]:
    t = normalize_text(term)
    out: Set[str] = {t}
    for canon in _syn_variant2canon.get(t, set()):
        out.add(canon); out.update(_syn_canon2variant.get(canon, set()))
    if t in _syn_canon2variant:
        out.update(_syn_canon2variant[t])
    return out

CONNECTOR = r"[\s\u3000]*(?:の|・|／|/|_|\-|–|—)?[\s\u3000]*"

def gen_ngrams(tokens: List[str], nmax: int = 3) -> List[List[str]]:
    toks = [t for t in tokens if t]
    out: List[List[str]] = []
    for n in range(2, min(nmax, len(toks)) + 1):
        for i in range(len(toks)-n+1):
            out.append(toks[i:i+n])
    return out

def phrase_contiguous_present(text: str, phrase: List[str]) -> bool:
    joined = "".join(phrase)
    return joined in text

def phrase_flexible_present(text: str, phrase: List[str]) -> bool:
    if not phrase: return False
    pat = re.escape(phrase[0])
    for w in phrase[1:]:
        pat += CONNECTOR + re.escape(w)
    return re.search(pat, text) is not None

def min_token_distance(text: str, a: str, b: str) -> Optional[int]:
    pos_a = [m.start() for m in re.finditer(re.escape(a), text)]
    pos_b = [m.start() for m in re.finditer(re.escape(b), text)]
    if not pos_a or not pos_b: return None
    i=j=0; best=None
    while i<len(pos_a) and j<len(pos_b):
        da = pos_a[i]; db = pos_b[j]
        d = abs(da - db)
        if best is None or d < best: best = d
        if da < db: i += 1
        else: j += 1
    return best

def html_escape(s: str) -> str:
    return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def highlight_simple(text: str, terms: List[str]) -> str:
    if not text: return ""
    esc = html_escape(text)
    hlset: Set[str] = set()
    for t in terms:
        hlset.add(normalize_text(t))
        hlset |= expand_with_synonyms(t)
    for t in sorted(hlset, key=len, reverse=True):
        if not t: continue
        et = html_escape(t)
        esc = re.sub(re.escape(et), lambda m: f"<mark>{m.group(0)}</mark>", esc)
    return esc

def build_item(rec: Dict[str, Any], terms: List[str], is_first_in_page: bool, matches: Optional[Dict[str,List[str]]] = None, hit_field: Optional[str] = None) -> Dict[str, Any]:  # ★ 引数に hit_field を追加
    title = record_as_text(rec, "title") or "(無題)"
    body  = record_as_text(rec, "text") or ""
    if is_first_in_page:
        snippet_src = body[:300]
    else:
        pos = -1
        for t in terms:
            t = normalize_text(t)
            if not t: continue
            p = body.find(t)
            if p >= 0: pos = p; break
        if pos < 0:
            snippet_src = body[:160]
        else:
            start = max(0, pos - 80); end = min(len(body), pos + 80)
            snippet_src = ("…" if start>0 else "") + body[start:end] + ("…" if end<len(body) else "")
    item = {
        "title":   highlight_simple(title, terms),
        "content": highlight_simple(snippet_src, terms),
        "url":     record_as_text(rec, "url"),
        "rank":    None,
        "date":    record_as_text(rec, "date"),
    }
    if hit_field:  # ★ UI で（タイトル/タグ/本文にヒット）表示に使う
        item["hit_field"] = hit_field
    if matches is not None:
        item["matches"] = matches
    return item

TOKEN_RE = re.compile(r'"([^"]+)"|(\S+)')

def parse_query(q: str) -> Tuple[List[str], List[str], List[str]]:
    must: List[str] = []
    minus: List[str] = []
    raw: List[str] = []
    for m in TOKEN_RE.finditer(normalize_text(q)):
        tok = m.group(1) if m.group(1) is not None else m.group(2)
        if not tok: continue
        raw.append(tok)
        if tok.startswith("-") and len(tok) > 1:
            minus.append(tok[1:])
        else:
            must.append(tok)
    return must, minus, raw

def _score_stage_a(rec: Dict[str, Any], tokens: List[str]) -> int:
    ttl = rec.get("__ttl_norm", ""); txt = rec.get("__txt_norm", ""); tag = rec.get("__tag_norm","")
    score = 0
    for raw in tokens:
        exts = expand_with_synonyms(raw) or {raw}
        for t in exts:
            if t:
                if ttl.count(t) > 0: score += 3 * ttl.count(t)
                if tag.count(t) > 0: score += 2 * tag.count(t)
                if txt.count(t) > 0: score += 1 * txt.count(t)
    for phr in gen_ngrams(tokens, 3):
        if phrase_contiguous_present(ttl, phr): score += BONUS_PHRASE_TTL
        if phrase_contiguous_present(txt, phr): score += BONUS_PHRASE_BODY
    return score

def _score_stage_b(rec: Dict[str, Any], tokens: List[str]) -> int:
    ttl = rec.get("__ttl_norm", ""); txt = rec.get("__txt_norm", ""); tag = rec.get("__tag_norm","")
    ftt = rec.get("__ttl_fold", ""); ftx = rec.get("__txt_fold", ""); ftg = rec.get("__tag_fold","")
    score = 0
    for raw in tokens:
        fr = fold_kana(normalize_text(raw))
        if fr:
            if ftt.count(fr) > 0: score += 3 * ftt.count(fr)
            if ftg.count(fr) > 0: score += 2 * ftg.count(fr)
            if ftx.count(fr) > 0: score += 1 * ftx.count(fr)
    for phr in gen_ngrams(tokens, 3):
        if phrase_flexible_present(ttl, phr): score += BONUS_FLEXPH_TTL
        if phrase_flexible_present(txt, phr): score += BONUS_FLEXPH_BODY
        if len(phr) == 2:
            d1 = min_token_distance(ttl, phr[0], phr[1])
            if d1 is not None and d1 <= NEAR_WIN: score += BONUS_NEAR_TTL
            d2 = min_token_distance(txt, phr[0], phr[1])
            if d2 is not None and d2 <= NEAR_WIN: score += BONUS_NEAR_BODY
    return score

def _score_stage_c(rec: Dict[str, Any], tokens: List[str]) -> int:
    ftt = rec.get("__ttl_fold", ""); ftx = rec.get("__txt_fold", ""); ftg = rec.get("__tag_fold","")
    score = 0
    for raw in tokens:
        fr = fold_kana(normalize_text(raw))
        if len(fr) >= 2:
            if fuzzy_contains(fr, ftt): score += 1
            if fuzzy_contains(fr, ftg): score += 1
            if fuzzy_contains(fr, ftx): score += 1
    return score

def sort_key_relevance(entry: Tuple[int, Optional[datetime], str, Dict[str, Any]]) -> Tuple:
    score, d, did, _ = entry
    date_key = d or datetime.min
    return (-int(score), -int(date_key.strftime("%Y%m%d%H%M%S")), did)

def sort_key_latest(entry: Tuple[int, Optional[datetime], str, Dict[str, Any]]) -> Tuple:
    score, d, did, _ = entry
    date_key = d or datetime.min
    return (-int(date_key.strftime("%Y%m%d%H%M%S")), -int(score), did)

def json_utf8(payload: Dict[str, Any], status: int = 200) -> JSONResponse:
    return JSONResponse(
        payload,
        status_code=status,
        media_type="application/json; charset=utf-8",
        headers={"Cache-Control":"no-store","Content-Type":"application/json; charset=utf-8"},
    )

class LRU:
    def __init__(self, cap: int):
        self.cap = cap
        self._d: OrderedDict[Tuple, Dict[str, Any]] = OrderedDict()
        self._ver: str = ""
    def clear(self):
        self._d.clear()
        self._ver = KB_HASH
    def get(self, key: Tuple):
        if self._ver != KB_HASH:
            self.clear()
            return None
        v = self._d.get(key)
        if v is None: return None
        self._d.move_to_end(key)
        return v
    def set(self, key: Tuple, val: Dict[str, Any]):
        if self._ver != KB_HASH:
            self.clear()
        self._d[key] = val
        self._d.move_to_end(key)
        if len(self._d) > self.cap:
            self._d.popitem(last=False)

_cache = LRU(CACHE_SIZE)

@app.get("/health")
def health():
    ok = os.path.exists(KB_PATH) and KB_LINES > 0
    return json_utf8({"ok": ok, "kb_url": KB_URL, "kb_size": KB_LINES, "kb_fingerprint": KB_HASH})

@app.get("/version")
def version():
    return json_utf8({"version": VERSION})

@app.get("/ui")
def ui():
    path = os.path.join("static", "ui.html")
    if os.path.exists(path):
        return FileResponse(path, media_type="text/html; charset=utf-8")
    return PlainTextResponse("static/ui.html not found", status_code=404)

@app.get("/admin/refresh")
def admin_refresh():
    lines, sha = _refresh_kb_globals(fetch_now=True)
    return json_utf8({"ok": lines>0, "kb_size": lines, "kb_fingerprint": sha,
                      "last_event": LAST_EVENT, "last_error": LAST_ERROR})

# ★ 追加：ヒットフィールドの決定（title > tags > body）
def _decide_hit_field(rec: Dict[str, Any], terms: List[str]) -> str:
    if not terms: return ""
    ttl = rec.get("__ttl_norm",""); txt = rec.get("__txt_norm",""); tag = rec.get("__tag_norm","")
    ftt = rec.get("__ttl_fold",""); ftx = rec.get("__txt_fold",""); ftg = rec.get("__tag_fold","")

    def present_in(s_norm: str, s_fold: str) -> bool:
        for raw in terms:
            exts = expand_with_synonyms(raw) or {raw}
            for t in exts:
                if not t: continue
                t_n = normalize_text(t); t_f = fold_kana(t_n)
                if (t_n and t_n in s_norm) or (t_f and t_f in s_fold):
                    return True
        return False

    if present_in(ttl, ftt): return "title"
    if present_in(tag, ftg): return "tag"
    if present_in(txt, ftx): return "body"
    return ""  # 想定外（AND 判定後なので通常到達しない）

def _calc_matches_for_debug(rec: Dict[str,Any], terms: List[str]) -> Dict[str,List[str]]:
    ttl = rec.get("__ttl_norm",""); txt = rec.get("__txt_norm",""); tag = rec.get("__tag_norm","")
    hit_ttl: List[str] = []; hit_tag: List[str] = []; hit_txt: List[str] = []
    for t in terms:
        exts = expand_with_synonyms(t) or {t}
        took = normalize_text(t)
        ok_t = any((et in ttl) for et in exts)
        ok_g = any((et in tag) for et in exts)
        ok_b = any((et in txt) for et in exts)
        if ok_t: hit_ttl.append(took)
        if ok_g: hit_tag.append(took)
        if ok_b: hit_txt.append(took)
    out = {}
    if hit_ttl: out["title"] = hit_ttl
    if hit_tag: out["tags"]  = hit_tag
    if hit_txt: out["body"]  = hit_txt
    return out

@app.get("/api/search")
def api_search(
    q: str = Query("", description="検索クエリ（-語=除外、末尾年/範囲はフィルタ可）"),
    page: int = Query(1, ge=1),
    page_size: int = Query(5, ge=1, le=50),
    order: str = Query("latest", pattern="^(relevance|latest)$"),
    refresh: int = Query(0, description="1=kb.jsonl / 同義語CSV を再取得・再読み込み"),
    logic: str = Query("and", pattern="^(and|or)$", description="and=両語必須（既定）/ or=どれか一致"),
    debug: int = Query(0, description="1で各件のヒット内訳を返す（診断用）"),
):
    try:
        if refresh == 1:
            _refresh_kb_globals(fetch_now=True)
            _cache.clear()

        if not os.path.exists(KB_PATH) or KB_LINES <= 0:
            return json_utf8({"items": [], "total_hits": 0, "page": page, "page_size": page_size,
                              "has_more": False, "next_page": None, "error": "kb_missing", "order_used": order})

        cache_key = (q, order, page, page_size, logic, debug)
        cached = _cache.get(cache_key)
        if cached is not None:
            return json_utf8(cached)

        base_q, y_tail, yr_tail = _parse_year_from_query(q)
        must_terms, minus_terms, raw_terms = parse_query(base_q)
        if not must_terms and not minus_terms:
            payload = {"items": [], "total_hits": 0, "page": page, "page_size": page_size,
                       "has_more": False, "next_page": None, "error": None, "order_used": order}
            _cache.set(cache_key, payload)
            return json_utf8(payload)

        rows = _KB_ROWS or []

        candidates: List[Dict[str, Any]] = []
        for rec in rows:
            if y_tail is not None or yr_tail is not None:
                if not _matches_year(rec, y_tail, yr_tail):
                    continue

            ttl = rec.get("__ttl_norm",""); txt = rec.get("__txt_norm",""); tag = rec.get("__tag_norm","")
            ftt = rec.get("__ttl_fold",""); ftx = rec.get("__txt_fold",""); ftg = rec.get("__tag_fold","")

            def contains_any(term: str) -> bool:
                exts = expand_with_synonyms(term) or {term}
                for t in exts:
                    if t and (t in ttl or t in txt or t in tag):
                        return True
                    ft = fold_kana(t)
                    if ft and (ft in ftt or ft in ftx or ft in ftg):
                        return True
                return False

            if minus_terms and any(contains_any(t) for t in minus_terms):
                continue

            if logic == "or":
                if not must_terms or any(contains_any(t) for t in must_terms):
                    candidates.append(rec)
            else:
                ok = True
                for t in must_terms:
                    if not contains_any(t):
                        ok = False; break
                if ok:
                    candidates.append(rec)

        if not candidates:
            payload = {"items": [], "total_hits": 0, "page": page, "page_size": page_size,
                       "has_more": False, "next_page": None, "error": None, "order_used": order}
            _cache.set(cache_key, payload)
            return json_utf8(payload)

        # ---- スコアリング（既存3段階） ----
        stage_a: List[Tuple[int, Optional[datetime], str, Dict[str, Any]]] = []
        k_terms = must_terms or raw_terms
        for rec in candidates:
            sc = _score_stage_a(rec, k_terms)
            if sc <= 0: continue
            stage_a.append((sc, rec.get("__date_obj"), rec.get("__doc_id"), rec))
        if not stage_a:
            payload = {"items": [], "total_hits": 0, "page": page, "page_size": page_size,
                       "has_more": False, "next_page": None, "error": None, "order_used": order}
            _cache.set(cache_key, payload)
            return json_utf8(payload)
        stage_a.sort(key=sort_key_relevance)
        stage_b_candidates = stage_a[:TOP_K_A]

        stage_b: List[Tuple[int, Optional[datetime], str, Dict[str, Any]]] = []
        for sc_a, d, did, rec in stage_b_candidates:
            sc = sc_a + _score_stage_b(rec, k_terms)
            stage_b.append((sc, d, did, rec))
        stage_b.sort(key=sort_key_relevance)
        stage_c_candidates = stage_b[:TOP_K_B]

        final_list: List[Tuple[int, Optional[datetime], str, Dict[str, Any]]] = []
        for sc_b, d, did, rec in stage_c_candidates:
            sc = sc_b + _score_stage_c(rec, k_terms)
            final_list.append((sc, d, did, rec))

        # ---- 同一ドキュメント統合（既存） ----
        best_by_id: Dict[str, Tuple[int, Optional[datetime], str, Dict[str, Any]]] = {}
        for entry in final_list:
            sc, d, did, rec = entry
            prev = best_by_id.get(did)
            if prev is None:
                best_by_id[did] = entry
            else:
                psc, pd, _, _ = prev
                if (sc > psc) or (sc == psc and (d or datetime.min) > (pd or datetime.min)):
                    best_by_id[did] = entry
        deduped = list(best_by_id.values())

        # ---- ★ 追加：hit_field を決定してデコレート ----
        decorated: List[Tuple[int, Optional[datetime], str, Dict[str, Any], str]] = []
        for sc, d, did, rec in deduped:
            hf = _decide_hit_field(rec, k_terms) or "body"
            decorated.append((sc, d, did, rec, hf))

        # ---- ★ 並び順：関連度バケット（title->tag->body）→ 日付降順 → スコア降順
        if order == "latest":
            bucket_order = {"title":0, "tag":1, "body":2}
            decorated.sort(key=lambda x: (bucket_order.get(x[4], 9), -(x[1] or datetime.min).timestamp(), -x[0], x[2]))
            order_used = "latest"
        else:
            # relevance 指定時は従来の「スコア→日付」だが、hit_field を同率時の第0優先に挿入
            bucket_order = {"title":0, "tag":1, "body":2}
            decorated.sort(key=lambda x: (bucket_order.get(x[4], 9), -x[0], -(x[1] or datetime.min).timestamp(), x[2]))
            order_used = "relevance"

        # ---- ページング
        total = len(decorated)
        start = (page - 1) * page_size
        end   = start + page_size
        page_slice = decorated[start:end]
        has_more = end < total
        next_page = page + 1 if has_more else None

        # ---- items 生成（hit_field を返す）
        items: List[Dict[str, Any]] = []
        for i, (sc, d, did, rec, hf) in enumerate(page_slice):
            m = _calc_matches_for_debug(rec, k_terms) if debug == 1 else None
            items.append(build_item(rec, k_terms, is_first_in_page=(i == 0), matches=m, hit_field=hf))
        for idx, _ in enumerate(decorated, start=1):
            if start < idx <= end:
                items[idx - start - 1]["rank"] = idx

        payload = {
            "items": items,
            "total_hits": total,
            "page": page,
            "page_size": page_size,
            "has_more": has_more,
            "next_page": next_page,
            "error": None,
            "order_used": order_used,
        }
        _cache.set(cache_key, payload)
        return json_utf8(payload)

    except Exception as e:
        return json_utf8({"items": [], "total_hits": 0, "page": 1, "page_size": page_size,
                          "has_more": False, "next_page": None, "error": "exception", "message": textify(e)})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
