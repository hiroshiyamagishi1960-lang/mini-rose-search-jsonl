# app.py — ミニバラ盆栽愛好会 デジタル資料館（JSONL版・検索仕様アップデート v3.1+d）
# 変更点（v3.1+d）:
# - ★重複排除をより厳密に: id が無い場合は (title_norm, url_canon) をキーに集約
#   ・url_canon: #fragment 除去、? の既知パラメータ（source=copy_link 等）を削除して正規化
# - ★並び順の決定化: 同点時のページ跨ぎ再出現を防止（score↓, date↓, url_canon↑, title_norm↑）
# - その他の検索仕様（v3.1の年末尾フィルタ/かな・カナ/同義語/ハイライト/モジバケ修復）は維持
#
# ※ UI 変更は不要（/api/search の返却のみ一意化/安定化）

import os, io, re, json, hashlib, unicodedata
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional
from urllib.parse import urlparse, urlunparse, parse_qsl

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, FileResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware

try:
    import requests
except Exception:
    requests = None

# ==================== 初期化 ====================
app = FastAPI(title="mini-rose-search-jsonl (kb.jsonl)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ==================== 設定 ====================
KB_URL   = (os.getenv("KB_URL", "") or "").strip()
_cfg     = (os.getenv("KB_PATH", "kb.jsonl") or "kb.jsonl").strip()
KB_PATH  = os.path.normpath(_cfg if os.path.isabs(_cfg) else os.path.join(os.getcwd(), _cfg))
VERSION  = os.getenv("APP_VERSION", "jsonl-2025-10-21-v3.1+d-stable")

MOJIBAKE_REPAIR = (os.getenv("MOJIBAKE_REPAIR", "1") != "0")  # 既定 ON

# ==================== 診断用 ====================
KB_LINES: int = 0
KB_HASH:  str = ""
LAST_ERROR: str = ""
LAST_EVENT: str = ""

_REPAIR_STATS = {
    "title_repaired": 0,
    "snippet_repaired": 0,
    "title_suspected": 0,
    "snippet_suspected": 0,
}

# メモリ常駐（高速化）
_KB_ROWS: Optional[List[Dict[str, Any]]] = None

# ==================== 正規化ユーティリティ ====================
def _nfkc(s: Optional[str]) -> str:
    return unicodedata.normalize("NFKC", s or "")

KATA_TO_HIRA = str.maketrans({chr(k): chr(k - 0x60) for k in range(ord("ァ"), ord("ン") + 1)})
HIRA_TO_KATA = str.maketrans({chr(h): chr(h + 0x60) for h in range(ord("ぁ"), ord("ん") + 1)})

def to_hira(s: str) -> str:
    return (s or "").translate(KATA_TO_HIRA)

def to_kata(s: str) -> str:
    return (s or "").translate(HIRA_TO_KATA)

def normalize_text(s: str) -> str:
    if not s:
        return ""
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

# ---- URL 正規化（dedupe用）----
# 追跡/複製由来のクエリは無視して同一視する（必要に応じて追加）
_DROP_QS = {"source", "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content"}

def canonical_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    try:
        p = urlparse(u)
        # fragment は比較対象外
        fragless = p._replace(fragment="")
        # 主要でないクエリは落とす（? を全部消すのではなく、残す価値のあるものだけ残す仕様にしたければここで調整）
        qs_pairs = [(k, v) for (k, v) in parse_qsl(fragless.query, keep_blank_values=True) if k not in _DROP_QS]
        qs = "&".join([f"{k}={v}" if v != "" else k for k, v in qs_pairs])
        norm = fragless._replace(query=qs)
        # スキーム/ホストは小文字化
        norm = norm._replace(scheme=norm.scheme.lower(), netloc=norm.netloc.lower())
        return urlunparse(norm)
    except Exception:
        return u

# ==================== 同義語（最小） ====================
SYNONYMS: Dict[str, List[str]] = {
    "苔": ["コケ", "こけ"],
    "コケ": ["苔", "こけ"],
    "こけ": ["苔", "コケ"],
}

# ==================== KB 取得・常駐 ====================
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
    cnt = 0
    sha = hashlib.sha256()
    with open(path, "rb") as f:
        for line in f:
            sha.update(line)
            if line.strip():
                cnt += 1
    return cnt, sha.hexdigest()

def _load_rows_into_memory(path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not os.path.exists(path): return rows
    with io.open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line: continue
            try:
                rows.append(json.loads(line))
            except Exception:
                continue
    return rows

def ensure_kb() -> Tuple[int, str]:
    global LAST_ERROR, LAST_EVENT, _KB_ROWS
    LAST_ERROR = ""; LAST_EVENT = ""
    if KB_URL:
        if requests is None:
            LAST_ERROR = "requests unavailable"
        else:
            try:
                r = requests.get(KB_URL, timeout=30)
                r.raise_for_status()
                blob = _bytes_to_jsonl(r.content)
                tmp = KB_PATH + ".tmp"
                os.makedirs(os.path.dirname(KB_PATH), exist_ok=True) if os.path.dirname(KB_PATH) else None
                with open(tmp, "wb") as wf:
                    wf.write(blob)
                os.replace(tmp, KB_PATH)
                LAST_EVENT = "fetched"
            except Exception as e:
                LAST_ERROR = f"fetch_or_save_failed: {type(e).__name__}: {e}"
    if os.path.exists(KB_PATH):
        try:
            lines, sha = _compute_lines_and_hash(KB_PATH)
            if lines <= 0:
                LAST_EVENT = LAST_EVENT or "empty_file"
            _KB_ROWS = _load_rows_into_memory(KB_PATH)
            return lines, sha
        except Exception as e:
            LAST_ERROR = f"hash_failed: {type(e).__name__}: {e}"
            _KB_ROWS = []
            return 0, ""
    else:
        LAST_EVENT = LAST_EVENT or "no_file"
        _KB_ROWS = []
        return 0, ""

def _refresh_kb_globals():
    global KB_LINES, KB_HASH
    lines, sha = ensure_kb()
    KB_LINES, KB_HASH = lines, sha
    return lines, sha

@app.on_event("startup")
def _startup():
    try:
        _refresh_kb_globals()
    except Exception as e:
        global KB_LINES, KB_HASH, LAST_ERROR
        KB_LINES, KB_HASH = 0, ""
        LAST_ERROR = f"startup_failed: {type(e).__name__}: {e}"

# ==================== 日付/年 抽出 ====================
JP_DATE_PATTERNS = [
    re.compile(r"^(\d{4})年(\d{1,2})月(\d{1,2})日?$"),
    re.compile(r"^(\d{4})年(\d{1,2})月$"),
    re.compile(r"^(\d{4})年$"),
]

def parse_date_str(s: str) -> Optional[datetime]:
    if not s: return None
    s = normalize_text(s)
    for pat in JP_DATE_PATTERNS:
        m = pat.match(s)
        if m:
            y = int(m.group(1))
            mth = int(m.group(2)) if len(m.groups()) >= 2 and m.group(2) else 1
            day = int(m.group(3)) if len(m.groups()) >= 3 and m.group(3) else 1
            try: return datetime(y, mth, day)
            except Exception: return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d", "%Y-%m", "%Y/%m", "%Y.%m", "%Y"):
        try: return datetime.strptime(s[:len(fmt)], fmt)
        except Exception: continue
    m = re.match(r"^(\d{4})", s)
    if m:
        try: return datetime(int(m.group(1)), 1, 1)
        except Exception: return None
    return None

def record_date(rec: Dict[str, Any]) -> Optional[datetime]:
    for k in (
        "date","date_primary","Date","published_at","published","created_at",
        # 日本語キーを追加
        "開催日/発行日","開催日","発行日","日付","作成日","更新日"
    ):
        d = rec.get(k)
        if d:
            dt_ = parse_date_str(textify(d))
            if dt_: return dt_
    return None

YEAR_RE = re.compile(r"(19\d{2}|20\d{2}|21\d{2})")

def _years_from_text(s: str) -> List[int]:
    if not s: return []
    s = _nfkc(textify(s))
    ys = [int(y) for y in YEAR_RE.findall(s)]
    return sorted(set(ys))

def _record_years(rec: Dict[str, Any]) -> List[int]:
    ys = set()
    d = record_date(rec)
    if d: ys.add(d.year)
    for field in ("issue","title","text","url","category","author"):
        v = rec.get(field) if field in rec else None
        if v:
            for y in _years_from_text(v):
                ys.add(y)
    return sorted(ys)

RANGE_SEP = r"(?:-|–|—|~|〜|～|\.{2})"

def _parse_year_from_query(q_raw: str) -> Tuple[str, Optional[int], Optional[Tuple[int,int]]]:
    """
    末尾に年 or 年範囲があるときだけ年フィルタを発動。
    さらに「末尾連結の年」（例: 'コンテスト2024'）も年フィルタとして解釈。
    先頭年（例: '2024コンテスト'）はフィルタ不発（リテラル検索）。
    """
    q = _nfkc(q_raw).strip()
    if not q: return "", None, None

    parts = q.replace("　"," ").split()
    if not parts:
        return "", None, None

    last = parts[-1]

    # 1) 末尾が「YYYY」だけのトークン
    if re.fullmatch(r"(19|20|21)\d{2}", last):
        base = " ".join(parts[:-1]).strip()
        return (base, int(last), None)

    # 2) 末尾が「YYYY-YYYY」等のトークン
    m_rng = re.fullmatch(rf"((?:19|20|21)\d{{2}})\s*{RANGE_SEP}\s*((?:19|20|21)\d{{2}})", last)
    if m_rng:
        y1, y2 = int(m_rng.group(1)), int(m_rng.group(2))
        if y1 > y2: y1, y2 = y2, y1
        base = " ".join(parts[:-1]).strip()
        return (base, None, (y1, y2))

    # 3) 末尾連結（例: 'コンテスト2024' → 最後のトークン内部の末尾が年）
    m_suf = re.fullmatch(rf"^(.*?)(?:((?:19|20|21)\d{{2}}))$", last)
    if m_suf:
        prefix, ystr = m_suf.group(1), m_suf.group(2)
        if prefix:
            base = " ".join(parts[:-1] + [prefix]).strip()
            return (base, int(ystr), None)

    # それ以外：年フィルタ不発（リテラル検索）
    return (q, None, None)

def _matches_year(rec: Dict[str, Any], year: Optional[int], yr: Optional[Tuple[int,int]]) -> bool:
    if year is None and yr is None: return True
    ys = _record_years(rec)
    if not ys: return False
    if year is not None: return year in ys
    lo, hi = yr[0], yr[1]
    return any(lo <= y <= hi for y in ys)

# ==================== フィールド抽出 ====================
TITLE_KEYS = [
    "title","Title","name","Name","page_title","source_title","heading","headline","subject",
    # 日本語キーを追加
    "タイトル","題名","見出し","名前","表題"
]
TEXT_KEYS  = [
    "text","content","body","description","summary","note","content_full","excerpt",
    # 日本語キーを追加
    "本文","内容","記事","テキスト","講習会等内容","講習会内容","資料本文","本文テキスト"
]

def _get_field(rec: Dict[str, Any], keys: List[str]) -> str:
    for k in keys:
        v = rec.get(k)
        if v:
            return textify(v)
    return ""

def record_as_text(rec: Dict[str, Any], field: str) -> str:
    key_map = {
        "title": TITLE_KEYS,
        "text":  TEXT_KEYS,
        "author": ["author","Author","writer","posted_by","講師","著者","講師/著者"],
        "issue":  ["issue","Issue","会報号"],
        "date":   ["date","date_primary","Date","published_at","published","created_at","開催日/発行日","開催日","発行日","日付","作成日","更新日"],
        "category": ["category","Category","tags","Tags","資料区分","区分","カテゴリ","カテゴリー","タグ"],
        "url":    ["url","source","link","permalink","出典URL","URL","リンク","出典","公開URL"],
    }
    return _get_field(rec, key_map.get(field, [field]))

# ==================== クエリ解析 ====================
TOKEN_RE = re.compile(r'"([^"]+)"|(\S+)')  # "..." or non-space token

def _expand_term_forms(term: str) -> List[str]:
    term = normalize_text(term)
    forms = [term]
    h = to_hira(term); k = to_kata(h)
    for x in (h, k):
        if x and x not in forms: forms.append(x)
    for key in (term, h, k):
        for alt in SYNONYMS.get(key, []):
            alt_n = normalize_text(alt)
            if alt_n and alt_n not in forms: forms.append(alt_n)
            ah = to_hira(alt_n); ak = to_kata(ah)
            for x in (ah, ak):
                if x and x not in forms: forms.append(x)
    return forms

def parse_query(q: str) -> Tuple[List[List[str]], List[List[str]], List[str], List[str]]:
    base = normalize_text(q)
    if not base:
        return [], [], [], []

    pos_groups: List[List[str]] = []
    neg_groups: List[List[str]] = []
    phrases: List[str] = []
    hl_terms: List[str] = []

    for m in TOKEN_RE.finditer(base):
        token = m.group(1) if m.group(1) is not None else m.group(2)
        if not token: continue

        if m.group(1) is not None:
            phrase = normalize_text(token)
            phrases.append(phrase)
            for t in re.split(r"\s+", phrase.strip()):
                if t:
                    for f in _expand_term_forms(t):
                        if f not in hl_terms: hl_terms.append(f)
            continue

        is_neg = token.startswith("-")
        if is_neg:
            token = token[1:].strip()
            if not token: continue

        or_parts = [p for p in token.split("|") if p]
        group: List[str] = []
        for p in or_parts:
            for f in _expand_term_forms(p):
                if f not in group: group.append(f)

        if not group: continue

        if is_neg:
            neg_groups.append(group)
        else:
            pos_groups.append(group)
            for f in group:
                if f not in hl_terms: hl_terms.append(f)

    return pos_groups, neg_groups, phrases, hl_terms

# ==================== マッチ＆スコア ====================
FIELD_WEIGHTS = {"title":12,"text":8,"author":5,"issue":3,"date":2,"category":2,"url":1}
PHRASE_BONUS_TITLE = 100
PHRASE_BONUS_TEXT  = 60
PHRASE_BONUS_OTHER = 40

def _norm_pair(s: str) -> Tuple[str, str]:
    ns = normalize_text(s)
    return ns, to_hira(ns)

def _contains_all_terms(hay: str, terms: List[str]) -> bool:
    if not hay: return False
    a, ah = _norm_pair(hay)
    for t in terms:
        if not t: return False
        b, bh = _norm_pair(t)
        if b not in a and bh not in ah:
            return False
    return True

def _count_occurrences(needle: str, hay: str) -> int:
    if not needle or not hay: return 0
    if " " in needle.strip():
        parts = [p for p in needle.split(" ") if p]
        return 1 if _contains_all_terms(hay, parts) else 0
    a, ah = _norm_pair(hay)
    b, bh = _norm_pair(needle)
    return a.count(b) + ah.count(bh)

def _field_texts(rec: Dict[str, Any]) -> Dict[str, str]:
    out = {}
    for field in FIELD_WEIGHTS.keys():
        out[field] = record_as_text(rec, "date") if field == "date" else record_as_text(rec, field)
    return out

def _group_hit_in_any_field(rec: Dict[str, Any], group: List[str]) -> Tuple[bool, int]:
    hit = False; score_add = 0
    fields = _field_texts(rec)
    for field, w in FIELD_WEIGHTS.items():
        s = fields.get(field) or ""
        if not s: continue
        c = 0
        for t in group:
            c += _count_occurrences(t, s)
        if c > 0:
            hit = True
            score_add += w * c
    return hit, score_add

def _phrase_match_any_field(rec: Dict[str, Any], phrase: str) -> Tuple[bool, int]:
    base = normalize_text(phrase)
    variants = [base]
    if " " in base:
        variants.append(base.replace(" ", ""))
        variants.append(re.sub(r"(\d+)\s+(\S+)", r"\1\2", base))
    ok = False; add = 0
    fields = _field_texts(rec)
    for field, _w in FIELD_WEIGHTS.items():
        s = fields.get(field) or ""
        if not s: continue
        ns, nsh = _norm_pair(s)
        hit_here = False
        for v in variants:
            vb, vbh = _norm_pair(v)
            if vb in ns or vbh in nsh:
                hit_here = True
                break
        if hit_here:
            ok = True
            if field == "title":   add += PHRASE_BONUS_TITLE
            elif field == "text":  add += PHRASE_BONUS_TEXT
            else:                  add += PHRASE_BONUS_OTHER
    return ok, add

def compute_score(rec: Dict[str, Any],
                  pos_groups: List[List[str]],
                  neg_groups: List[List[str]],
                  phrases: List[str]) -> int:
    for ng in neg_groups:
        ng_hit, _ = _group_hit_in_any_field(rec, ng)
        if ng_hit:
            return -1

    total = 0
    for pg in pos_groups:
        pg_hit, add = _group_hit_in_any_field(rec, pg)
        if not pg_hit:
            return -1
        total += add

    for ph in phrases:
        ok, bonus = _phrase_match_any_field(rec, ph)
        if ok:
            total += bonus

    return total

# ==================== タイトル・フォールバック/表示整形 ====================
_RE_HAS_LETTER = re.compile(r"[A-Za-z0-9\u3040-\u30FF\u4E00-\u9FFF]")

def _is_meaningful_line(s: str) -> bool:
    if not s: return False
    t = _nfkc(s).strip()
    if not t: return False
    if not _RE_HAS_LETTER.search(t):
        return False
    return True

def _score_title_candidate(line: str, hl_terms: List[str]) -> int:
    t = _nfkc(line).strip()
    if len(t) < 6: return -1
    score = max(0, 120 - min(len(t), 120))
    for term in hl_terms:
        if not term: continue
        a, ah = _norm_pair(t)
        b, bh = _norm_pair(term)
        if b in a or bh in ah:
            score += 30
    return score

def _shorten_title(s: str, limit: int = 80) -> str:
    t = _nfkc(s).strip()
    return t if len(t) <= limit else (t[:limit] + "…")

def _extract_title_fallback(body: str, hl_terms: List[str]) -> Optional[str]:
    if not body:
        return None
    lines = body.splitlines()
    best = None
    best_score = -1
    for raw in lines:
        if not _is_meaningful_line(raw): 
            continue
        t = _nfkc(raw).strip()
        if len(t) < 6:
            continue
        sc = _score_title_candidate(t, hl_terms)
        if sc > best_score:
            best = t
            best_score = sc
        if sc >= 100:
            break
    if best:
        return _shorten_title(best, 80)
    for raw in lines:
        t = _nfkc(raw).strip()
        if t:
            return _shorten_title(t, 80)
    return None

TAG_RE = re.compile(r"<[^>]+>")

def html_escape(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def highlight_html(text: str, terms: List[str]) -> str:
    if not text: return ""
    esc = html_escape(text)
    for t in sorted(set(terms), key=len, reverse=True):
        et = html_escape(t)
        if not et: continue
        esc = re.sub(re.escape(et), lambda m: f"<mark>{m.group(0)}</mark>", esc)
    return esc

_SUSPICIOUS_SET = set("縺繧譁蜷螟雉邱驕鬮豌菴遘遞逕譬荳辟鬘譟")
_JP_RANGE = [(0x3040,0x309F),(0x30A0,0x30FF),(0x4E00,0x9FFF)]

def _ratio_suspicious(s: str) -> float:
    if not s: return 0.0
    n = sum(1 for ch in s if ch in _SUSPICIOUS_SET)
    return n / max(1, len(s))

def _ratio_japanese(s: str) -> float:
    if not s: return 0.0
    def is_jp(ch: str) -> bool:
        cp = ord(ch)
        return any(lo <= cp <= hi for lo,hi in _JP_RANGE)
    n = sum(1 for ch in s if is_jp(ch))
    return n / max(1, len(s))

def _try_repairs(s: str) -> str:
    cands = []
    try:
        cands.append((s.encode("cp932", errors="ignore").decode("utf-8", errors="ignore"), "cp932->utf8"))
    except Exception:
        pass
    try:
        b = s.encode("latin1", errors="ignore")
        cands.append((b.decode("cp932", errors="ignore"), "latin1->cp932"))
    except Exception:
        pass
    best = s; best_gain = 0.0
    base_jp = _ratio_japanese(s)
    for cand, _route in cands:
        if cand and cand != s:
            gain = _ratio_japanese(cand) - base_jp
            if gain > best_gain:
                best = cand
                best_gain = gain
    return best

def maybe_repair_for_display(s: str, kind: str) -> str:
    if not MOJIBAKE_REPAIR or not s:
        return s
    suspicious = _ratio_suspicious(s)
    if suspicious < 0.12:
        return s
    if kind == "title":
        _REPAIR_STATS["title_suspected"] += 1
    else:
        _REPAIR_STATS["snippet_suspected"] += 1
    repaired = _try_repairs(s)
    if repaired != s and _ratio_japanese(repaired) > _ratio_japanese(s):
        if kind == "title":
            _REPAIR_STATS["title_repaired"] += 1
        else:
            _REPAIR_STATS["snippet_repaired"] += 1
        return repaired
    return s

def make_head_snippet(body: str, terms: List[str], max_chars: int) -> str:
    if not body: return ""
    head = body[:max_chars]
    head = maybe_repair_for_display(head, "snippet")
    out = highlight_html(head, terms)
    if len(body) > max_chars: out += "…"
    return out

def make_hit_snippet(body: str, terms: List[str], max_chars: int, side: int = 80) -> str:
    if not body: return ""
    body = maybe_repair_for_display(body, "snippet")
    marked = highlight_html(body, terms)
    plain = TAG_RE.sub("", marked)
    if not plain: return ""
    m = re.search(r"<mark>", marked)
    if not m: return make_head_snippet(body, terms, max_chars)
    pm = TAG_RE.sub("", marked[:m.start()])
    pos = len(pm)
    start = max(0, pos - side); end = min(len(plain), pos + side)
    snippet_text = plain[start:end]
    if start > 0: snippet_text = "…" + snippet_text
    if end < len(plain): snippet_text = snippet_text + "…"
    snippet_html = highlight_html(snippet_text, terms)
    if len(TAG_RE.sub("", snippet_html)) > max_chars + 40:
        t = TAG_RE.sub("", snippet_html)[:max_chars] + "…"
        snippet_html = html_escape(t)
    return snippet_html

def build_item(rec: Dict[str, Any], hl_terms: List[str], is_first_in_page: bool) -> Dict[str, Any]:
    orig_title = record_as_text(rec, "title")
    body  = record_as_text(rec, "text")

    # (1) タイトルフォールバック（必要時）
    title_disp = (orig_title or "").strip()
    if not title_disp or title_disp == "(無題)":
        fb = _extract_title_fallback(body, hl_terms)
        if fb:
            title_disp = fb
        else:
            title_disp = orig_title or "(無題)"

    # (2) 文字化け修復＋ハイライト
    title_disp = maybe_repair_for_display(title_disp, "title")
    title_h = highlight_html(title_disp, hl_terms)

    snippet = (
        make_head_snippet(body, hl_terms, max_chars=300)
        if is_first_in_page else
        make_hit_snippet(body, hl_terms, max_chars=160, side=80)
    )
    return {
        "title": title_h,
        "content": snippet,
        "url": record_as_text(rec, "url"),
        "rank": None,
        "date": record_as_text(rec, "date"),
    }

# ==================== レコード反復 ====================
def iter_records():
    global _KB_ROWS
    if _KB_ROWS is not None:
        for rec in _KB_ROWS:
            yield rec
        return
    if not os.path.exists(KB_PATH): return
    with io.open(KB_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line: continue
            try:
                yield json.loads(line)
            except Exception:
                continue

# ==================== 共通レスポンスヘルパ ====================
def json_utf8(payload: Dict[str, Any], status: int = 200) -> JSONResponse:
    return JSONResponse(
        payload,
        status_code=status,
        media_type="application/json; charset=utf-8",
        headers={"Cache-Control": "no-store", "Content-Type": "application/json; charset=utf-8"},
    )

# ==================== エンドポイント ====================
@app.get("/health")
def health():
    ok = os.path.exists(KB_PATH) and KB_LINES > 0
    return json_utf8({"ok": ok, "kb_url": KB_URL, "kb_size": KB_LINES, "kb_fingerprint": KB_HASH})

@app.get("/version")
def version():
    return json_utf8({"version": VERSION})

@app.get("/diag")
def diag(q: str = Query("", description="クエリ解析＆年尾解釈の確認用")):
    base_q, y, yr = _parse_year_from_query(q)
    return json_utf8({
        "kb": {"path": KB_PATH, "exists": os.path.exists(KB_PATH), "lines": KB_LINES, "sha256": KB_HASH, "url": KB_URL},
        "env": {"APP_VERSION": VERSION, "cwd": os.getcwd(), "MOJIBAKE_REPAIR": MOJIBAKE_REPAIR},
        "last": {"event": LAST_EVENT, "error": LAST_ERROR},
        "repair_stats": _REPAIR_STATS,
        "query_parse": {"raw": q, "base_q": base_q, "year": y, "year_range": yr},
        "ui": {"static_ui_html": os.path.exists(os.path.join("static", "ui.html"))},
    })

@app.get("/ui")
def ui():
    path = os.path.join("static", "ui.html")
    if os.path.exists(path):
        return FileResponse(path, media_type="text/html; charset=utf-8")
    return PlainTextResponse("static/ui.html not found", status_code=404)

@app.get("/admin/refresh")
def admin_refresh():
    lines, sha = _refresh_kb_globals()
    return json_utf8({
        "ok": lines > 0, "kb_size": lines, "kb_fingerprint": sha,
        "last_event": LAST_EVENT, "last_error": LAST_ERROR,
        "repair_stats": _REPAIR_STATS,
    })

# ---- 並び順キー（決定化）----
def _sort_key_relevance(entry: Tuple[int, Optional[datetime], Dict[str, Any]]) -> Tuple:
    score, d, rec = entry
    date_key = d or datetime.min
    url_c = canonical_url(record_as_text(rec, "url"))
    title_n = normalize_text(record_as_text(rec, "title"))
    # score↓, date↓, url↑, title↑
    return (-int(score), -int(date_key.strftime("%Y%m%d%H%M%S")), url_c, title_n)

def _sort_key_latest(entry: Tuple[int, Optional[datetime], Dict[str, Any]]) -> Tuple:
    score, d, rec = entry
    date_key = d or datetime.min
    url_c = canonical_url(record_as_text(rec, "url"))
    title_n = normalize_text(record_as_text(rec, "title"))
    # date↓, score↓, url↑, title↑
    return (-int(date_key.strftime("%Y%m%d%H%M%S")), -int(score), url_c, title_n)

@app.get("/api/search")
def api_search(
    q: str = Query("", description="検索クエリ（末尾年/年範囲でフィルタ可：例『コンテスト2024』『剪定 1999〜2001』）"),
    page: int = Query(1, ge=1),
    page_size: int = Query(5, ge=1, le=50),
    order: str = Query("relevance", pattern="^(relevance|latest)$"),
    refresh: int = Query(0, description="1=kb.jsonl を再取得・再読み込み"),
):
    try:
        if refresh == 1:
            _refresh_kb_globals()

        if not os.path.exists(KB_PATH) or KB_LINES <= 0:
            return json_utf8({"items": [], "total_hits": 0, "page": page, "page_size": page_size,
                              "has_more": False, "next_page": None, "error": "kb_missing", "order_used": order})

        # 末尾の年/範囲のみ年フィルタ
        base_q, year_tail, yr_tail = _parse_year_from_query(q)
        q_used = base_q

        pos_groups, neg_groups, phrases, hl_terms = parse_query(q_used)
        if not pos_groups and not neg_groups and not phrases:
            return json_utf8({"items": [], "total_hits": 0, "page": page, "page_size": page_size,
                              "has_more": False, "next_page": None, "error": None, "order_used": order})

        # ★★★ ベストヒット集約（id or (title_norm, url_canon)）＋最良スコア採用
        best: Dict[Any, Tuple[int, Optional[datetime], Dict[str, Any]]] = {}

        for rec in iter_records():
            if not _matches_year(rec, year_tail, yr_tail):
                continue
            score = compute_score(rec, pos_groups, neg_groups, phrases)
            if score < 0:
                continue
            d = record_date(rec)

            # キー生成（厳密化）
            rid = rec.get("id")
            title_n = normalize_text(record_as_text(rec, "title"))
            url_c   = canonical_url(record_as_text(rec, "url"))
            key = rid or (title_n, url_c)

            prev = best.get(key)
            if (prev is None) or (score > prev[0]) or (score == prev[0] and (d or datetime.min) > (prev[1] or datetime.min)):
                best[key] = (score, d, rec)

        # 集約 → 配列化
        hits: List[Tuple[int, Optional[datetime], Dict[str, Any]]] = list(best.values())
        total_hits = len(hits)

        # 決定的ソート（ページング前に一度だけ）
        if order == "latest":
            hits.sort(key=_sort_key_latest)
            order_used = "latest"
        else:
            hits.sort(key=_sort_key_relevance)
            order_used = "relevance"

        # ページング
        start = (page - 1) * page_size
        end = start + page_size
        page_hits = hits[start:end]
        has_more = end < total_hits
        next_page = page + 1 if has_more else None

        # 表示用アイテム化
        items: List[Dict[str, Any]] = []
        for i, (_, _d, rec) in enumerate(page_hits):
            items.append(build_item(rec, hl_terms, is_first_in_page=(i == 0)))

        # ランク付け（1始まり）
        for idx, _ in enumerate(hits, start=1):
            if start < idx <= end:
                items[idx - start - 1]["rank"] = idx

        return json_utf8({"items": items, "total_hits": total_hits, "page": page, "page_size": page_size,
                          "has_more": has_more, "next_page": next_page, "error": None, "order_used": order_used})

    except Exception as e:
        return json_utf8({"items": [], "total_hits": 0, "page": 1, "page_size": page_size,
                          "has_more": False, "next_page": None, "error": "exception", "message": textify(e)})

# ==================== ローカル実行 ====================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
