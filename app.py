# app.py — ミニバラ盆栽愛好会 デジタル資料館（JSONL版・文字化け対策）
# 変更点（本パッチ）:
# - すべてのJSON応答に charset=utf-8 を明示（誤デコード防止）
# - 表示用フィールド（title / snippet）に限定してモジバケ自動修復（環境変数 MOJIBAKE_REPAIR=0 で無効化）
# - /diag に修復件数を表示
#
# 検索仕様は従前のまま（AND/OR/NOT, フレーズ, 年末尾フィルタ等）。仕様変更は別途パッチとします。

import os, io, re, json, hashlib, unicodedata
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional

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
VERSION  = os.getenv("APP_VERSION", "jsonl-2025-10-19-mojibake-fix1")

MOJIBAKE_REPAIR = (os.getenv("MOJIBAKE_REPAIR", "1") != "0")  # 既定 ON

# ==================== 診断用 ====================
KB_LINES: int = 0
KB_HASH:  str = ""
LAST_ERROR: str = ""
LAST_EVENT: str = ""

# 修復診断カウンタ
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

# ==================== 同義語（最小） ====================
SYNONYMS: Dict[str, List[str]] = {
    "苔": ["コケ", "こけ"],
    "コケ": ["苔", "こけ"],
    "こけ": ["苔", "コケ"],
}

# ==================== KB 取得・常駐 ====================
def _bytes_to_jsonl(blob: bytes) -> bytes:
    """配列JSONでも来たら JSONL に変換して保存"""
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
    """KBを取得→保存→行数/ハッシュ計算→メモリ常駐。戻り値=(lines, sha256)。"""
    global LAST_ERROR, LAST_EVENT, _KB_ROWS
    LAST_ERROR = ""; LAST_EVENT = ""
    # 取得（任意）
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
    # 行数/ハッシュ
    if os.path.exists(KB_PATH):
        try:
            lines, sha = _compute_lines_and_hash(KB_PATH)
            if lines <= 0:
                LAST_EVENT = LAST_EVENT or "empty_file"
            # メモリ常駐
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
    """ensure_kb の結果をグローバル診断値に反映"""
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

# ==================== 日付/年 抽出（従前どおり） ====================
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
    for k in ("date", "date_primary", "Date", "published_at", "published", "created_at"):
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
    """末尾に 2024 / 2023-2025 などがあれば切り出す（従前通り。今は仕様変更せず）"""
    q = _nfkc(q_raw).strip()
    if not q: return "", None, None
    parts = q.replace("　"," ").split()
    last = parts[-1] if parts else ""
    if re.fullmatch(r"(19|20|21)\d{2}", last):
        return (" ".join(parts[:-1]).strip(), int(last), None)
    m = re.fullmatch(rf"((?:19|20|21)\d{{2}})\s*{RANGE_SEP}\s*((?:19|20|21)\d{{2}})", last)
    if m:
        y1, y2 = int(m.group(1)), int(m.group(2))
        if y1 > y2: y1, y2 = y2, y1
        return (" ".join(parts[:-1]).strip(), None, (y1, y2))
    return (q, None, None)

def _matches_year(rec: Dict[str, Any], year: Optional[int], yr: Optional[Tuple[int,int]]) -> bool:
    if year is None and yr is None: return True
    ys = _record_years(rec)
    if not ys: return False
    if year is not None: return year in ys
    lo, hi = yr[0], yr[1]
    return any(lo <= y <= hi for y in ys)

# ==================== フィールド抽出 ====================
TITLE_KEYS = ["title", "Title", "name", "Name", "page_title", "source_title", "heading", "headline", "subject"]
TEXT_KEYS  = ["text", "content", "body", "description", "summary", "note", "content_full", "excerpt"]

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
        "author": ["author", "Author", "writer", "posted_by"],
        "issue":  ["issue", "Issue"],
        "date":   ["date", "date_primary", "Date", "published_at", "published", "created_at"],
        "category": ["category", "Category", "tags", "Tags"],
        "url":    ["url", "source", "link", "permalink"],
    }
    return _get_field(rec, key_map.get(field, [field]))

# ==================== クエリ解析（従前どおり） ====================
TOKEN_RE = re.compile(r'"([^"]+)"|(\S+)')  # "..." or non-space token
SUFFIX_SPLIT = ("結果","一覧","発表","報告","ギャラリー")

def _space_variants(term: str) -> List[str]:
    """例: 'コンテスト結果'→'コンテスト 結果' / 'コンテスト2024'→'コンテスト 2024'（従前仕様）"""
    t = _nfkc(term)
    outs: List[str] = []
    m = re.match(r"^(.*?)(\d{2,})$", t)
    if m and m.group(1) and m.group(2):
        outs.append((m.group(1).strip() + " " + m.group(2)).strip())
    m2 = re.match(r"^([ァ-ンヴーぁ-んー]+)([一-龥]+)$", t)
    if m2:
        outs.append(m2.group(1) + " " + m2.group(2))
    m3 = re.match(r"^([一-龥]+)([ァ-ンヴーぁ-んー]+)$", t)
    if m3:
        outs.append(m3.group(1) + " " + m3.group(2))
    for suf in SUFFIX_SPLIT:
        if t.endswith(suf) and len(t) > len(suf):
            outs.append(t[:-len(suf)].strip() + " " + suf)
            break
    return [o for o in outs if o and o != t]

def _expand_term_forms(term: str) -> List[str]:
    """表記ゆれ（かな/カナ/同義）＋ 空白あり/なしのバリアント（従前仕様）"""
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
    more: List[str] = []
    for f in list(forms):
        more += _space_variants(f)
    for m in more:
        if m not in forms: forms.append(m)
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

# ==================== マッチ＆スコア（従前どおり） ====================
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
        ok, _ = _group_hit_in_any_field(rec, ng)
        if ok: return -1
    total = 0
    for g in pos_groups:
        ok, add = _group_hit_in_any_field(rec, g)
        if not ok: return -1
        total += add
    if phrases:
        for p in phrases:
            ok, add = _phrase_match_any_field(rec, p)
            if not ok: return -1
            total += add
    return total

# ==================== 抜粋/ハイライト + モジバケ修復 ====================
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

# --- モジバケ検知・修復（表示専用） ---
_SUSPICIOUS_SET = set("縺繧譁蜷螟雉邱驕鬮豌菴遘遞逕譬荳辟鬘譟")
_JP_RANGE = [
    (0x3040, 0x309F),  # ひらがな
    (0x30A0, 0x30FF),  # カタカナ
    (0x4E00, 0x9FFF),  # CJK
]

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
    """
    代表的なモジバケからの復元を2通り試す。
    成功判定: 日本語比率の改善、および文字列の変化。
    """
    cands = []
    try:
        cands.append((s.encode("cp932", errors="ignore").decode("utf-8", errors="ignore"), "cp932->utf8"))
    except Exception:
        pass
    try:
        # latin1 バイト経由で cp932 デコード（環境により有効な場合あり）
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
                best_gain = gain
                best = cand
    return best

def maybe_repair_for_display(s: str, kind: str) -> str:
    """
    表示専用の自動修復。元文字列は保持。
    kind: "title" or "snippet"（統計用）
    """
    if not MOJIBAKE_REPAIR or not s:
        return s
    suspicious = _ratio_suspicious(s)
    if suspicious < 0.12:  # しきい値（経験則）
        return s
    # 診断カウント（疑い）
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
    title = record_as_text(rec, "title") or "(無題)"
    body  = record_as_text(rec, "text")
    # 表示前にモジバケ修復 → ハイライト
    title_disp = maybe_repair_for_display(title, "title")
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
def diag(q: str = Query("", description="クエリ（年尾解析の確認用）")):
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

@app.get("/api/search")
def api_search(
    q: str = Query("", description="検索クエリ（末尾に年/年範囲も可：例『コンテスト 2024』『剪定 1999〜2001』）"),
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

        # 末尾の年/範囲（従前仕様）
        base_q, year_tail, yr_tail = _parse_year_from_query(q)
        q_used = base_q

        pos_groups, neg_groups, phrases, hl_terms = parse_query(q_used)
        if not pos_groups and not neg_groups and not phrases:
            return json_utf8({"items": [], "total_hits": 0, "page": page, "page_size": page_size,
                              "has_more": False, "next_page": None, "error": None, "order_used": order})

        hits: List[Tuple[int, Optional[datetime], Dict[str, Any]]] = []
        for rec in iter_records():
            if not _matches_year(rec, year_tail, yr_tail):
                continue
            score = compute_score(rec, pos_groups, neg_groups, phrases)
            if score < 0:
                continue
            d = record_date(rec)
            hits.append((score, d, rec))

        total_hits = len(hits)

        if order == "latest":
            hits.sort(key=lambda x: (x[1] or datetime.min), reverse=True)
            order_used = "latest"
        else:
            hits.sort(key=lambda x: (x[0], x[1] or datetime.min), reverse=True)
            order_used = "relevance"

        start = (page - 1) * page_size
        end = start + page_size
        page_hits = hits[start:end]
        has_more = end < total_hits
        next_page = page + 1 if has_more else None

        items: List[Dict[str, Any]] = []
        for i, (_, _d, rec) in enumerate(page_hits):
            items.append(build_item(rec, hl_terms, is_first_in_page=(i == 0)))

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
