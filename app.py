# app.py — Mini Rose Search (JSONL)
# 仕様: 日本語の多層ゆれ吸収（NFKC/かなフォールディング/軽量ファジー）+ 年フィルタ + フレーズ
# 空白=AND / '|'=OR / '-語'=NOT / "..."=フレーズ（完全一致）/ 「空白あり」と「空白なし」をスコアで揃える
# 1件目=冒頭~300字、2件目以降=ヒット周辺~160字
# 例外時もHTTP200+JSONで返却
# VERSION: jsonl-2025-10-19-stable-fuzzy

import os, io, re, json, hashlib, unicodedata
from datetime import datetime, date
from typing import List, Dict, Any, Tuple, Optional

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, FileResponse, PlainTextResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

try:
    import requests
except Exception:
    requests = None

# ==================== アプリ初期化 ====================
APP_VERSION = os.getenv("APP_VERSION", "jsonl-2025-10-19-stable-fuzzy")
app = FastAPI(title="mini-rose-search-jsonl (kb.jsonl)", version=APP_VERSION)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)

if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

# ==================== 設定 ====================
KB_URL  = os.getenv("KB_URL", "").strip()
KB_PATH = os.getenv("KB_PATH", "kb.jsonl").strip() or "kb.jsonl"   # 既定: リポ直下の kb.jsonl（歴史互換）

JSON_HEADERS = {"content-type": "application/json; charset=utf-8"}

# ==================== ユーティリティ ====================
def textify(x: Any) -> str:
    if x is None: return ""
    if isinstance(x, str): return x
    try:
        return str(x)
    except Exception:
        return ""

def normalize_text(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("\u3000", " ")
    s = re.sub(r"[\r\n\t]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

# ---------- かなフォールディング（完成版） ----------
_SMALL_TO_BASE = str.maketrans({
    "ぁ":"あ","ぃ":"い","ぅ":"う","ぇ":"え","ぉ":"お",
    "ゃ":"や","ゅ":"ゆ","ょ":"よ","ゎ":"わ","っ":"つ",
    "ゕ":"か","ゖ":"け"
})
_A_SET = set("あかさたなはまやらわがざだばぱぁゃゎっ")
_I_SET = set("いきしちにひみりぎじぢびぴぃ")
_U_SET = set("うくすつぬふむゆるぐずづぶぷぅゅっ")
_E_SET = set("えけせてねへめれげぜでべぺぇ")
_O_SET = set("おこそとのほもよろをごぞどぼぽぉょ")

def _kana_to_hira_all(s: str) -> str:
    out = []
    for ch in s:
        o = ord(ch)
        if 0x30A1 <= o <= 0x30FA:  # カタカナ→ひらがな
            out.append(chr(o - 0x60))
        elif ch in ("ヵ", "ヶ"):
            out.append({"ヵ": "か", "ヶ": "け"}[ch])
        else:
            out.append(ch)
    return "".join(out)

def _long_to_vowel(prev: str) -> str:
    if not prev: return ""
    if prev in _A_SET: return "あ"
    if prev in _I_SET: return "い"
    if prev in _U_SET: return "う"
    if prev in _E_SET: return "え"
    if prev in _O_SET: return "お"
    return ""

def fold_kana(s: str) -> str:
    """NFKC→ひらがな→小書き正規化→長音→母音→濁点除去→lower"""
    if not s: return ""
    s = unicodedata.normalize("NFKC", s)
    s = _kana_to_hira_all(s)
    s = s.translate(_SMALL_TO_BASE)
    buf = []
    for ch in s:
        if ch == "ー":
            buf.append(_long_to_vowel(buf[-1] if buf else ""))
        else:
            buf.append(ch)
    s = "".join(buf)
    d = unicodedata.normalize("NFD", s)
    d = "".join(c for c in d if ord(c) not in (0x3099, 0x309A))  # 濁点/半濁点を除去
    s = unicodedata.normalize("NFC", d)
    return s.lower().strip()

def hira_to_kata(s: str) -> str:
    out=[]
    for ch in s:
        o=ord(ch)
        if 0x3041 <= o <= 0x3096:
            out.append(chr(o + 0x60))
        elif ch in ("ゕ","ゖ"):
            out.append({"ゕ":"ヵ","ゖ":"ヶ"}[ch])
        else:
            out.append(ch)
    return "".join(out)

# ---------- 軽量ファジー（編集距離 ≤1） ----------
def levenshtein_le1(a: str, b: str) -> bool:
    if a == b: return True
    la, lb = len(a), len(b)
    if abs(la - lb) > 1: return False
    i = j = diff = 0
    while i < la and j < lb:
        if a[i] == b[j]:
            i += 1; j += 1
        else:
            diff += 1
            if diff > 1: return False
            if la == lb: i += 1; j += 1
            elif la > lb: i += 1
            else: j += 1
    diff += (la - i) + (lb - j)
    return diff <= 1

def fuzzy_substring_match(term: str, text: str) -> bool:
    """fold_kana 同士で編集距離≤1。語長<2は誤爆が多いので除外。"""
    if not term or not text: return False
    t = fold_kana(term)
    x = fold_kana(text)
    if len(t) < 2: return False
    L = len(t)
    for w in (L-1, L, L+1):
        if w < 2: continue
        for i in range(0, max(0, len(x)-w+1)):
            if levenshtein_le1(t, x[i:i+w]):
                return True
    return False

# ==================== KBの取得/検証 ====================
def ensure_kb() -> Tuple[int, str, str]:
    """
    KB_PATH に kb.jsonl が無ければ KB_URL から取得（あれば）。
    戻り値: (line_count, sha256, path)
    """
    if KB_URL and KB_URL.startswith("http"):
        if requests is None:
            raise RuntimeError("requests が利用できません（requirements.txt に requests を追加）")
        try:
            r = requests.get(KB_URL, timeout=30)
            r.raise_for_status()
            with open(KB_PATH, "wb") as f:
                f.write(r.content)
            last_event = "fetched"
        except Exception:
            last_event = "fetch_failed"
    else:
        last_event = "skipped"

    if not os.path.exists(KB_PATH):
        return 0, "", os.path.abspath(KB_PATH)

    line_count = 0
    sha = hashlib.sha256()
    with open(KB_PATH, "rb") as f:
        for line in f:
            sha.update(line)
            line_count += 1
    return line_count, sha.hexdigest(), os.path.abspath(KB_PATH)

KB_LINES: int = 0
KB_HASH: str = ""
KB_ABS: str = ""

@app.on_event("startup")
def _startup():
    global KB_LINES, KB_HASH, KB_ABS
    try:
        KB_LINES, KB_HASH, KB_ABS = ensure_kb()
    except Exception:
        KB_LINES, KB_HASH, KB_ABS = 0, "", os.path.abspath(KB_PATH)

# ==================== 日付/年 ユーティリティ ====================
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
        v = rec.get(k)
        if v:
            dt = parse_date_str(textify(v))
            if dt: return dt
    return None

def _years_from_text(s: str) -> List[int]:
    if not s: return []
    s = normalize_text(s)
    ys = [int(y) for y in re.findall(r"\b(19\d{2}|20\d{2}|21\d{2})\b", s)]
    return list(sorted(set(ys)))

def record_years(rec: Dict[str, Any]) -> List[int]:
    ys = set()
    d = record_date(rec)
    if d: ys.add(d.year)
    for fld in ("issue", "title", "text", "url"):
        ys.update(_years_from_text(textify(rec.get(fld, ""))))
    return sorted(ys)

_RANGE_SEP = r"(?:-|–|—|~|〜|～|\.{2})"

def parse_year_tail(q_raw: str) -> Tuple[str, Optional[int], Optional[Tuple[int,int]]]:
    q = normalize_text(q_raw)
    if not q: return "", None, None
    parts = q.split(" ")
    last = parts[-1] if parts else ""
    m1 = re.fullmatch(r"(?:19|20|21)\d{2}", last)
    if m1:
        base = " ".join(parts[:-1]).strip()
        return (base, int(last), None)
    m2 = re.fullmatch(rf"((?:19|20|21)\d{2})\s*{_RANGE_SEP}\s*((?:19|20|21)\d{2})", last)
    if m2:
        a, b = int(m2.group(1)), int(m2.group(2))
        if a > b: a, b = b, a
        base = " ".join(parts[:-1]).strip()
        return (base, None, (a, b))
    return (q, None, None)

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
        "issue":  ["issue", "Issue", "会報号", "出典"],
        "date":   ["date", "date_primary", "Date", "published_at", "published", "created_at", "開催日/発行日", "開催日／発行日", "開催日・発行日"],
        "category": ["category", "Category", "資料区分", "tags", "Tags"],
        "url":    ["url", "source", "link", "permalink", "出典URL", "外部URL", "URL", "リンク", "Link"],
    }
    return _get_field(rec, key_map.get(field, [field]))

# ==================== 同義語（最小辞書） ====================
SYNONYMS: Dict[str, List[str]] = {
    "苔":   ["コケ", "こけ", "ゴケ", "ごけ"],
    "接ぎ木": ["つぎ木", "つぎき", "接木"],
    "挿し木": ["さし木", "さし芽"],
    "うどんこ病": ["ウドンコ"],
    "黒星病": ["クロボシ", "黒点病"],
    "用土": ["土", "土の配合"],
}

# ==================== クエリ解析 ====================
TOKEN_RE = re.compile(r'"([^"]+)"|(\S+)')  # "..." or non-space token

def _expand_term_forms(term: str) -> List[str]:
    """表記ゆれ吸収（NFKC→かな折りたたみ。+簡易同義語）。入力語は分割しない。"""
    base = normalize_text(term)
    forms = [base]
    fk = fold_kana(base)
    if fk and fk not in forms:
        forms.append(fk)
    # 同義語
    for key in (base, fk, hira_to_kata(fk)):
        for alt in SYNONYMS.get(key, []):
            alt_n = normalize_text(alt)
            if alt_n and alt_n not in forms:
                forms.append(alt_n)
            fk2 = fold_kana(alt_n)
            if fk2 and fk2 not in forms:
                forms.append(fk2)
    return forms

def parse_query(q: str) -> Tuple[List[List[str]], List[List[str]], List[str], List[str], List[str]]:
    """
    戻り値:
      pos_groups: ANDの各グループ（要素は OR リスト：複数表記ゆれ）
      neg_groups: 除外（NOT）の各グループ
      phrases:    フレーズ（"..."）のリスト（必須条件）
      hl_terms:   ハイライト用語（入力で有効になった語）
      soft_phrases: 空白を除いた連結候補（スコアボーナス用、必須ではない）
    """
    base = normalize_text(q)
    if not base:
        return [], [], [], [], []

    pos_groups: List[List[str]] = []
    neg_groups: List[List[str]] = []
    phrases: List[str] = []
    hl_terms: List[str] = []
    raw_tokens: List[str] = []

    for m in TOKEN_RE.finditer(base):
        token = m.group(1) if m.group(1) is not None else m.group(2)
        if not token: continue

        if m.group(1) is not None:
            phrases.append(token)
            raw_tokens.append(token)
            for t in re.split(r"\s+", token.strip()):
                if t:
                    for f in _expand_term_forms(t):
                        if f not in hl_terms:
                            hl_terms.append(f)
            continue

        is_neg = token.startswith("-")
        if is_neg:
            token = token[1:].strip()
            if not token:
                continue

        or_parts = [p for p in token.split("|") if p]
        group: List[str] = []
        for p in or_parts:
            ex = _expand_term_forms(p)
            for f in ex:
                if f not in group:
                    group.append(f)

        if not group:
            continue

        if is_neg:
            neg_groups.append(group)
        else:
            pos_groups.append(group)
            raw_tokens.append(token)
            for f in group:
                if f not in hl_terms:
                    hl_terms.append(f)

    # 空白あり ⇔ 空白なし の“揃え”（スコアボーナス用のソフトフレーズ）
    tokens_plain = [t for t in raw_tokens if not t.startswith('"') and not t.endswith('"') and not t.startswith("-")]
    soft_phrases: List[str] = []
    if len(tokens_plain) >= 2:
        joined_all = "".join(tokens_plain)
        if joined_all and joined_all not in soft_phrases:
            soft_phrases.append(joined_all)
        # 連結の部分列（先頭2語のみ）も候補に
        pair = "".join(tokens_plain[:2])
        if pair and pair not in soft_phrases:
            soft_phrases.append(pair)

    return pos_groups, neg_groups, phrases, hl_terms, soft_phrases

# ==================== マッチ＆スコア ====================
FIELD_WEIGHTS = {
    "title":   12,
    "text":     8,
    "author":   5,
    "issue":    3,
    "date":     2,
    "category": 2,
}

PHRASE_BONUS_TITLE = 100
PHRASE_BONUS_TEXT  = 60
SOFT_PHRASE_BONUS_TITLE = 24
SOFT_PHRASE_BONUS_TEXT  = 16

def _count_occurrences(needle: str, hay: str) -> int:
    """通常/かな折りたたみの双方で部分一致回数を数える。"""
    if not needle or not hay: return 0
    a = normalize_text(hay)
    ah = fold_kana(a)
    b = normalize_text(needle)
    bh = fold_kana(b)
    return a.count(b) + ah.count(bh)

def _contains_phrase(hay: str, phrase: str) -> bool:
    if not hay or not phrase: return False
    a = normalize_text(hay)
    ah = fold_kana(a)
    p = normalize_text(phrase)
    ph = fold_kana(p)
    p = re.sub(r"\s+", " ", p)
    ph = re.sub(r"\s+", " ", ph)
    return (p in a) or (ph in ah)

def _group_hit_in_any_field(rec: Dict[str, Any], group: List[str]) -> Tuple[bool, int]:
    hit = False
    score_add = 0
    for field, w in FIELD_WEIGHTS.items():
        s = record_as_text(rec, "date") if field == "date" else record_as_text(rec, field)
        if not s:
            continue
        c = 0
        for t in group:
            c += _count_occurrences(t, s)
        if c > 0:
            hit = True
            score_add += w * c
        else:
            # 完全不一致なら軽量ファジー（1語でも合えば加点）
            for t in group:
                if fuzzy_substring_match(t, s):
                    hit = True
                    score_add += max(1, int(round(w * 0.6)))
                    break
    return hit, score_add

def compute_score(rec: Dict[str, Any],
                  pos_groups: List[List[str]],
                  neg_groups: List[List[str]],
                  phrases: List[str],
                  soft_phrases: List[str]) -> int:
    # NOT：どれかに当たったら除外
    for ng in neg_groups:
        ok, _ = _group_hit_in_any_field(rec, ng)
        if ok:
            return -1

    # AND：全グループで少なくとも1表記ヒット必須
    total = 0
    for g in pos_groups:
        ok, add = _group_hit_in_any_field(rec, g)
        if not ok:
            return -1
        total += add

    # フレーズ（必須条件）
    if phrases:
        title = record_as_text(rec, "title")
        text  = record_as_text(rec, "text")
        for p in phrases:
            in_title = _contains_phrase(title, p)
            in_text  = _contains_phrase(text, p)
            if not (in_title or in_text):
                return -1
            if in_title: total += PHRASE_BONUS_TITLE
            if in_text:  total += PHRASE_BONUS_TEXT

    # 空白⇔連結の“ソフト”フレーズはボーナスのみ（除外条件にしない）
    if soft_phrases:
        title = record_as_text(rec, "title")
        text  = record_as_text(rec, "text")
        for p in soft_phrases:
            if _contains_phrase(title, p):
                total += SOFT_PHRASE_BONUS_TITLE
            if _contains_phrase(text, p):
                total += SOFT_PHRASE_BONUS_TEXT

    return total

# ==================== 抜粋・ハイライト ====================
TAG_RE = re.compile(r"<[^>]+>")

def html_escape(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def highlight(text: str, terms: List[str]) -> str:
    if not text:
        return ""
    esc = html_escape(text)
    # 長い順に
    for t in sorted(set(terms), key=len, reverse=True):
        et = html_escape(t)
        if not et: continue
        try:
            esc = re.sub(re.escape(et), lambda m: f"<mark>{m.group(0)}</mark>", esc)
        except re.error:
            pass
    return esc

def make_head_snippet(body: str, terms: List[str], max_chars: int) -> str:
    if not body:
        return ""
    head = body[:max_chars]
    out = highlight(head, terms)
    if len(body) > max_chars:
        out += "…"
    return out

def make_hit_snippet(body: str, terms: List[str], max_chars: int, side: int = 80) -> str:
    if not body:
        return ""
    marked = highlight(body, terms)
    plain = TAG_RE.sub("", marked)
    if not plain:
        return ""
    m = re.search(r"<mark>", marked)
    if not m:
        return make_head_snippet(body, terms, max_chars)
    pm = TAG_RE.sub("", marked[:m.start()])
    pos = len(pm)
    start = max(0, pos - side)
    end = min(len(plain), pos + side)
    snippet_text = plain[start:end]
    if start > 0:
        snippet_text = "…" + snippet_text
    if end < len(plain):
        snippet_text = snippet_text + "…"
    snippet_html = highlight(snippet_text, terms)
    if len(TAG_RE.sub("", snippet_html)) > max_chars + 40:
        t = TAG_RE.sub("", snippet_html)[:max_chars] + "…"
        snippet_html = html_escape(t)
    return snippet_html

def build_item(rec: Dict[str, Any], hl_terms: List[str], is_first_in_page: bool) -> Dict[str, Any]:
    body = record_as_text(rec, "text")
    snippet = (
        make_head_snippet(body, hl_terms, max_chars=300)
        if is_first_in_page else
        make_hit_snippet(body, hl_terms, max_chars=160, side=80)
    )
    return {
        "title": record_as_text(rec, "title") or "(無題)",
        "content": snippet,
        "url": record_as_text(rec, "url"),
        "rank": None,
        "date": record_as_text(rec, "date"),
    }

# ==================== 入力→年フィルタ ====================
def matches_year(rec: Dict[str, Any], year: Optional[int], y_from: Optional[int], y_to: Optional[int]) -> bool:
    if year is None and y_from is None and y_to is None:
        return True
    ys = record_years(rec)
    if not ys:
        return False
    if year is not None:
        return year in ys
    lo = y_from if y_from is not None else -10**9
    hi = y_to   if y_to   is not None else  10**9
    return any(lo <= y <= hi for y in ys)

# ==================== エンドポイント ====================
@app.get("/health")
def health():
    ok = os.path.exists(KB_PATH)
    return JSONResponse(
        {"ok": ok, "kb_url": KB_URL, "kb_size": KB_LINES, "kb_fingerprint": KB_HASH},
        headers=JSON_HEADERS
    )

@app.get("/version")
def version():
    return JSONResponse({"version": app.version}, headers=JSON_HEADERS)

@app.get("/diag")
def diag(q: str = Query("", description="動作確認（年尾パース・エンコード警告）")):
    base_q, y, yr = parse_year_tail(q)
    y_from, y_to = (yr or (None, None))
    # もじばけ検知（繧/縺/蜷 などが一定割合以上）
    def mojibake_score(s: str) -> float:
        if not s: return 0.0
        bad = sum(ch in "繧縺蜷鬘辟" for ch in s)
        return bad / max(1, len(s))
    warn = mojibake_score(q) > 0.05
    return JSONResponse({
        "kb": {"path": KB_ABS or KB_PATH, "exists": os.path.exists(KB_PATH), "lines": KB_LINES, "sha256": KB_HASH, "url": KB_URL},
        "env": {"APP_VERSION": app.version, "cwd": os.getcwd()},
        "last": {"event": "fetched" if KB_LINES else "missing", "error": ""},
        "query_parse": {"raw": q, "base_q": base_q, "year": y, "year_range": yr},
        "ui": {"static_ui_html": os.path.exists(os.path.join("static", "ui.html"))},
        "encoding_warning": warn
    }, headers=JSON_HEADERS)

@app.get("/ui", response_class=HTMLResponse)
def ui():
    path = os.path.join("static", "ui.html")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            html = f.read()
        return HTMLResponse(html, headers={"Cache-Control": "no-store"})
    return HTMLResponse("<h1>static/ui.html not found</h1>", status_code=404, headers={"Cache-Control": "no-store"})

def iter_records():
    with io.open(KB_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue

@app.get("/api/search")
def api_search(
    q: str = Query("", description="検索クエリ"),
    page: int = Query(1, ge=1),
    page_size: int = Query(5, ge=1, le=50),
    order: str = Query("relevance", pattern="^(relevance|latest)$"),
    year: Optional[int] = Query(None, description="年（4桁）"),
    year_from: Optional[int] = Query(None, description="開始年（4桁）"),
    year_to: Optional[int] = Query(None, description="終了年（4桁）"),
):
    """
    - 空白=AND, '|'=OR, '-語'=NOT, "..."=フレーズ（必須条件）
    - 空白あり/なしはスコアで揃える（ソフトフレーズボーナス）
    - relevance: スコア順（同点は新しい日付優先） / latest: 代表日降順
    """
    try:
        if not os.path.exists(KB_PATH):
            return JSONResponse(
                {"items": [], "total_hits": 0, "page": page, "page_size": page_size,
                 "has_more": False, "next_page": None, "error": "kb_missing", "order_used": order},
                headers=JSON_HEADERS
            )

        # 末尾の年/範囲を自動解釈（引数優先）
        base_q, y_tail, yr_tail = parse_year_tail(q)
        y = year if year is not None else y_tail
        yf, yt = year_from, year_to
        if yr_tail is not None:
            yf = yf if yf is not None else yr_tail[0]
            yt = yt if yt is not None else yr_tail[1]

        pos_groups, neg_groups, phrases, hl_terms, soft_phrases = parse_query(base_q)
        if not pos_groups and not neg_groups and not phrases:
            return JSONResponse(
                {"items": [], "total_hits": 0, "page": page, "page_size": page_size,
                 "has_more": False, "next_page": None, "error": None, "order_used": order},
                headers=JSON_HEADERS
            )

        hits: List[Tuple[int, Optional[datetime], Dict[str, Any]]] = []

        for rec in iter_records():
            # 年フィルタ
            if not matches_year(rec, y, yf, yt):
                continue
            score = compute_score(rec, pos_groups, neg_groups, phrases, soft_phrases)
            if score < 0:
                continue
            d = record_date(rec)
            hits.append((score, d, rec))

        total_hits = len(hits)

        # 並び順
        if order == "latest":
            hits.sort(key=lambda x: (x[1] or datetime.min), reverse=True)
            order_used = "latest"
        else:
            hits.sort(key=lambda x: (x[0], x[1] or datetime.min), reverse=True)
            order_used = "relevance"

        # ページング
        start = (page - 1) * page_size
        end = start + page_size
        page_hits = hits[start:end]
        has_more = end < total_hits
        next_page = page + 1 if has_more else None

        # 結果
        items: List[Dict[str, Any]] = []
        for i, (_, _d, rec) in enumerate(page_hits):
            items.append(build_item(rec, hl_terms, is_first_in_page=(i == 0)))

        # rank（全体順位）付与
        for idx, _ in enumerate(hits, start=1):
            if start < idx <= end:
                items[idx - start - 1]["rank"] = idx

        return JSONResponse(
            {"items": items, "total_hits": total_hits, "page": page, "page_size": page_size,
             "has_more": has_more, "next_page": next_page, "error": None, "order_used": order_used},
            headers=JSON_HEADERS
        )

    except Exception as e:
        return JSONResponse(
            {"items": [], "total_hits": 0, "page": 1, "page_size": page_size,
             "has_more": False, "next_page": None, "error": "exception", "message": textify(e)},
            headers=JSON_HEADERS
        )

@app.get("/")
def root():
    return PlainTextResponse("Mini Rose Search API is running.\n", headers={"content-type":"text/plain; charset=utf-8"})
