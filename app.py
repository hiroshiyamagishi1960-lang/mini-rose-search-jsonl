# app.py — ミニバラ盆栽愛好会 デジタル資料館（JSONL版）
# 一般化された多語検索対応:
#  - 空白=AND, "|"=OR, "-語"=NOT, "..."=フレーズ一致
#  - 語尾の一般ルール（結果/報告/案内/募集/要項/記録/予定/日程…）
#  - こけ/コケ/苔 の同一視（かな折り＋同義語）
#  - 先頭カード=本文冒頭~300字固定、以降=ヒット周辺~160字
#  - 例外でもHTTP200でJSONを返しUIが落ちない
#  - /diag でKB自己診断
# 2025-10-18 general-v1

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

app = FastAPI(title="mini-rose-search-jsonl (kb.jsonl)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ===== 設定 =====
KB_URL = os.getenv("KB_URL", "").strip()
KB_PATH = os.getenv("KB_PATH", "/data/kb.jsonl").strip() or "/data/kb.jsonl"
VERSION = os.getenv("APP_VERSION", "jsonl-2025-10-18-general-v1")

# ===== 文字整形 =====
KATA_TO_HIRA = str.maketrans({chr(k): chr(k - 0x60) for k in range(ord("ァ"), ord("ン") + 1)})
HIRA_TO_KATA = str.maketrans({chr(h): chr(h + 0x60) for h in range(ord("ぁ"), ord("ん") + 1)})

def to_hira(s: str) -> str:
    return s.translate(KATA_TO_HIRA)

def to_kata(s: str) -> str:
    return s.translate(HIRA_TO_KATA)

def normalize_text(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("\u3000", " ")
    s = re.sub(r"[\r\n\t]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

# 文字化けの簡易補正（効けば採用）
MOJI_PAT = re.compile(r"[縺蜑荳邨鬘蛻譛繧蝨髱]")
def fix_mojibake(s: str) -> str:
    if not s or not MOJI_PAT.search(s):
        return s
    try:
        t = s.encode("cp932", errors="ignore").decode("utf-8", errors="ignore")
        if t and t != s:
            return t
    except Exception:
        pass
    try:
        t = s.encode("latin1", errors="ignore").decode("utf-8", errors="ignore")
        if t and t != s:
            return t
    except Exception:
        pass
    return s

# ===== 同義語・語尾 =====
SYNONYMS: Dict[str, List[str]] = {
    # 代表例：苔
    "苔": ["コケ", "こけ"],
    "コケ": ["苔", "こけ"],
    "こけ": ["苔", "コケ"],
    # よく使う一般語尾（ひら・カナも吸収）
    "結果": ["けっか", "ケッカ"],
    "報告": ["ほうこく", "ホウコク", "レポート"],
    "案内": ["あんない", "アンナイ", "ご案内"],
    "募集": ["ぼしゅう", "ボシュウ"],
    "要項": ["ようこう", "ヨウコウ"],
    "記録": ["きろく", "キロク"],
    "予定": ["よてい", "ヨテイ"],
    "日程": ["にってい", "ニッテイ"],
    "要領": ["ようりょう", "ヨウリョウ"],
    "一覧": ["いちらん", "イチラン"],
    "まとめ": ["マトメ"],
}
SUFFIXES = set([
    "結果","報告","案内","募集","要項","記録","予定","日程","要領","一覧","まとめ"
])

# ===== KB 確保 =====
def ensure_kb() -> Tuple[int, str]:
    os.makedirs(os.path.dirname(KB_PATH), exist_ok=True)
    if KB_URL and KB_URL.startswith("http"):
        if requests is None:
            raise RuntimeError("requests が利用できません（requirements.txt に requests を追加してください）")
        r = requests.get(KB_URL, timeout=30)
        r.raise_for_status()
        with open(KB_PATH, "wb") as f:
            f.write(r.content)

    if not os.path.exists(KB_PATH):
        raise FileNotFoundError(f"KB not found: {KB_PATH}")

    line_count = 0
    sha = hashlib.sha256()
    with open(KB_PATH, "rb") as f:
        for line in f:
            sha.update(line)
            line_count += 1
    return line_count, sha.hexdigest()

KB_LINES: int = 0
KB_HASH: str = ""

@app.on_event("startup")
def _startup():
    global KB_LINES, KB_HASH
    try:
        KB_LINES, KB_HASH = ensure_kb()
    except Exception:
        KB_LINES, KB_HASH = 0, ""

# ===== ユーティリティ =====
def parse_date_str(s: str) -> Optional[datetime]:
    if not s:
        return None
    s = s.strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d", "%Y-%m", "%Y/%m", "%Y.%m", "%Y"):
        try:
            return datetime.strptime(s[:len(fmt)], fmt)
        except Exception:
            continue
    m = re.match(r"^(\d{4})", s)
    if m:
        try:
            return datetime(int(m.group(1)), 1, 1)
        except Exception:
            return None
    return None

def extract_year_filter(q: str) -> Tuple[str, Optional[int], Optional[int]]:
    s = normalize_text(q)
    m = re.search(r"(?:^|\s)(\d{4})-(\d{4})\s*$", s)
    if m:
        a, b = int(m.group(1)), int(m.group(2))
        s = s[:m.start()].strip()
        return s, min(a, b), max(a, b)
    m = re.search(r"(?:^|\s)(\d{4})\s*$", s)
    if m:
        y = int(m.group(1))
        s = s[:m.start()].strip()
        return s, y, y
    return s, None, None

def textify(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, str):
        return x
    return str(x)

def record_date(rec: Dict[str, Any]) -> Optional[datetime]:
    for k in ("date", "date_primary", "Date", "published_at"):
        d = rec.get(k)
        if d:
            dt = parse_date_str(textify(d))
            if dt:
                return dt
    return None

# ===== トークナイズ（AND / OR / NOT / フレーズ） =====
TOKEN_RE = re.compile(r'"([^"]+)"|(\S+)')  # "..." または 非空白塊

def _expand_one_term(term: str) -> List[str]:
    """単語1つを同義語＆かな両対応に展開"""
    term = normalize_text(term)
    hira = to_hira(term)
    out = [hira]
    out += SYNONYMS.get(term, [])
    out += SYNONYMS.get(hira, [])
    kata = to_kata(hira)
    if kata != term:
        out.append(kata)
    # 重複除去・空除去
    out = [normalize_text(t) for t in out if t]
    out = list(dict.fromkeys(out))
    return out

def _maybe_split_suffix(token: str) -> Tuple[Optional[str], Optional[str]]:
    """末尾が一般語尾なら (base, suffix) を返す。そうでなければ (None, None)。"""
    for suf in sorted(SUFFIXES, key=len, reverse=True):
        if token.endswith(suf) and len(token) > len(suf):
            base = token[:len(token) - len(suf)]
            if base:
                return base, suf
    return None, None

def parse_query(q: str) -> Tuple[List[List[str]], List[List[str]], List[str]]:
    """
    返り値:
      - pos_groups: ANDの各グループ（各要素は OR リスト）
      - neg_groups: 除外グループ（どれか当たれば除外）
      - phrase_terms: 「base+suffix」等のフレーズ完全一致候補
    仕様:
      - 空白=AND, '|'=OR, '-語'=NOT, "..."=フレーズ（ANDの1項目として扱う）
      - tokenが「base+suffix（一般語尾）」なら AND で2グループに分割し、フレーズ候補も追加
      - 分かち書きで base と suffix が両方含まれていれば、それも自然に AND になる
    """
    base = normalize_text(q)
    if not base:
        return [], [], []
    hira_base = to_hira(base)

    pos_groups: List[List[str]] = []
    neg_groups: List[List[str]] = []
    phrase_terms: List[str] = []

    for m in TOKEN_RE.finditer(hira_base):
        token = m.group(1) if m.group(1) is not None else m.group(2)
        if not token:
            continue

        is_neg = token.startswith("-")
        if is_neg:
            token = token[1:].strip()
            if not token:
                continue

        # OR分解
        parts = [t for t in token.split("|") if t]

        # フレーズ（"..."）はそのまま1グループ
        # TOKEN_REで "..." は m.group(1) に入り、上ですでに抽出済み

        # 各パーツを処理
        # - 一般語尾なら AND分割（base群 と suffix群）＋ フレーズ候補 base+suffix
        # - それ以外は OR群としてまとめる
        and_chunks: List[List[str]] = []  # ここに AND で積む

        for p in parts:
            base_s, suf = _maybe_split_suffix(p)
            if base_s and suf:
                base_group = _expand_one_term(base_s)
                suf_group = _expand_one_term(suf)
                and_chunks.append(base_group)
                and_chunks.append(suf_group)
                # フレーズ候補（ひら/カナ）
                phrase_terms.append(normalize_text(base_s + suf))
                phrase_terms.append(normalize_text(to_kata(base_s) + suf))
            else:
                # 普通の語（OR群へ）
                or_group = _expand_one_term(p)
                and_chunks.append(or_group)

        # and_chunks を AND として積む（各要素は ORの集合）
        if is_neg:
            neg_groups.extend(and_chunks)
        else:
            pos_groups.extend(and_chunks)

    # フレーズ候補整形
    phrase_terms = [t for t in phrase_terms if t]
    phrase_terms = list(dict.fromkeys(phrase_terms))
    return pos_groups, neg_groups, phrase_terms

# ===== スコアリング =====
FIELD_WEIGHTS = {
    "title": 12,
    "text":  8,
    "author": 5,
    "issue":  3,
    "date":   2,
    "category": 2,
}
PHRASE_BONUS_TITLE = 100
PHRASE_BONUS_TEXT  = 60

def _get_field(rec: Dict[str, Any], keys: List[str]) -> str:
    for k in keys:
        v = rec.get(k)
        if v:
            return textify(v)
    return ""

def record_as_text(rec: Dict[str, Any], field: str) -> str:
    key_map = {
        "title": ["title"],
        "text": ["text", "content", "body"],
        "author": ["author"],
        "issue": ["issue"],
        "date": ["date", "date_primary"],
        "category": ["category"],
        "url": ["url", "source"],
    }
    raw = _get_field(rec, key_map.get(field, [field]))
    return fix_mojibake(raw)

def match_count(term: str, text: str) -> int:
    if not term or not text:
        return 0
    a = normalize_text(text); b = normalize_text(term)
    ah = to_hira(a);         bh = to_hira(b)
    return a.count(b) + ah.count(bh)

def group_hit_count(group: List[str], text: str) -> int:
    """ORグループの中で何回ヒットしたか（累積）"""
    total = 0
    for t in group:
        total += match_count(t, text)
    return total

def matches_group_any_field(rec: Dict[str, Any], group: List[str]) -> Tuple[bool, int]:
    """ORグループがどこかのフィールドにヒットしたか／スコア寄与"""
    hit = False
    score_add = 0
    for field, w in FIELD_WEIGHTS.items():
        s = record_as_text(rec, "date") if field == "date" else record_as_text(rec, field)
        if not s:
            continue
        c = group_hit_count(group, s)
        if c > 0:
            hit = True
            score_add += w * c
    return hit, score_add

def contains_phrase(s: str, phrases: List[str]) -> bool:
    if not s or not phrases:
        return False
    ns = normalize_text(s); nh = to_hira(ns)
    for p in phrases:
        np = normalize_text(p)
        if np in ns or to_hira(np) in nh:
            return True
    return False

def compute_score(rec: Dict[str, Any],
                  pos_groups: List[List[str]],
                  neg_groups: List[List[str]],
                  phrase_terms: List[str]) -> int:
    # 1) NOT判定（どれか当たれば除外）
    for ng in neg_groups:
        ok, _ = matches_group_any_field(rec, ng)
        if ok:
            return -1

    # 2) AND判定（全グループがヒット必須）＆スコア集計
    total = 0
    for g in pos_groups:
        ok, add = matches_group_any_field(rec, g)
        if not ok:
            return -1
        total += add

    # 3) フレーズ完全一致ボーナス
    title = record_as_text(rec, "title")
    text  = record_as_text(rec, "text")
    if contains_phrase(title, phrase_terms):
        total += PHRASE_BONUS_TITLE
    if contains_phrase(text,  phrase_terms):
        total += PHRASE_BONUS_TEXT

    return total

# ===== 抜粋・ハイライト =====
TAG_RE = re.compile(r"<[^>]+>")

def html_escape(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def highlight(text: str, terms: List[str]) -> str:
    if not text:
        return ""
    esc = html_escape(text)
    # 長い語から順に
    for t in sorted(set(terms), key=len, reverse=True):
        et = html_escape(t)
        if et:
            esc = re.sub(re.escape(et), lambda m: f"<mark>{m.group(0)}</mark>", esc)
    return esc

def make_head_snippet(body: str, terms: List[str], max_chars: int) -> str:
    if not body:
        return ""
    b = fix_mojibake(body)
    head = b[:max_chars]
    out = highlight(head, terms)
    if len(b) > max_chars:
        out += "…"
    return out

def make_hit_snippet(body: str, terms: List[str], max_chars: int, side: int = 80) -> str:
    if not body:
        return ""
    b = fix_mojibake(body)
    marked = highlight(b, terms)
    plain = TAG_RE.sub("", marked)
    if not plain:
        return ""
    m = re.search(r"<mark>", marked)
    if not m:
        return make_head_snippet(b, terms, max_chars)
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

# ===== エンドポイント =====
@app.get("/health")
def health():
    ok = os.path.exists(KB_PATH)
    return {"ok": ok, "kb_url": KB_URL, "kb_size": KB_LINES, "kb_fingerprint": KB_HASH}

@app.get("/version")
def version():
    return {"version": VERSION}

@app.get("/diag")
def diag():
    has_ui = os.path.exists(os.path.join("static", "ui.html"))
    return {
        "kb": {"path": KB_PATH, "lines": KB_LINES, "sha256": KB_HASH, "url": KB_URL},
        "env": {"APP_VERSION": VERSION},
        "ui": {"static_ui_html": has_ui},
    }

@app.get("/ui")
def ui():
    path = os.path.join("static", "ui.html")
    if os.path.exists(path):
        return FileResponse(path, media_type="text/html; charset=utf-8")
    return PlainTextResponse("static/ui.html not found", status_code=404)

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

@app.get("/api/search")
def api_search(
    q: str = Query("", description="検索クエリ"),
    page: int = Query(1, ge=1),
    page_size: int = Query(5, ge=1, le=50),
    order: str = Query("relevance", pattern="^(relevance|latest)$"),
):
    """
    - 空白=AND, "|"=OR, "-語"=NOT, "..."=フレーズ一致
    - 語尾の一般ルール（結果/報告/案内/募集/要項/記録/予定/日程…）
    - relevance: スコア順（同点は新しい日付優先） / latest: 日付降順
    - 1ページ目先頭=冒頭300字、以降=ヒット周辺160字
    - 例外時も200でJSON返却（UIを落とさない）
    """
    try:
        if not os.path.exists(KB_PATH):
            return JSONResponse(
                {"items": [], "total_hits": 0, "page": page, "page_size": page_size,
                 "has_more": False, "next_page": None, "error": "kb_missing", "order_used": order},
                headers={"Cache-Control": "no-store"},
            )

        q_wo_year, y_from, y_to = extract_year_filter(q)
        pos_groups, neg_groups, phrase_terms = parse_query(q_wo_year)
        if not pos_groups and not neg_groups:
            return JSONResponse(
                {"items": [], "total_hits": 0, "page": page, "page_size": page_size,
                 "has_more": False, "next_page": None, "error": None, "order_used": order},
                headers={"Cache-Control": "no-store"},
            )

        hits: List[Tuple[int, Optional[datetime], Dict[str, Any]]] = []

        for rec in iter_records():
            # 年フィルタ
            if y_from or y_to:
                d = record_date(rec)
                if not d:
                    continue
                if y_from and d.year < y_from:
                    continue
                if y_to and d.year > y_to:
                    continue

            score = compute_score(rec, pos_groups, neg_groups, phrase_terms)
            if score < 0:
                continue
            d = record_date(rec)
            hits.append((score, d, rec))

        total_hits = len(hits)

        # 並び替え
        if order == "latest":
            hits.sort(key=lambda x: (x[1] or datetime.min), reverse=True)
            order_used = "latest"
        else:
            hits.sort(key=lambda x: (x[0], x[1] or datetime.min), reverse=True)  # 同点は新しい方
            order_used = "relevance"

        # ページング
        start = (page - 1) * page_size
        end = start + page_size
        page_hits = hits[start:end]
        has_more = end < total_hits
        next_page = page + 1 if has_more else None

        # ハイライト語（実際に使っている語＋フレーズ）
        hl_terms: List[str] = sorted({t for g in pos_groups for t in g} | {t for g in neg_groups for t in g} | set(phrase_terms))

        items: List[Dict[str, Any]] = []
        for i, (_, _d, rec) in enumerate(page_hits):
            items.append(build_item(rec, hl_terms, is_first_in_page=(i == 0)))

        # rank 付与
        for idx, _ in enumerate(hits, start=1):
            if start < idx <= end:
                items[idx - start - 1]["rank"] = idx

        return JSONResponse(
            {"items": items, "total_hits": total_hits, "page": page, "page_size": page_size,
             "has_more": has_more, "next_page": next_page, "error": None, "order_used": order_used},
            headers={"Cache-Control": "no-store"},
        )

    except Exception as e:
        return JSONResponse(
            {"items": [], "total_hits": 0, "page": 1, "page_size": page_size,
             "has_more": False, "next_page": None, "error": "exception", "message": textify(e)},
            headers={"Cache-Control": "no-store"},
        )

# ローカル実行
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
