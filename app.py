# app.py — ミニバラ盆栽愛好会 デジタル資料館（JSONL版）
# 一般検索：空白=AND / '|'=OR / '-語'=NOT / "..."=フレーズ（単語は自動分割しない）
# 1件目=冒頭~300字、2件目以降=ヒット周辺~160字、苔=コケ=こけ 同一視
# 例外時もHTTP200+JSONで返却、/diagで自己診断（直近のエラー露出）
# VERSION: jsonl-2025-10-18-general-v5

import os, io, re, json, hashlib, unicodedata
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, FileResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware

try:
    import requests  # Render想定（requirements.txt に requests）
except Exception:
    requests = None

# ==================== アプリ初期化 ====================
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
# 相対は相対のまま許可（以前の“/data固定”を廃止）。絶対ならそのまま。
KB_PATH  = os.path.normpath(_cfg if os.path.isabs(_cfg) else os.path.join(os.getcwd(), _cfg))
VERSION  = os.getenv("APP_VERSION", "jsonl-2025-10-18-general-v5")

# ==================== 診断用グローバル ====================
KB_LINES: int = 0
KB_HASH:  str = ""
LAST_ERROR: str = ""
LAST_EVENT: str = ""  # 例: "fetched", "kept_local", "converted_json_array_to_jsonl", "no_file"

# ==================== 正規化・ユーティリティ ====================
KATA_TO_HIRA = str.maketrans({chr(k): chr(k - 0x60) for k in range(ord("ァ"), ord("ン") + 1)})
HIRA_TO_KATA = str.maketrans({chr(h): chr(h + 0x60) for h in range(ord("ぁ"), ord("ん") + 1)})

def to_hira(s: str) -> str:
    return (s or "").translate(KATA_TO_HIRA)

def to_kata(s: str) -> str:
    return (s or "").translate(HIRA_TO_KATA)

def normalize_text(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s)          # 全角/半角・互換正規化
    s = s.replace("\u3000", " ")                   # 全角スペース→半角
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

# ==================== 同義語（必要最低限） ====================
SYNONYMS: Dict[str, List[str]] = {
    "苔": ["コケ", "こけ"],
    "コケ": ["苔", "こけ"],
    "こけ": ["苔", "コケ"],
}

# ==================== KB取得・保存（JSONL保証） ====================
def _bytes_to_jsonl(blob: bytes) -> bytes:
    """
    もし全体が JSON 配列なら JSONL に変換。
    既にJSONL（各行が独立のJSON）ならそのまま返す。
    """
    if not blob:
        return b""
    s = blob.decode("utf-8", errors="replace").strip()
    if not s:
        return b""
    # JSON配列 → JSONL
    if s.startswith("["):
        try:
            data = json.loads(s)
            if isinstance(data, list):
                lines = [
                    json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
                    for obj in data
                ]
                out = ("\n".join(lines) + "\n").encode("utf-8")
                return out
        except Exception:
            # 解析に失敗したらそのまま戻す（後段でエラー計上）
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

def ensure_kb() -> Tuple[int, str]:
    """
    1) KB_URL があれば取得 → JSONL保証 → 一時ファイルに保存 → os.replace でアトミック置換
    2) ローカルKBが JSON 配列形式なら JSONL に変換
    3) 行数・指紋を計算
    """
    global LAST_ERROR, LAST_EVENT
    LAST_ERROR = ""
    LAST_EVENT = ""

    # 1) 取得して保存
    if KB_URL:
        if requests is None:
            LAST_ERROR = "requests が利用できません（requirements.txt に requests を追加）"
        else:
            try:
                r = requests.get(KB_URL, timeout=30)
                r.raise_for_status()
                blob = r.content
                blob = _bytes_to_jsonl(blob)
                # 一時保存→置換（途中失敗でも旧ファイルは保持）
                tmp_path = KB_PATH + ".tmp"
                os.makedirs(os.path.dirname(KB_PATH), exist_ok=True) if os.path.dirname(KB_PATH) else None
                with open(tmp_path, "wb") as wf:
                    wf.write(blob)
                os.replace(tmp_path, KB_PATH)
                LAST_EVENT = "fetched"
            except Exception as e:
                LAST_ERROR = f"fetch_or_save_failed: {type(e).__name__}: {e}"

    # 2) ローカルで JSON 配列を検出したら JSONL に変換
    if os.path.exists(KB_PATH):
        try:
            with open(KB_PATH, "rb") as rf:
                head = rf.read(1)
            if head == b"[":
                try:
                    with open(KB_PATH, "rb") as rf:
                        blob = rf.read()
                    blob2 = _bytes_to_jsonl(blob)
                    if blob2 and blob2 != blob:
                        tmp_path = KB_PATH + ".tmp"
                        with open(tmp_path, "wb") as wf:
                            wf.write(blob2)
                        os.replace(tmp_path, KB_PATH)
                        LAST_EVENT = (LAST_EVENT + "+converted_json_array_to_jsonl").strip("+")
                except Exception as e:
                    LAST_ERROR = f"convert_json_array_failed: {type(e).__name__}: {e}"
        except Exception as e:
            LAST_ERROR = f"read_head_failed: {type(e).__name__}: {e}"

    # 3) 行数・指紋
    if os.path.exists(KB_PATH):
        try:
            lines, sha = _compute_lines_and_hash(KB_PATH)
            if lines <= 0:
                # ファイルはあるが実質空
                LAST_EVENT = LAST_EVENT or "empty_file"
            return lines, sha
        except Exception as e:
            LAST_ERROR = f"hash_failed: {type(e).__name__}: {e}"
            return 0, ""
    else:
        LAST_EVENT = LAST_EVENT or "no_file"
        return 0, ""

# ==================== 起動時にKB確認 ====================
@app.on_event("startup")
def _startup():
    global KB_LINES, KB_HASH
    try:
        KB_LINES, KB_HASH = ensure_kb()
    except Exception as e:
        # ここには通常来ないが、最終防御
        KB_LINES, KB_HASH = 0, ""
        try:
            from traceback import format_exc
            globals()["LAST_ERROR"] = f"startup_failed: {type(e).__name__}: {e}\n{format_exc()}"
        except Exception:
            globals()["LAST_ERROR"] = f"startup_failed: {type(e).__name__}: {e}"

# ==================== 日付パース ====================
JP_DATE_PATTERNS = [
    re.compile(r"^(\d{4})年(\d{1,2})月(\d{1,2})日?$"),
    re.compile(r"^(\d{4})年(\d{1,2})月$"),
    re.compile(r"^(\d{4})年$"),
]

def parse_date_str(s: str) -> Optional[datetime]:
    if not s:
        return None
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
            dt = parse_date_str(textify(d))
            if dt: return dt
    return None

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

# ==================== クエリ解析（空白=AND, '|'=OR, '-語'=NOT, "..."=フレーズ） ====================
TOKEN_RE = re.compile(r'"([^"]+)"|(\S+)')  # "..." or non-space token

def _expand_term_forms(term: str) -> List[str]:
    """検索用の表記ゆれ吸収（かな・カナ・同義）。入力語は自動分割しない。"""
    term = normalize_text(term)
    forms = [term]

    # かな・カナ相互
    h = to_hira(term)
    k = to_kata(h)
    for x in (h, k):
        if x and x not in forms:
            forms.append(x)

    # 同義語
    for key in [term, h, k]:
        for alt in SYNONYMS.get(key, []):
            alt_n = normalize_text(alt)
            if alt_n and alt_n not in forms:
                forms.append(alt_n)
            # その同義語の かな/カナ も追加
            ah = to_hira(alt_n)
            ak = to_kata(ah)
            for x in (ah, ak):
                if x and x not in forms:
                    forms.append(x)

    return forms

def parse_query(q: str) -> Tuple[List[List[str]], List[List[str]], List[str], List[str]]:
    """
    戻り値:
      pos_groups: ANDの各グループ（要素は OR リスト：複数表記ゆれ）
      neg_groups: 除外（NOT）の各グループ
      phrases:    フレーズ（"..."）のリスト（必須条件）
      hl_terms:   ハイライト用語（入力で有効になった語＝分割なし）
    """
    base = normalize_text(q)
    if not base:
        return [], [], [], []

    pos_groups: List[List[str]] = []
    neg_groups: List[List[str]] = []
    phrases: List[str] = []
    hl_terms: List[str] = []

    for m in TOKEN_RE.finditer(base):
        token = m.group(1) if m.group(1) is not None else m.group(2)
        if not token:
            continue

        # フレーズ（必須条件）：並びそのものを含む
        if m.group(1) is not None:
            phrases.append(normalize_text(token))
            # ハイライトはフレーズ中の各語にも付けたいので分割して追加
            for t in re.split(r"\s+", token.strip()):
                if t:
                    for f in _expand_term_forms(t):
                        if f not in hl_terms:
                            hl_terms.append(f)
            continue

        # NOT？
        is_neg = token.startswith("-")
        if is_neg:
            token = token[1:].strip()
            if not token:
                continue

        # OR（縦棒）を複数語として扱う
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
            # ハイライト対象（入力語の表記ゆれすべて）
            for f in group:
                if f not in hl_terms:
                    hl_terms.append(f)

    return pos_groups, neg_groups, phrases, hl_terms

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

def _norm_pair(s: str) -> Tuple[str, str]:
    ns = normalize_text(s)
    return ns, to_hira(ns)

def _count_occurrences(needle: str, hay: str) -> int:
    """NFKC＆ひらがな化の双方で部分一致回数を数える（続き文字OK、空白有無OK）。"""
    if not needle or not hay: return 0
    a, ah = _norm_pair(hay)
    b, bh = _norm_pair(needle)
    return a.count(b) + ah.count(bh)

def _contains_phrase(hay: str, phrase: str) -> bool:
    if not hay or not phrase: return False
    a, ah = _norm_pair(hay)
    p, ph = _norm_pair(phrase)
    # 余分な空白を1つに圧縮して比較（全角半角吸収）
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
    return hit, score_add

def compute_score(rec: Dict[str, Any],
                  pos_groups: List[List[str]],
                  neg_groups: List[List[str]],
                  phrases: List[str]) -> int:
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

    # フレーズ（必須条件）：どれかのフィールドに含まれること
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

    return total

# ==================== 抜粋・ハイライト ====================
TAG_RE = re.compile(r"<[^>]+>")

def html_escape(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def highlight(text: str, terms: List[str]) -> str:
    """termsの各表記をそのまま<mark>で囲む（スペース・全角半角の差は本文側の実表記で付与）。"""
    if not text:
        return ""
    esc = html_escape(text)
    # 長い順に置換（短い語で長い語の中を先に塗らないように）
    for t in sorted(set(terms), key=len, reverse=True):
        et = html_escape(t)
        if not et:
            continue
        esc = re.sub(re.escape(et), lambda m: f"<mark>{m.group(0)}</mark>", esc)
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
    # 再度長さ制御（HTMLタグ除去後の実長で）
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

# ==================== エンドポイント ====================
@app.get("/health")
def health():
    ok = os.path.exists(KB_PATH) and KB_LINES > 0
    return {
        "ok": ok,
        "kb_url": KB_URL,
        "kb_size": KB_LINES,
        "kb_fingerprint": KB_HASH,
    }

@app.get("/version")
def version():
    return {"version": VERSION}

@app.get("/diag")
def diag():
    return {
        "kb": {
            "path": KB_PATH,
            "exists": os.path.exists(KB_PATH),
            "lines": KB_LINES,
            "sha256": KB_HASH,
            "url": KB_URL,
        },
        "env": {"APP_VERSION": VERSION, "cwd": os.getcwd()},
        "last": {"event": LAST_EVENT, "error": LAST_ERROR},
        "ui": {"static_ui_html": os.path.exists(os.path.join("static", "ui.html"))},
    }

@app.get("/ui")
def ui():
    path = os.path.join("static", "ui.html")
    if os.path.exists(path):
        return FileResponse(path, media_type="text/html; charset=utf-8")
    return PlainTextResponse("static/ui.html not found", status_code=404)

def iter_records():
    if not os.path.exists(KB_PATH):
        return
    with io.open(KB_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                # 不正行はスキップ（将来 /diag に統計する場合はここでカウント）
                continue

@app.get("/api/search")
def api_search(
    q: str = Query("", description="検索クエリ"),
    page: int = Query(1, ge=1),
    page_size: int = Query(5, ge=1, le=50),
    order: str = Query("relevance", pattern="^(relevance|latest)$"),
):
    """
    - 空白=AND, '|'=OR, '-語'=NOT, "..."=フレーズ（必須条件）
    - 入力語は自動分割しない（例：'コンテスト結果' は単一語として検索）
    - relevance: スコア順（同点は新しい日付優先） / latest: 発行日降順
    - 1件目=冒頭~300字、2件目以降=ヒット周辺~160字
    - 例外時もHTTP200で JSON を返す
    """
    try:
        if not os.path.exists(KB_PATH) or KB_LINES <= 0:
            return JSONResponse(
                {"items": [], "total_hits": 0, "page": page, "page_size": page_size,
                 "has_more": False, "next_page": None, "error": "kb_missing", "order_used": order},
                headers={"Cache-Control": "no-store"},
            )

        pos_groups, neg_groups, phrases, hl_terms = parse_query(q)
        if not pos_groups and not neg_groups and not phrases:
            return JSONResponse(
                {"items": [], "total_hits": 0, "page": page, "page_size": page_size,
                 "has_more": False, "next_page": None, "error": None, "order_used": order},
                headers={"Cache-Control": "no-store"},
            )

        hits: List[Tuple[int, Optional[datetime], Dict[str, Any]]] = []

        for rec in iter_records():
            score = compute_score(rec, pos_groups, neg_groups, phrases)
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
            headers={"Cache-Control": "no-store"},
        )

    except Exception as e:
        return JSONResponse(
            {"items": [], "total_hits": 0, "page": 1, "page_size": page_size,
             "has_more": False, "next_page": None, "error": "exception", "message": textify(e)},
            headers={"Cache-Control": "no-store"},
        )

# ==================== ローカル実行 ====================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
