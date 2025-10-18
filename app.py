# app.py  ― ミニバラ盆栽愛好会 デジタル資料館（JSONL版）
# FastAPI + Uvicorn（Render想定）
# - /api/search : 検索API
# - /health     : ヘルスチェック
# - /version    : バージョン表示
# - /diag       : 自己診断（KB/環境の確認）
# - /ui         : static/ui.html を返す
#
# 仕様反映:
#  1) 1ページ目の先頭カードは常に「冒頭～約300字」
#  2) 「結果」は停用語として検索/ハイライトから除外（誤ヒットと見かけ対策）
#  3) 文字化け（縺…/邨… など）を可能な範囲で自動補正してからマッチ＆表示
#  4) 例外時も 200 + JSON を返す（UIが500で落ちない）

import os
import io
import re
import json
import hashlib
import unicodedata
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, FileResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware

try:
    import requests  # requirements.txt に requests が必要
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

# ======== 設定 ========
KB_URL = os.getenv("KB_URL", "").strip()
KB_PATH = os.getenv("KB_PATH", "/data/kb.jsonl").strip() or "/data/kb.jsonl"
VERSION = os.getenv("APP_VERSION", "jsonl-2025-10-18-hotfix-v2")

# ======== 文字整形・停用語 ========
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
    s = re.sub(r"\s+", " ", s).strip()  # ← .trim() バグ修正
    return s

# 文字化けの簡易補正（UTF-8↔CP932 誤変換の痕跡を見たら試す）
MOJI_PAT = re.compile(r"[縺蜑荳邨鬘蛻譛繧蝨髱]")
def fix_mojibake(s: str) -> str:
    if not s:
        return s
    if not MOJI_PAT.search(s):
        return s
    # よくある誤変換を2通りほど試す（失敗時は元のまま）
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

# 同義語
SYNONYMS: Dict[str, List[str]] = {
    "苔": ["コケ", "こけ"],
    "コケ": ["苔", "こけ"],
    "こけ": ["苔", "コケ"],
}

STOP_TERMS = {"結果"}  # 汎用語は検索にもハイライトにも使わない

COMPOUND_RESULT_RE = re.compile(r"^(.+?)結果$")

def expand_query_to_groups(q: str) -> List[List[str]]:
    """
    入力qを正規化→ANDグループ列に展開。
    - 「◯◯結果」は「◯◯」のみを使い、「結果」は停用（ANDに含めない）
    - 停用語は完全に除外
    """
    base = normalize_text(q)
    if not base:
        return []
    hira_base = to_hira(base)
    raw_terms = [t for t in re.split(r"\s+", hira_base) if t]

    groups: List[List[str]] = []
    for term in raw_terms:
        m = COMPOUND_RESULT_RE.match(term)
        if m:
            left = m.group(1)
            if left and left not in STOP_TERMS:
                left_group = [left] + SYNONYMS.get(left, [])
                left_group = list(dict.fromkeys(left_group))
                # かな→カナも足す
                kata = to_kata(left)
                if kata != left:
                    left_group.append(kata)
                groups.append(left_group)
            continue

        if term in STOP_TERMS:
            continue

        group: List[str] = [term] + SYNONYMS.get(term, [])
        kata = to_kata(term)
        if kata != term:
            group.append(kata)
        group = list(dict.fromkeys(group))
        groups.append(group)

    return groups

# ======== JSONL 読み込み ========
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

# ======== ユーティリティ ========
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

# ======== スコアリング ========
FIELD_WEIGHTS = {
    "title": 12,
    "text": 8,
    "author": 5,
    "issue": 3,
    "date": 2,
    "category": 2,
}

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

def match_term_in_text(t: str, s: str) -> int:
    if not t or not s:
        return 0
    a = normalize_text(s)
    b = normalize_text(t)
    ah = to_hira(a)
    bh = to_hira(b)
    return a.count(b) + ah.count(bh)

def compute_score(rec: Dict[str, Any], groups: List[List[str]]) -> int:
    total = 0
    for group in groups:
        group_hit = False
        for term in group:
            for field, w in FIELD_WEIGHTS.items():
                s = record_as_text(rec, "date") if field == "date" else record_as_text(rec, field)
                if not s:
                    continue
                c = match_term_in_text(term, s)
                if c > 0:
                    total += w * c
                    group_hit = True
        if not group_hit:
            return -1
    return total

# ======== スニペット生成 ========
TAG_RE = re.compile(r"<[^>]+>")

def html_escape(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def highlight(text: str, terms: List[str]) -> str:
    if not text:
        return ""
    esc = html_escape(text)
    for t in sorted({t for t in terms if t not in STOP_TERMS}, key=len, reverse=True):
        et = html_escape(t)
        if et:
            esc = re.sub(re.escape(et), lambda m: f"<mark>{m.group(0)}</mark>", esc)
    return esc

def make_head_snippet(body: str, terms: List[str], max_chars: int) -> str:
    """本文冒頭から max_chars、必要なら…を付ける。"""
    if not body:
        return ""
    b = fix_mojibake(body)
    head = b[:max_chars]
    out = highlight(head, terms)
    if len(b) > max_chars:
        out += "…"
    return out

def make_hit_snippet(body: str, terms: List[str], max_chars: int, side: int = 80) -> str:
    """最初のヒット周辺を抜粋。ヒットが無ければ冒頭にフォールバック。"""
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

# ======== エンドポイント ========
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

def build_item(rec: Dict[str, Any], terms_for_hit: List[str], is_first_in_page: bool) -> Dict[str, Any]:
    body = record_as_text(rec, "text")
    if is_first_in_page:
        snippet = make_head_snippet(body, terms_for_hit, max_chars=300)  # ★ 先頭300字固定
    else:
        snippet = make_hit_snippet(body, terms_for_hit, max_chars=160, side=80)
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
    - AND（語群ごとに1語以上ヒット）
    - relevance: スコア順 / latest: 発行日降順
    - 1ページ目の先頭カードは冒頭300字、以降はヒット周辺160字
    - 例外時も 200 + JSON を返す（UIを落とさない）
    """
    try:
        if not os.path.exists(KB_PATH):
            return JSONResponse(
                {"items": [], "total_hits": 0, "page": page, "page_size": page_size,
                 "has_more": False, "next_page": None, "error": "kb_missing", "order_used": order},
                headers={"Cache-Control": "no-store"},
            )

        q_wo_year, y_from, y_to = extract_year_filter(q)
        groups = expand_query_to_groups(q_wo_year)
        if not groups:
            return JSONResponse(
                {"items": [], "total_hits": 0, "page": page, "page_size": page_size,
                 "has_more": False, "next_page": None, "error": None, "order_used": order},
                headers={"Cache-Control": "no-store"},
            )

        hits: List[Tuple[int, Optional[datetime], Dict[str, Any]]] = []
        for rec in iter_records():
            if y_from or y_to:
                d = record_date(rec)
                if not d:
                    continue
                if y_from and d.year < y_from:
                    continue
                if y_to and d.year > y_to:
                    continue

            score = compute_score(rec, groups)
            if score < 0:
                continue
            d = record_date(rec)
            hits.append((score, d, rec))

        total_hits = len(hits)

        order_used = order
        if order == "latest":
            hits.sort(key=lambda x: (x[1] or datetime.min), reverse=True)
        else:
            hits.sort(key=lambda x: (x[0], x[1] or datetime.min), reverse=True)

        start = (page - 1) * page_size
        end = start + page_size
        page_hits = hits[start:end]
        has_more = end < total_hits
        next_page = page + 1 if has_more else None

        items: List[Dict[str, Any]] = []
        # ハイライト対象語から停用語を除外
        terms_for_hit: List[str] = sorted({t for g in groups for t in g if t not in STOP_TERMS})
        for i, (_, _d, rec) in enumerate(page_hits):
            item = build_item(rec, terms_for_hit, is_first_in_page=(i == 0))
            items.append(item)

        for idx, _ in enumerate(hits, start=1):
            if start < idx <= end:
                items[idx - start - 1]["rank"] = idx

        resp = {
            "items": items,
            "total_hits": total_hits,
            "page": page,
            "page_size": page_size,
            "has_more": has_more,
            "next_page": next_page,
            "error": None,
            "order_used": order_used,
        }
        return JSONResponse(resp, headers={"Cache-Control": "no-store"})

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
