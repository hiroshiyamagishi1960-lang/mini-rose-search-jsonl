# app.py  ― ミニバラ盆栽愛好会 デジタル資料館（JSONL版）
# FastAPI + Uvicorn（Render想定）
# - /api/search : 検索API
# - /health     : ヘルスチェック
# - /version    : バージョン表示
# - /ui         : static/ui.html を返す
#
# 先頭=約300字抜粋、2〜5件=ハイライト周辺。コンテスト結果の分割、苔/コケ/こけの同一視に対応。

import os
import io
import re
import json
import hashlib
import unicodedata
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional

import fastapi
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, FileResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware

try:
    import requests  # Render では利用可
except Exception:
    requests = None

APP = FastAPI(title="mini-rose-search-jsonl (kb.jsonl)")

# CORS（同一オリジン前提。必要最小限）
APP.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 管理者のみの想定だが、公開閲覧UIのため広めに
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ======== 設定 ========
KB_URL = os.getenv("KB_URL", "").strip()
KB_PATH = os.getenv("KB_PATH", "/data/kb.jsonl").strip() or "/data/kb.jsonl"
VERSION = os.getenv("APP_VERSION", "jsonl-2025-10-17-normalized-stable-v6")

# ======== 文字種整形＆同義語 ========
# カタカナ→ひらがな
KATA_TO_HIRA = str.maketrans({chr(k): chr(k - 0x60) for k in range(ord("ァ"), ord("ン") + 1)})

def to_hira(s: str) -> str:
    return s.translate(KATA_TO_HIRA)

def normalize_text(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("\u3000", " ")
    s = re.sub(r"[\r\n\t]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

# 同義語（必要に応じて拡張）
SYNONYMS: Dict[str, List[str]] = {
    "苔": ["コケ", "こけ"],
    "コケ": ["苔", "こけ"],
    "こけ": ["苔", "コケ"],
    # 例： "剪定": ["せん定"], "施肥": ["肥料"], "薔薇": ["バラ"]
}

# 「○○結果」の連結語を「○○」「結果」に分割
COMPOUND_RESULT_RE = re.compile(r"^(.+?)結果$")

def expand_query_to_groups(q: str) -> List[List[str]]:
    """
    入力クエリqを正規化し、AND群（グループ）に展開する。
    各グループは OR（いずれかがヒットすればよい）。
    例：
      "コンテスト結果 苔" →
        [ ["コンテスト", "こんてすと"], ["結果"], ["苔","コケ","こけ"] ]
    """
    base = normalize_text(q)
    if not base:
        return []

    # かな変換（検索判定用）
    hira_base = to_hira(base)
    raw_terms = [t for t in re.split(r"\s+", hira_base) if t]

    groups: List[List[str]] = []
    for term in raw_terms:
        # 連結語（〜結果）なら二分割（AND になるよう二つのグループを追加）
        m = COMPOUND_RESULT_RE.match(term)
        if m:
            left = m.group(1)
            # 左辺の同義語群
            left_group = [left]
            left_group += SYNONYMS.get(left, [])
            left_group = list(dict.fromkeys(left_group))
            groups.append(left_group)
            # 「結果」グループ
            groups.append(["結果"])
            continue

        group: List[str] = [term]
        group += SYNONYMS.get(term, [])
        # カタカナ対策：ひらがな←→カタカナの両方を入れておく
        hira = term
        kata = hira.translate({ord(c): chr(ord(c) + 0x60) for c in hira})  # ひら→カタ
        if kata != term:
            group.append(kata)
        group = list(dict.fromkeys(group))
        groups.append(group)

    return groups

# ======== JSONL 読み込み ========
def ensure_kb() -> Tuple[int, str]:
    """
    KB_URL から KB_PATH に kb.jsonl を確保。件数とSHA256を返す。
    """
    os.makedirs(os.path.dirname(KB_PATH), exist_ok=True)
    if KB_URL and KB_URL.startswith("http"):
        if requests is None:
            raise RuntimeError("requests が利用できません")
        r = requests.get(KB_URL, timeout=30)
        r.raise_for_status()
        with open(KB_PATH, "wb") as f:
            f.write(r.content)

    # ファイル存在チェック
    if not os.path.exists(KB_PATH):
        # 事前配置されていればそのまま使う想定
        raise FileNotFoundError(f"KB not found: {KB_PATH}")

    # 行数とハッシュ
    line_count = 0
    sha = hashlib.sha256()
    with open(KB_PATH, "rb") as f:
        for line in f:
            sha.update(line)
            line_count += 1
    return line_count, sha.hexdigest()

KB_LINES: int = 0
KB_HASH: str = ""

@APP.on_event("startup")
def _startup():
    global KB_LINES, KB_HASH
    try:
        KB_LINES, KB_HASH = ensure_kb()
    except Exception:
        # 失敗しても起動はする（/healthで確認できる）
        KB_LINES, KB_HASH = 0, ""

# ======== ユーティリティ ========
def parse_date_str(s: str) -> Optional[datetime]:
    if not s:
        return None
    s = s.strip()
    # ISO / YYYY-MM-DD / YYYY/MM/DD / YYYY.MM.DD / YYYY
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d", "%Y-%m", "%Y/%m", "%Y.%m", "%Y"):
        try:
            return datetime.strptime(s[:len(fmt)], fmt)
        except Exception:
            continue
    # 数字だけ年
    m = re.match(r"^(\d{4})", s)
    if m:
        try:
            return datetime(int(m.group(1)), 1, 1)
        except Exception:
            return None
    return None

def extract_year_filter(q: str) -> Tuple[str, Optional[int], Optional[int]]:
    """
    クエリ末尾の年/年範囲を抽出。
    例： '剪定 1999-2001' → ('剪定', 1999, 2001)
         'コンテスト 2023' → ('コンテスト', 2023, 2023)
    """
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
    for k in key_map.get(field, [field]):
        v = rec.get(k)
        if v:
            return textify(v)
    return ""

def match_term_in_text(t: str, s: str) -> int:
    """単純出現回数（大文字小文字・全角半角をNFKCで吸収）"""
    if not t or not s:
        return 0
    a = normalize_text(s)
    b = normalize_text(t)
    # ひらがな検索も許容（本文もひらがな化版を作って試す）
    ah = to_hira(a)
    bh = to_hira(b)
    return a.count(b) + ah.count(bh)

def compute_score(rec: Dict[str, Any], groups: List[List[str]]) -> int:
    """
    AND: 各グループから少なくとも1語ヒットしている必要あり。
    スコアはヒット箇所×重みの合算。
    """
    total = 0
    for group in groups:
        group_hit = False
        for term in group:
            for field, w in FIELD_WEIGHTS.items():
                if field == "date":
                    s = record_as_text(rec, "date")
                else:
                    s = record_as_text(rec, field)
                if not s:
                    continue
                c = match_term_in_text(term, s)
                if c > 0:
                    total += w * c
                    group_hit = True
        if not group_hit:
            return -1  # このグループは未ヒット → 全体として不合格
    return total

# ======== 抜粋生成（HTML <mark> 付き） ========
TAG_RE = re.compile(r"<[^>]+>")

def html_escape(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def highlight(text: str, terms: List[str]) -> str:
    """簡易ハイライト：terms を <mark> で囲む（HTMLエスケープ後）。"""
    if not text:
        return ""
    esc = html_escape(text)
    # 長い語から順に置換（前の置換を壊さないように）
    for t in sorted(set(terms), key=len, reverse=True):
        if not t:
            continue
        et = html_escape(t)
        try:
            esc = re.sub(re.escape(et), lambda m: f"<mark>{m.group(0)}</mark>", esc)
        except Exception:
            pass
    return esc

def make_snippet(body: str, terms_for_hit: List[str], max_chars: int, side: int = 80) -> str:
    """
    terms の最初のヒット周辺を抽出。無ければ先頭から max_chars。
    戻りは HTML（<mark> 含む）。
    """
    if not body:
        return ""
    # まずマーク済み全文を作る（エスケープ込み）
    marked = highlight(body, terms_for_hit)
    plain = TAG_RE.sub("", marked)
    if not plain:
        return ""

    # 最初の<mark>の位置
    m = re.search(r"<mark>", marked)
    if not m:
        # ヒットなし → 先頭から
        out = plain[:max_chars]
        return html_escape(out) + ("…" if len(plain) > max_chars else "")

    # 近傍をテキスト長基準で切り出す
    # いったん <mark> をテキストに換算して位置を見積もる簡易アプローチ
    # → 近傍 side 前後＋… を付与
    pm = TAG_RE.sub("", marked[:m.start()])
    pos = len(pm)
    start = max(0, pos - side)
    end = min(len(plain), pos + side)
    snippet_text = plain[start:end]
    if start > 0:
        snippet_text = "…" + snippet_text
    if end < len(plain):
        snippet_text = snippet_text + "…"

    # snippet_text 内の語を再度ハイライト
    snippet_html = highlight(snippet_text, terms_for_hit)
    # 上限（±40 まで許容）
    if len(TAG_RE.sub("", snippet_html)) > max_chars + 40:
        # 生テキストで詰める
        t = TAG_RE.sub("", snippet_html)[:max_chars] + "…"
        snippet_html = html_escape(t)
    return snippet_html

# ======== エンドポイント ========
@APP.get("/health")
def health():
    ok = os.path.exists(KB_PATH)
    return {
        "ok": ok,
        "kb_url": KB_URL,
        "kb_size": KB_LINES,
        "kb_fingerprint": KB_HASH,
    }

@APP.get("/version")
def version():
    return {"version": VERSION}

@APP.get("/ui")
def ui():
    path = os.path.join("static", "ui.html")
    if os.path.exists(path):
        return FileResponse(path, media_type="text/html; charset=utf-8")
    return PlainTextResponse("static/ui.html not found", status_code=404)

def iter_records() -> Any:
    """kb.jsonl を1行ずつ辞書で返す。"""
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
    snippet_len = 300 if is_first_in_page else 160
    snippet = make_snippet(body, terms_for_hit, max_chars=snippet_len, side=80)
    return {
        "title": record_as_text(rec, "title") or "(無題)",
        "content": snippet,
        "url": record_as_text(rec, "url"),
        "rank": None,  # 後で付与
        "date": record_as_text(rec, "date"),
    }

@APP.get("/api/search")
def api_search(
    q: str = Query("", description="検索クエリ"),
    page: int = Query(1, ge=1),
    page_size: int = Query(5, ge=1, le=50),
    order: str = Query("relevance", regex="^(relevance|latest)$"),
):
    """
    - AND（語群ごとに1語以上ヒット）
    - relevance: スコア順、latest: 発行日降順
    - 1ページ目の先頭だけ約300字、残りは約160字
    """
    if not os.path.exists(KB_PATH):
        return JSONResponse(
            {"items": [], "total_hits": 0, "page": page, "page_size": page_size, "has_more": False, "next_page": None, "error": "kb_missing", "order_used": order},
            headers={"Cache-Control": "no-store"},
        )

    # 年フィルタを抽出
    q_wo_year, y_from, y_to = extract_year_filter(q)
    groups = expand_query_to_groups(q_wo_year)
    if not groups:
        return JSONResponse(
            {"items": [], "total_hits": 0, "page": page, "page_size": page_size, "has_more": False, "next_page": None, "error": None, "order_used": order},
            headers={"Cache-Control": "no-store"},
        )

    # 検索
    hits: List[Tuple[int, Optional[datetime], Dict[str, Any]]] = []  # (score, date, rec)
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

        score = compute_score(rec, groups)
        if score < 0:
            continue
        d = record_date(rec)
        hits.append((score, d, rec))

    total_hits = len(hits)

    # 並び順
    order_used = order
    if order == "latest":
        hits.sort(key=lambda x: (x[1] or datetime.min), reverse=True)
    else:
        # relevance
        hits.sort(key=lambda x: (x[0], x[1] or datetime.min), reverse=True)

    # ページング
    start = (page - 1) * page_size
    end = start + page_size
    page_hits = hits[start:end]
    has_more = end < total_hits
    next_page = page + 1 if has_more else None

    # 結果組み立て（先頭だけ300字）
    items: List[Dict[str, Any]] = []
    # ハイライト語（グループを単純化して配列化）
    terms_for_hit: List[str] = sorted({t for g in groups for t in g})
    for i, (_, _d, rec) in enumerate(page_hits):
        is_first = (i == 0)  # ★ ページ内の先頭のみ 300字
        item = build_item(rec, terms_for_hit, is_first_in_page=is_first)
        items.append(item)

    # rank（全体順位）を付ける
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


# ローカル開発用
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(APP, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
