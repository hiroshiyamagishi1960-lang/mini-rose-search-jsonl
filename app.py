# app.py — simple-search-2025-11-18-fallback
# 目的：
#   - ミニバラ盆栽デジタル資料館（JSONL版）の検索を
#     「日付順＋年フィルタ」で素直に動かすシンプル版。
#   - タイトル中の年はフィルタに使わず、発行日/開催日だけで年を判定。
#   - タイトル・本文・タグが空のレコードは「レコード丸ごと」を検索対象にして取りこぼしを防ぐ。
#   - UI や static ファイル構成は変更しない（/ui, /static/... は既存どおり）。

import os
import io
import re
import json
import unicodedata
import hashlib
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

from fastapi import FastAPI, Query, Request
from fastapi.responses import (
    JSONResponse,
    HTMLResponse,
    FileResponse,
    PlainTextResponse,
    RedirectResponse,
    Response,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

# ========= 設定 =========

KB_PATH = os.getenv("KB_PATH", "kb.jsonl")
VERSION = os.getenv("APP_VERSION", "jsonl-2025-11-18-simple-fallback")

PAGE_SIZE_DEFAULT = 5
FIRST_SNIPPET_LEN = 300
OTHER_SNIPPET_LEN = 185

app = FastAPI(title="mini-rose-search-jsonl (simple-fallback)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="static"), name="static")

# ========= KB 状態 =========

KB_ROWS: List[Dict[str, Any]] = []
KB_LINES: int = 0
KB_HASH: str = ""
LAST_ERROR: str = ""


# ========= 共通ユーティリティ =========

def _nfkc(s: Optional[str]) -> str:
    return unicodedata.normalize("NFKC", s or "")


def normalize_text(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("\u3000", " ")
    s = re.sub(r"[\r\n\t]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def textify(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, str):
        return x
    try:
        return json.dumps(x, ensure_ascii=False)
    except Exception:
        return str(x)


# かなフォールディング（カタカナ⇔ひらがな、小さいかな、長音「ー」などを吸収）

KATA_TO_HIRA = str.maketrans(
    {chr(k): chr(k - 0x60) for k in range(ord("ァ"), ord("ン") + 1)}
)
HIRA_SMALL2NORM = {
    "ぁ": "あ",
    "ぃ": "い",
    "ぅ": "う",
    "ぇ": "え",
    "ぉ": "お",
    "ゃ": "や",
    "ゅ": "ゆ",
    "ょ": "よ",
    "っ": "つ",
    "ゎ": "わ",
}
DAKUTEN = "\u3099"
HANDAKUTEN = "\u309A"
VOWELS = {"あ", "い", "う", "え", "お"}


def _strip_diacritics(hira: str) -> str:
    nfkd = unicodedata.normalize("NFD", hira)
    no_marks = "".join(ch for ch in nfkd if ch not in (DAKUTEN, HANDAKUTEN))
    return unicodedata.normalize("NFC", no_marks)


def _long_vowel_to_vowel(hira: str) -> str:
    out: List[str] = []
    prev = ""
    for ch in hira:
        if ch == "ー" and prev in VOWELS:
            out.append(prev)
        else:
            out.append(ch)
            prev = ch
    return "".join(out)


def fold_kana(s: str) -> str:
    if not s:
        return ""
    t = _nfkc(s)
    t = t.translate(KATA_TO_HIRA)
    t = "".join(HIRA_SMALL2NORM.get(ch, ch) for ch in t)
    t = _long_vowel_to_vowel(t)
    t = _strip_diacritics(t)
    return t


# ========= レコード → 文字列抽出 =========

TITLE_KEYS = ["title", "Title", "名前", "タイトル", "題名", "見出し", "subject", "headline"]
TEXT_KEYS = ["content", "text", "body", "本文", "内容", "記事", "description"]
DATE_KEYS = [
    "開催日/発行日",
    "date",
    "Date",
    "published_at",
    "published",
    "created_at",
    "更新日",
    "作成日",
    "日付",
    "開催日",
    "発行日",
]
URL_KEYS = ["url", "URL", "link", "permalink", "出典URL", "公開URL", "source"]
TAG_KEYS = ["tags", "tag", "タグ", "区分", "分類", "カテゴリ", "category", "keywords"]


def record_as_text(rec: Dict[str, Any], field: str) -> str:
    key_map = {
        "title": TITLE_KEYS,
        "text": TEXT_KEYS,
        "date": DATE_KEYS,
        "url": URL_KEYS,
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


# ========= 日付抽出（発行日/開催日 優先） =========

_DATE_RE = re.compile(
    r"(?P<y>(?:19|20|21)\d{2})[./\-年]?(?:(?P<m>0?[1-9]|1[0-2])[./\-月]?(?:(?P<d>0?[1-9]|[12]\d|3[01])日?)?)?",
    re.UNICODE,
)

_ERA_RE = re.compile(r"(令和|平成|昭和)\s*(\d{1,2})\s*年(?:\s*(\d{1,2})\s*月(?:\s*(\d{1,2})\s*日)?)?")


def _era_to_seireki(era: str, nen: int) -> int:
    base = {"令和": 2018, "平成": 1988, "昭和": 1925}.get(era)
    return base + nen if base is not None else nen


def _first_valid_date_from_string(s: str) -> Optional[datetime]:
    if not s:
        return None
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
    # 1) 開催日/発行日など、DATE_KEYS 優先
    for k in DATE_KEYS:
        v = rec.get(k)
        if not v:
            continue
        dt = _first_valid_date_from_string(textify(v))
        if dt:
            return dt

    # 2) 最後の手段として、本文/タイトル/URL の中の西暦を拾う（年だけ）
    cand_year: Optional[int] = None
    for field in ("text", "title", "url"):
        v = record_as_text(rec, field)
        for y in re.findall(r"(19\d{2}|20\d{2}|21\d{2})", _nfkc(v)):
            yy = int(y)
            cand_year = max(yy, cand_year or 0)
    if cand_year:
        return datetime(cand_year, 1, 1)

    return None


# ========= KB 読み込み =========

def _compute_lines_and_hash(path: str) -> Tuple[int, str]:
    cnt = 0
    sha = hashlib.sha256()
    with open(path, "rb") as f:
        for line in f:
            sha.update(line)
            if line.strip():
                cnt += 1
    return cnt, sha.hexdigest()


def _load_rows(path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not os.path.exists(path):
        return rows
    with io.open(path, "r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln:
                continue
            try:
                rows.append(json.loads(ln))
            except Exception:
                continue
    return rows


def _attach_precomputed_fields(rows: List[Dict[str, Any]]) -> None:
    for rec in rows:
        title = record_as_text(rec, "title")
        text = record_as_text(rec, "text")
        tags = record_as_tags(rec)

        ttl_norm = normalize_text(title)
        txt_norm = normalize_text(text)
        tag_norm = normalize_text(tags)

        # ★ 保険：
        # タイトル・本文・タグがすべて空の場合は、
        # レコード全体の JSON を「本文」として検索対象にする。
        if not ttl_norm and not txt_norm and not tag_norm:
            raw = textify(rec)
            txt_norm = normalize_text(raw)

        rec["__ttl_norm"] = ttl_norm
        rec["__txt_norm"] = txt_norm
        rec["__tag_norm"] = tag_norm

        rec["__ttl_fold"] = fold_kana(ttl_norm) if ttl_norm else ""
        rec["__txt_fold"] = fold_kana(txt_norm[:120000]) if txt_norm else ""
        rec["__tag_fold"] = fold_kana(tag_norm) if tag_norm else ""

        rec["__date_obj"] = record_date(rec)


def ensure_kb() -> None:
    global KB_ROWS, KB_LINES, KB_HASH, LAST_ERROR
    LAST_ERROR = ""
    if not os.path.exists(KB_PATH):
        KB_ROWS = []
        KB_LINES = 0
        KB_HASH = ""
        LAST_ERROR = f"kb_not_found:{KB_PATH}"
        return
    try:
        lines, sha = _compute_lines_and_hash(KB_PATH)
        rows = _load_rows(KB_PATH)
        _attach_precomputed_fields(rows)
        KB_ROWS = rows
        KB_LINES = lines
        KB_HASH = sha
    except Exception as e:
        KB_ROWS = []
        KB_LINES = 0
        KB_HASH = ""
        LAST_ERROR = f"kb_load_failed:{type(e).__name__}:{e}"


@app.on_event("startup")
def _startup() -> None:
    ensure_kb()


# ========= 共通レスポンス =========

def json_utf8(payload: Dict[str, Any], status: int = 200) -> JSONResponse:
    return JSONResponse(
        payload,
        status_code=status,
        media_type="application/json; charset=utf-8",
        headers={"Cache-Control": "no-store"},
    )


# ========= ルート / health / version / UI =========

@app.get("/")
def root_redirect():
    return RedirectResponse(url="/ui", status_code=302)


def _wants_html(request: Request) -> bool:
    q = request.query_params
    if q.get("view") in {"html", "1"} or q.get("html") == "1":
        return True
    accept = (request.headers.get("accept") or "").lower()
    return "text/html" in accept


def _html_page(title: str, inner: str) -> HTMLResponse:
    html = f"""<!doctype html><html lang="ja"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<style>
  :root{{--b:#e5e7eb;--txt:#0f172a}}
  body{{margin:0;padding:16px;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Noto Sans JP","Yu Gothic",Meiryo,sans-serif;color:var(--txt)}}
  .bar{{position:sticky;top:0;background:#fff;padding:10px 0;border-bottom:1px solid var(--b);margin-bottom:16px}}
  a.btn{{display:inline-block;padding:8px 12px;border:1px solid var(--b);border-radius:10px;text-decoration:none}}
  pre{{white-space:pre-wrap;word-break:break-word;background:#fafafa;border:1px solid var(--b);border-radius:8px;padding:12px}}
</style></head><body>
  <div class="bar"><a class="btn" href="/ui">← 検索画面に戻る</a></div>
  {inner}
</body></html>"""
    return HTMLResponse(html)


@app.get("/health")
def health(request: Request):
    payload = {
        "ok": bool(KB_ROWS),
        "kb_path": KB_PATH,
        "kb_size": KB_LINES,
        "kb_fingerprint": KB_HASH,
        "last_error": LAST_ERROR,
        "version": VERSION,
    }
    if _wants_html(request):
        return _html_page("Health", f"<h1>Health</h1><pre>{json.dumps(payload, ensure_ascii=False, indent=2)}</pre>")
    return json_utf8(payload)


@app.get("/version")
def version(request: Request):
    payload = {"version": VERSION}
    if _wants_html(request):
        return _html_page("Version", f"<h1>Version</h1><pre>{json.dumps(payload, ensure_ascii=False, indent=2)}</pre>")
    return json_utf8(payload)


@app.get("/admin/refresh")
def admin_refresh():
    ensure_kb()
    ok = bool(KB_ROWS)
    return json_utf8(
        {
            "ok": ok,
            "kb_size": KB_LINES,
            "kb_fingerprint": KB_HASH,
            "last_error": LAST_ERROR,
        }
    )


@app.get("/service-worker.js")
def get_sw():
    sw_path = os.path.join("static", "service-worker.js")
    if not os.path.exists(sw_path):
        return Response("service-worker.js not found under /static/", status_code=404)
    return FileResponse(
        sw_path,
        media_type="application/javascript; charset=utf-8",
        headers={"Cache-Control": "no-cache, must-revalidate", "Service-Worker-Allowed": "/"},
    )


@app.get("/ui")
def ui():
    path = os.path.join("static", "ui.html")
    if os.path.exists(path):
        return FileResponse(path, media_type="text/html; charset=utf-8")
    return PlainTextResponse("static/ui.html not found", status_code=404)


# ========= 検索クエリ処理（年フィルタを含む） =========

RANGE_SEP = r"(?:-|–|—|~|〜|～|\.{2})"


def _parse_year_from_query(q_raw: str) -> Tuple[str, Optional[int], Optional[Tuple[int, int]]]:
    """
    末尾の「西暦4桁」または「西暦4桁-西暦4桁」を年フィルタとして解釈する。

    対応パターン:
      - コンテスト 2024
      - コンテスト2024
      - 剪定 1999-2001
      - 剪定1999-2001
    """
    q = _nfkc(q_raw).strip()
    if not q:
        return "", None, None

    # 全角スペースも半角に統一
    q = q.replace("　", " ")

    parts = q.split()
    last = parts[-1] if parts else ""

    # 「語＋年」がくっついている場合をばらす（コンテスト2024 → コンテスト + 2024）
    m_suffix = re.fullmatch(rf"(.+?)((?:19|20|21)\d{{2}}(?:\s*{RANGE_SEP}\s*(?:19|20|21)\d{{2}})?)", last)
    if m_suffix:
        head = m_suffix.group(1)
        tail = m_suffix.group(2)
        if head:
            parts[-1] = head
            parts.append(tail)
        last = tail

    # 4桁西暦だけ
    if re.fullmatch(r"(?:19|20|21)\d{2}", last):
        base = " ".join(parts[:-1]).strip()
        return base, int(last), None

    # 1999-2001 のような範囲
    m_rng = re.fullmatch(
        rf"((?:19|20|21)\d{{2}})\s*{RANGE_SEP}\s*((?:19|20|21)\d{{2}})",
        last,
    )
    if m_rng:
        y1, y2 = int(m_rng.group(1)), int(m_rng.group(2))
        if y1 > y2:
            y1, y2 = y2, y1
        base = " ".join(parts[:-1]).strip()
        return base, None, (y1, y2)

    # 語尾に年がくっついているパターン（剪定1999 など）をもう一度チェック
    m_tail = re.fullmatch(rf"^(.*?)(?:((?:19|20|21)\d{{2}}))$", last)
    if m_tail and m_tail.group(1):
        base_parts = parts[:-1] + [m_tail.group(1)]
        base = " ".join(base_parts).strip()
        return base, int(m_tail.group(2)), None

    # 年指定なし
    return q, None, None


TOKEN_RE = re.compile(r'"([^"]+)"|(\S+)')


def parse_query(q: str) -> Tuple[List[str], List[str], List[str]]:
    must: List[str] = []
    minus: List[str] = []
    raw: List[str] = []
    for m in TOKEN_RE.finditer(normalize_text(q)):
        tok = m.group(1) if m.group(1) is not None else m.group(2)
        if not tok:
            continue
        raw.append(tok)
        if tok.startswith("-") and len(tok) > 1:
            minus.append(tok[1:])
        else:
            must.append(tok)
    return must, minus, raw


def _matches_year(rec: Dict[str, Any], year: Optional[int], year_range: Optional[Tuple[int, int]]) -> bool:
    """
    年フィルタは「発行日/開催日などの正式な日付(__date_obj)」だけを見る。
    タイトル・本文・URL に書いてある西暦は完全に無視する。
    """
    if year is None and year_range is None:
        return True

    d = rec.get("__date_obj")
    if d is None:
        d = record_date(rec)
        rec["__date_obj"] = d

    if d is None:
        # 年指定付き検索では、年が分からない記事は対象外
        return False

    y = d.year

    if year is not None:
        return y == year

    lo, hi = year_range
    return lo <= y <= hi


def html_escape(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def highlight_simple(text: str, terms: List[str]) -> str:
    if not text:
        return ""
    esc = html_escape(text)
    if not terms:
        return esc

    norm_terms = [normalize_text(t) for t in terms if normalize_text(t)]
    norm_terms = sorted(set(norm_terms), key=len, reverse=True)

    for t in norm_terms:
        pattern = re.escape(html_escape(t))
        esc = re.sub(pattern, lambda m: f"<mark>{m.group(0)}</mark>", esc)
    return esc


def build_item(
    rec: Dict[str, Any],
    terms: List[str],
    is_first_in_page: bool,
    matches: Optional[Dict[str, List[str]]] = None,
) -> Dict[str, Any]:
    title = record_as_text(rec, "title") or "(無題)"
    body = record_as_text(rec, "text") or ""

    if is_first_in_page:
        if len(body) <= FIRST_SNIPPET_LEN:
            snippet_src = body
        else:
            snippet_src = body[:FIRST_SNIPPET_LEN] + "…"
    else:
        if len(body) <= OTHER_SNIPPET_LEN:
            snippet_src = body
        else:
            snippet_src = body[:OTHER_SNIPPET_LEN] + "…"

    item: Dict[str, Any] = {
        "title": highlight_simple(title, terms),
        "content": highlight_simple(snippet_src, terms),
        "url": record_as_text(rec, "url"),
        "date": record_as_text(rec, "date"),
        "rank": None,
    }
    if matches:
        item["matches"] = matches
    return item


def _calc_matches_for_debug(rec: Dict[str, Any], terms: List[str]) -> Dict[str, List[str]]:
    ttl = rec.get("__ttl_norm", "")
    txt = rec.get("__txt_norm", "")
    tag = rec.get("__tag_norm", "")
    hit_ttl: List[str] = []
    hit_tag: List[str] = []
    hit_txt: List[str] = []
    for t in terms:
        nt = normalize_text(t)
        if not nt:
            continue
        if nt in ttl:
            hit_ttl.append(nt)
        if nt in tag:
            hit_tag.append(nt)
        if nt in txt:
            hit_txt.append(nt)
    out: Dict[str, List[str]] = {}
    if hit_ttl:
        out["title"] = hit_ttl
    if hit_tag:
        out["tags"] = hit_tag
    if hit_txt:
        out["body"] = hit_txt
    return out


# ========= /api/search 本体 =========

@app.get("/api/search")
def api_search(
    q: str = Query("", description="検索クエリ（-語=除外、末尾年/範囲はフィルタ）"),
    page: int = Query(1, ge=1),
    page_size: int = Query(PAGE_SIZE_DEFAULT, ge=1, le=50),
    order: str = Query("latest", description="latest 固定（互換用）"),
    debug: int = Query(0, description="1でヒット内訳を返す（診断用）"),
):
    if not KB_ROWS:
        return json_utf8(
            {
                "items": [],
                "total_hits": 0,
                "page": page,
                "page_size": page_size,
                "has_more": False,
                "next_page": None,
                "error": "kb_not_loaded",
                "order_used": "latest",
            },
            status=503,
        )

    base_q, year, year_range = _parse_year_from_query(q)
    must_terms, minus_terms, raw_terms = parse_query(base_q)

    if not must_terms and not minus_terms:
        return json_utf8(
            {
                "items": [],
                "total_hits": 0,
                "page": page,
                "page_size": page_size,
                "has_more": False,
                "next_page": None,
                "error": None,
                "order_used": "latest",
            }
        )

    # --- 年フィルタ（発行日/開催日だけを見る） ---
    candidates: List[Dict[str, Any]] = []
    for rec in KB_ROWS:
        if year is not None or year_range is not None:
            if not _matches_year(rec, year, year_range):
                continue
        candidates.append(rec)

    if not candidates:
        return json_utf8(
            {
                "items": [],
                "total_hits": 0,
                "page": page,
                "page_size": page_size,
                "has_more": False,
                "next_page": None,
                "error": None,
                "order_used": "latest",
            }
        )

    # --- キーワードフィルタ（AND／除外語） ---
    filtered: List[Dict[str, Any]] = []
    for rec in candidates:
        ttl = rec.get("__ttl_norm", "")
        txt = rec.get("__txt_norm", "")
        tag = rec.get("__tag_norm", "")
        fttl = rec.get("__ttl_fold", "")
        ftxt = rec.get("__txt_fold", "")
        ftag = rec.get("__tag_fold", "")

        def contains_any(term: str) -> bool:
            nt = normalize_text(term)
            if not nt:
                return False
            if nt in ttl or nt in txt or nt in tag:
                return True
            fn = fold_kana(nt)
            if fn and (fn in fttl or fn in ftxt or fn in ftag):
                return True
            return False

        # 除外語
        if minus_terms and any(contains_any(t) for t in minus_terms):
            continue

        # AND 条件（すべての must_terms がどこかに入っている）
        ok = True
        for t in must_terms:
            if not contains_any(t):
                ok = False
                break
        if ok:
            filtered.append(rec)

    if not filtered:
        return json_utf8(
            {
                "items": [],
                "total_hits": 0,
                "page": page,
                "page_size": page_size,
                "has_more": False,
                "next_page": None,
                "error": None,
                "order_used": "latest",
            }
        )

    # --- スコア計算（軽いもの：タイトル>タグ>本文） ---
    scored: List[Tuple[int, datetime, str, Dict[str, Any]]] = []
    terms = must_terms or raw_terms

    for rec in filtered:
        ttl = rec.get("__ttl_norm", "")
        txt = rec.get("__txt_norm", "")
        tag = rec.get("__tag_norm", "")

        score = 0
        for t in terms:
            nt = normalize_text(t)
            if not nt:
                continue
            if nt in ttl:
                score += 3 * ttl.count(nt)
            if nt in tag:
                score += 2 * tag.count(nt)
            if nt in txt:
                score += 1 * txt.count(nt)

        d = rec.get("__date_obj") or record_date(rec) or datetime(1900, 1, 1)
        rec["__date_obj"] = d
        did = hashlib.sha256((record_as_text(rec, "title") or "").encode("utf-8")).hexdigest()[:16]
        scored.append((score, d, did, rec))

    # --- 並べ方：日付降順 → スコア降順 ---
    scored.sort(key=lambda x: (-int(x[1].strftime("%Y%m%d%H%M%S")), -x[0], x[2]))

    total = len(scored)
    start = (page - 1) * page_size
    end = start + page_size
    page_slice = scored[start:end]
    has_more = end < total
    next_page = page + 1 if has_more else None

    items: List[Dict[str, Any]] = []
    for idx, (score, d, did, rec) in enumerate(page_slice, start=start + 1):
        matches = _calc_matches_for_debug(rec, terms) if debug == 1 else None
        items.append(build_item(rec, terms, is_first_in_page=(idx == start + 1), matches=matches))
        items[-1]["rank"] = idx

    payload = {
        "items": items,
        "total_hits": total,
        "page": page,
        "page_size": page_size,
        "has_more": has_more,
        "next_page": next_page,
        "error": None,
        "order_used": "latest",
    }
    return json_utf8(payload)


# ========= エントリポイント（ローカル実行用） =========

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
