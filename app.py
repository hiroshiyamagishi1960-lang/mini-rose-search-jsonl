# app.py — Mini Rose Search API
# 方針反映版：日本語短語ファジー抑止 / 代表日=開催日/発行日 / order=latest でページング前ソート / UI変更なし
# 版: ui-2025-10-06-best-practice + jsonl-fallback-2025-10-14

import os, re, json, unicodedata, datetime as dt, hashlib, io, time
from typing import List, Dict, Any, Tuple, Optional
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, PlainTextResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import requests
from urllib.parse import urlparse, urlunparse
import httpx
# === ここから追記（app.pyのimportsの下あたり） ==========================
import os, time, json
from typing import Optional
from fastapi import APIRouter
import httpx

DIAG = {
    "kb_url": os.getenv("KB_URL", "").strip(),
    "has_kb": False,
    "kb_size": 0,
    "loaded_at": None,
    "last_error": None,
    "etag": None,
}

_kb_lines_cache: Optional[list[str]] = None
router_diag = APIRouter()

async def _fetch_kb_text(url: str) -> str:
    # 生RAWを確実に取得するための最小実装
    headers = {
        "User-Agent": "mini-rose-search-jsonl/diag",
        "Accept": "text/plain,*/*",
    }
    timeout = httpx.Timeout(20.0, connect=10.0)
    async with httpx.AsyncClient(follow_redirects=True, timeout=timeout) as client:
        r = await client.get(url, headers=headers)
        r.raise_for_status()
        # ETag等（あれば）保存
        DIAG["etag"] = r.headers.get("ETag")
        return r.text

async def _load_kb(force: bool = False) -> None:
    global _kb_lines_cache
    try:
        if not DIAG["kb_url"]:
            raise RuntimeError("KB_URL is empty")
        if _kb_lines_cache is not None and not force:
            # 既に読み込み済み
            return
        text = await _fetch_kb_text(DIAG["kb_url"])
        lines = [ln for ln in text.splitlines() if ln.strip()]
        _kb_lines_cache = lines
        DIAG["kb_size"] = len(lines)
        DIAG["has_kb"] = len(lines) > 0
        DIAG["loaded_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        DIAG["last_error"] = None
    except Exception as e:
        DIAG["has_kb"] = False
        DIAG["kb_size"] = 0
        DIAG["last_error"] = repr(e)

@router_diag.get("/kb/status")
async def kb_status():
    await _load_kb(force=False)
    return DIAG

@router_diag.post("/kb/reload")
async def kb_reload():
    await _load_kb(force=True)
    return DIAG

@router_diag.get("/health2")
async def health2():
    await _load_kb(force=False)
    return {"ok": True, "has_kb": DIAG["has_kb"], "kb_size": DIAG["kb_size"], "kb_url": DIAG["kb_url"]}

@router_diag.get("/diag2")
async def diag2():
    await _load_kb(force=False)
    return {"version_hint": "jsonl-diag2", **DIAG}

# FastAPI本体にマウント（既存 app 変数がある前提）
try:
    app.include_router(router_diag)
except Exception:
    # app が未定義のタイミングなら後で include してもOK
    pass
# === 追記ここまで =========================================================
# ==== 環境変数（Notion：あれば優先。無ければJSONLフォールバック） ====
NOTION_TOKEN       = os.getenv("NOTION_TOKEN", "")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID", "")

# JSONL（kb版）
KB_URL  = os.getenv("KB_URL", "").strip()
KB_PATH = os.getenv("KB_PATH", "kb.jsonl").strip() or "kb.jsonl"   # ローカル保存名

# （任意）他フィールド
FIELD_TITLE  = os.getenv("FIELD_TITLE",  "")
FIELD_ISSUE  = os.getenv("FIELD_ISSUE",  "")
FIELD_AUTHOR = os.getenv("FIELD_AUTHOR", "")
FIELD_URL    = os.getenv("FIELD_URL", "出典URL")
FIELD_TEXT   = os.getenv("FIELD_TEXT",   "")

# 公開 Notion サブドメイン（必要なら上書き）
NOTION_PUBLIC_HOST = os.getenv("NOTION_PUBLIC_HOST", "receptive-paste-be4.notion.site")

JSON_HEADERS = {"content-type": "application/json; charset=utf-8"}

app = FastAPI(title="Mini Rose Search API",
              version="ui-2025-10-06-best-practice+jsonl")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"], allow_credentials=True
)

# ==== UI配信（/ui は毎回最新：no-store）====
if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/ui", response_class=HTMLResponse)
def ui():
    path = "static/ui.html"
    if os.path.isfile(path):
        with open(path, "r", encoding="utf-8") as f:
            html = f.read()
        return HTMLResponse(html, headers={"Cache-Control": "no-store"})
    return HTMLResponse("<h1>Not Found</h1>", status_code=404, headers={"Cache-Control": "no-store"})

# =============================
# かなフォールディング
# =============================
def _nfkc(s: Optional[str]) -> str:
    return unicodedata.normalize("NFKC", s or "")

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

def _kana_to_hira(s: str) -> str:
    out = []
    for ch in s:
        code = ord(ch)
        if 0x30A1 <= code <= 0x30F6:
            out.append(chr(code - 0x60))
        elif ch in ("ヵ","ヶ"):
            out.append({"ヵ":"か","ヶ":"け"}[ch])
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
    if not s: return ""
    s = unicodedata.normalize("NFKC", s)
    s = _kana_to_hira(s)
    s = s.translate(_SMALL_TO_BASE)
    buf = []
    for ch in s:
        if ch == "ー":
            buf.append(_long_to_vowel(buf[-1] if buf else ""))
        else:
            buf.append(ch)
    s = "".join(buf)
    d = unicodedata.normalize("NFD", s)
    d = "".join(c for c in d if ord(c) not in (0x3099, 0x309A))
    s = unicodedata.normalize("NFC", d)
    return s.lower().strip()

def hira_to_kata(s: str) -> str:
    out = []
    for ch in s:
        code = ord(ch)
        if 0x3041 <= code <= 0x3096:
            out.append(chr(code + 0x60))
        elif ch in ("ゕ","ゖ"):
            out.append({"ゕ":"ヵ","ゖ":"ヶ"}[ch])
        else:
            out.append(ch)
    return "".join(out)

# =============================
# 同義語（小辞書）
# =============================
KANJI_EQ: Dict[str, set] = {
    "苔": {"こけ", "コケ", "ごけ", "ゴケ"},
    "剪定": {"せん定", "せんてい"},
    "施肥": {"肥料", "追肥"},
    "用土": {"土", "土の配合"},
    "挿し木": {"さし木", "さし芽"},
    "接ぎ木": {"つぎ木", "つぎき"},
    "植え替え": {"うえ替え", "うえかえ"},
    "黒星病": {"クロボシ", "黒点病"},
    "うどんこ病": {"ウドンコ"},
    "アブラムシ": {"アブラムシ類"},
    "文人木": {"ぶんじん木"},
    "小品盆栽": {"しょうひん盆栽"},
    "枝枯れ": {"枝がれ"},
    "薔薇": {"バラ", "ばら", "薔薇(バラ)"},
    "ミニバラ": {"ミニ薔薇", "みにばら"},
}
REVERSE_EQ: Dict[str, set] = {}
for canon, vars_ in KANJI_EQ.items():
    for v in vars_:
        REVERSE_EQ.setdefault(v, set()).add(canon)
    REVERSE_EQ.setdefault(canon, set()).add(canon)

def expand_with_domain_dict(term: str) -> set:
    out = set()
    if term in KANJI_EQ:
        out.add(term)
        out |= KANJI_EQ[term]
    if term in REVERSE_EQ:
        out |= REVERSE_EQ[term]
        for c in REVERSE_EQ[term]:
            out |= KANJI_EQ.get(c, set())
    return out

# =============================
# 軽量ファジー（編集距離 ≤1）— 日本語短語の暴発抑止
# =============================
_ASCII_ONLY = re.compile(r'^[\x20-\x7E]+$')  # 半角英数記号のみ

def levenshtein_le1(a: str, b: str) -> bool:
    if a == b:
        return True
    la, lb = len(a), len(b)
    if abs(la - lb) > 1:
        return False
    i = j = diff = 0
    while i < la and j < lb:
        if a[i] == b[j]:
            i += 1; j += 1
        else:
            diff += 1
            if diff > 1:
                return False
            if la == lb:
                i += 1; j += 1
            elif la > lb:
                i += 1
            else:
                j += 1
    diff += (la - i) + (lb - j)
    return diff <= 1

def fuzzy_substring_match(term: str, text: str) -> bool:
    """英語の軽い綴り揺れのみ救済。非ASCII or 短語(<3)は無効化。"""
    if not term or not text:
        return False
    if not _ASCII_ONLY.match(term) or len(term) < 3:
        return False
    L = len(term)
    t = text[:4000]
    for w in (L-1, L, L+1):
        if w < 3:     # 1〜2文字窓は見ない（暴発防止）
            continue
        for i in range(0, max(0, len(t)-w+1)):
            if levenshtein_le1(term, t[i:i+w]):
                return True
    return False

# =============================
# Notion ユーティリティ
# =============================
def _get_rich_text(prop: Dict[str, Any]) -> str:
    return "".join([b.get("plain_text","") for b in prop.get("rich_text", [])]).strip()

def _get_title(prop: Dict[str, Any]) -> str:
    return "".join([a.get("plain_text","") for a in prop.get("title", [])]).strip()

def _get_text_from_property(p: Dict[str, Any]) -> str:
    if "rich_text" in p:      return _get_rich_text(p)
    if "title" in p:          return _get_title(p)
    if "date" in p and p["date"]: return p["date"].get("start","")
    if "url" in p:            return p.get("url") or ""
    if "number" in p:         return str(p.get("number") or "")
    if "select" in p and p["select"]: return p["select"].get("name","")
    if "multi_select" in p:   return ", ".join([x.get("name","") for x in p["multi_select"]])
    if "people" in p:         return ", ".join([x.get("name","") for x in p["people"]])
    if "email" in p:          return p.get("email") or ""
    if "phone_number" in p:   return p.get("phone_number") or ""
    if "checkbox" in p:       return "true" if p.get("checkbox") else "false"
    return ""

def _as_str(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, (list, tuple)):
        return _as_str(x[0]) if x else ""
    try:
        return str(x)
    except Exception:
        return ""

def _url_key_candidates() -> List[str]:
    cand: List[str] = []
    raw = (FIELD_URL or "").strip()
    if raw:
        if raw.startswith("[") and raw.endswith("]"):
            try:
                arr = json.loads(raw)
                for x in arr:
                    if isinstance(x, str) and x.strip():
                        cand.append(x.strip())
            except Exception:
                cand.append(raw)
        elif "," in raw:
            cand += [s.strip() for s in raw.split(",") if s.strip()]
        else:
            cand.append(raw)
    cand += ["出典URL", "外部URL", "URL", "リンク", "Link"]
    seen=set(); out=[]
    for x in cand:
        if isinstance(x, str) and x and x not in seen:
            seen.add(x); out.append(x)
    return out

# ===== Notion内部URL → 外部公開URLへ変換 =====
_UUID_COMP = re.compile(r"([0-9a-f]{32}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})", re.I)

def _normalize_uuid(u: str) -> str:
    u = (u or "").replace("-", "").lower()
    if len(u) != 32 or not re.fullmatch(r"[0-9a-f]{32}", u):
        return ""
    return f"{u[0:8]}-{u[8:12]}-{u[12:16]}-{u[16:20]}-{u[20:32]}"

def notion_internal_to_public(url: str) -> str:
    """Notion内部URLを公開URLに変換。公開されていないページは404。"""
    if not url:
        return url
    if ".notion.site" in url:
        return url
    if "notion.so" in url:
        m = _UUID_COMP.search(url)
        if m:
            pid = _normalize_uuid(m.group(1))
            if pid:
                return f"https://{NOTION_PUBLIC_HOST}/{pid}"
    return url

def _clean_public_url(u: str) -> str:
    if not u: return ""
    u = _as_str(u).strip()
    if not u: return ""
    try:
        p = urlparse(u)
        base = urlunparse((p.scheme, p.netloc, p.path, "", "", ""))
    except Exception:
        base = u
    # Notion内部リンクなら公開URLへ変換
    base = notion_internal_to_public(base)
    return base

def _extract_url_only(props: Dict[str, Any], page: Dict[str, Any]) -> str:
    for k in _url_key_candidates():
        if k in props:
            v = _get_text_from_property(props[k]).strip()
            if v:
                return _clean_public_url(v)
    return ""

def _extract_by_keys(props: Dict[str, Any], keys: List[str], *, fallback_title: bool=False) -> str:
    for k in keys:
        if k and k in props:
            val = _get_text_from_property(props[k])
            if val:
                return val
    if fallback_title:
        for _, p in props.items():
            if isinstance(p, dict) and "title" in p:
                t = _get_title(p)
                if t: return t
    return ""

def _uniq_keep_order(seq: List[str]) -> List[str]:
    seen=set(); out=[]
    for s in seq:
        if not s: continue
        if s not in seen:
            seen.add(s); out.append(s)
    return out

# ---- 代表日キー（app.py固定運用）----
def primary_date_keys() -> List[str]:
    """「開催日/発行日」運用。表記ゆれ（全角/半角/・）も吸収。"""
    cand = ["開催日/発行日", "開催日／発行日", "開催日・発行日"]
    seen=set(); out=[]
    for x in cand:
        if x and x not in seen:
            seen.add(x); out.append(x)
    return out

def _make_head(r: Dict[str,str]) -> str:
    parts = _uniq_keep_order([
        r.get("issue",""),
        r.get("date_primary",""),
        r.get("author",""),
        r.get("title",""),
    ])
    return "／".join(parts)

def _highlight_terms(text: str, terms_all: List[str]) -> str:
    if not text or not terms_all: return text or ""
    ts = sorted(set([t for t in terms_all if t]), key=len, reverse=True)
    pat = "|".join(re.escape(t) for t in ts)
    try:
        return re.sub(pat, lambda m:f"<mark>{m.group(0)}</mark>", text, flags=re.IGNORECASE)
    except re.error:
        return text

def _text_hit_any(text: str, terms: List[str]) -> bool:
    if not text or not terms:
        return False
    t_low = (text or "").lower()
    t_fold = fold_kana(text or "")
    for term in terms:
        if not term: continue
        if term.lower() in t_low: return True
        if fold_kana(term) in t_fold: return True
    return False

# =============================
# ★ ここが不足していた関数：UI表示用にタイトル/本文を整形 ★
# =============================
def _apply_head_and_excerpt(r: Dict[str, str],
                            head_hl: str,
                            text_raw: str,
                            is_first_in_page: bool,
                            hl_terms: List[str]) -> Dict[str, Any]:
    """
    1件目は本文抜粋（ハイライト付）を返す。2件目以降は「（本文にヒット）」タグのみ。
    既存UIに合わせて title/content に詰め替える直前の中間整形。
    """
    title = r.get("title", "")
    out = dict(r)
    if is_first_in_page:
        out["title"] = f"{head_hl or title}"
        out["text"]  = _highlight_terms(text_raw, hl_terms) + "<br><br>"
    else:
        hits_in_head = (head_hl != _make_head(r))
        text_hit = _text_hit_any(text_raw, hl_terms)
        tag = "" if hits_in_head else ("（本文にヒット）」"[:-1] if text_hit else "")
        out["title"] = f"{head_hl}{tag}" if head_hl else f"{title}{tag}"
        out["text"]  = ""
    return out

# =============================
# Notion 全取得（Notionが設定されていれば使用）
# =============================
NOTION_VER = "2022-06-28"
def notion_query_all() -> List[Dict[str, Any]]:
    if not (NOTION_TOKEN and NOTION_DATABASE_ID):
        return []
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VER,
        "Content-Type": "application/json; charset=utf-8",
    }
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
    pages, next_cursor = [], None
    while True:
        payload = {"page_size": 100}
        if next_cursor: payload["start_cursor"] = next_cursor
        r = requests.post(url, headers=headers, data=json.dumps(payload))
        if r.status_code != 200: break
        js = r.json()
        pages.extend(js.get("results", []))
        if js.get("has_more"): next_cursor = js.get("next_cursor")
        else: break
    return pages

# ---- プロパティ抽出（Notion）----
def _extract_properties(page: Dict[str, Any]) -> Dict[str, str]:
    props = page.get("properties", {})
    return {
        "title": _extract_by_keys(props, [FIELD_TITLE,"Title","Name","タイトル","名前"], fallback_title=True),
        "issue": _extract_by_keys(props, [FIELD_ISSUE,"出典","会報号"]),
        "date_primary": _extract_by_keys(props, primary_date_keys()),  # 代表日は単一プロパティ
        "author": _extract_by_keys(props, [FIELD_AUTHOR,"講師","著者"]),
        "url": _extract_url_only(props, page),   # 外部URL優先・内部→外部変換あり
        "text": _extract_by_keys(props, [
            FIELD_TEXT,
            "講習会等内容","講義等内容","本文","内容","本文テキスト",
        ]),
        "category": _extract_by_keys(props, ["資料区分"]),
        # Notionメタ
        "last_edited_time": page.get("last_edited_time",""),
        "created_time": page.get("created_time",""),
    }

# =============================
# JSONL（kb.jsonl）ローダ
# =============================
_kb_cache: Dict[str, Any] = {
    "sha256": "",
    "lines": 0,
    "loaded_at": 0.0,
    "records": [],   # List[Dict[str, str]]
}

def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def _download_kb_if_needed() -> None:
    """KB_URLがあればダウンロード。なければローカルKB_PATHだけ参照。"""
    if KB_URL:
        try:
            r = requests.get(KB_URL, timeout=20)
            if r.status_code == 200 and r.text:
                with open(KB_PATH, "w", encoding="utf-8") as f:
                    f.write(r.text)
        except Exception:
            pass  # ネットワーク一時失敗は無視（手元のKB_PATHをそのまま使う）

def load_kb(force: bool=False) -> Tuple[List[Dict[str,str]], str, int]:
    """
    kb.jsonl を読み込み（必要なら KB_URL から取得）、records を返す。
    返り値：（records, sha256, lines）
    """
    # 1) 取得（必要なら）
    _download_kb_if_needed()

    # 2) 変更検知（sha256）
    sha = ""
    lines = 0
    try:
        sha = _sha256_file(KB_PATH) if os.path.exists(KB_PATH) else ""
    except Exception:
        sha = ""

    if (not force) and sha and sha == _kb_cache.get("sha256",""):
        return _kb_cache["records"], _kb_cache["sha256"], _kb_cache["lines"]

    # 3) 読み込み
    records: List[Dict[str,str]] = []
    if os.path.exists(KB_PATH):
        with io.open(KB_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line: continue
                try:
                    rec = json.loads(line)
                    # refresh_kb.py の出力キーに合わせる（最低限）
                    m = {
                        "title": rec.get("title","") or "",
                        "text":  rec.get("text","") or "",
                        "url":   rec.get("url","") or "",
                        "author": rec.get("author","") or "",
                        "issue": rec.get("issue","") or "",
                        "date_primary": rec.get("date_primary","") or "",
                        "category": rec.get("category","") or "",
                    }
                    if (m["title"] or m["text"]):
                        records.append(m)
                except Exception:
                    continue
    lines = len(records)

    # 4) キャッシュ更新
    _kb_cache.update({
        "sha256": sha,
        "lines": lines,
        "loaded_at": time.time(),
        "records": records
    })
    return records, sha, lines

# =============================
# 検索ロジック + 年/範囲フィルタ（共通）
# =============================
W_TITLE, W_TEXT, W_AUTHOR, W_ISSUE, W_DATES, W_CATEGORY = 2.0, 1.6, 0.8, 0.6, 0.4, 0.5
BONUS_PHRASE = 2.0
BONUS_FUZZY  = 0.6
PENALTY_GIJIROKU = 1.0

def normalize(q: str) -> str:
    repl = [("接木","接ぎ木"),("つぎ木","接ぎ木"),("土作り","土の作り方"),("土づくり","土の作り方")]
    s = q
    for a,b in repl: s = s.replace(a,b)
    return s.strip()

_JP_WORDS = re.compile(r"[一-龥ぁ-んァ-ンー]{2,}|[A-Za-z0-9]{2,}")

def jp_terms(q: str) -> List[str]:
    if not q: return []
    qn = normalize(q).replace("　"," ")
    toks = [t for t in qn.split() if t]
    toks += _JP_WORDS.findall(qn)
    if not toks and len(qn)==1: toks=[qn]
    uniq=[]
    for t in sorted(set(toks), key=len, reverse=True):
        if len(uniq)>=5: break
        uniq.append(t)
    return uniq

def expand_terms_with_fold_and_dict(terms: List[str]) -> List[str]:
    out=set()
    for t in terms:
        out.add(t)
        out.add(fold_kana(t))
        out |= expand_with_domain_dict(t)
    return [x for x in sorted(out) if x]

def make_highlight_terms(base_terms: List[str]) -> List[str]:
    hs = set()
    for t in base_terms:
        if not t: 
            continue
        hs.add(t)
        f = fold_kana(t)
        if f:
            hs.add(f)
            hs.add(hira_to_kata(f))
        hs |= expand_with_domain_dict(t)
    return sorted(hs, key=len, reverse=True)

def _parse_date_any(s: str) -> Optional[dt.date]:
    if not s: return None
    s = _nfkc(s).strip()
    for fmt in ("%Y-%m-%d","%Y/%m/%d"):
        try: return dt.datetime.strptime(s, fmt).date()
        except: pass
    m = re.search(r"(\d{4})\D(\d{1,2})\D(\d{1,2})", s)
    if m:
        try: return dt.date(int(m.group(1)),int(m.group(2)),int(m.group(3)))
        except: return None
    m = re.search(r"(\d{4})年", s)
    if m:
        try: return dt.date(int(m.group(1)), 1, 1)
        except: return None
    return None

def _years_from_text(s: str) -> List[int]:
    s = _nfkc(s)
    ys = [int(y) for y in re.findall(r"\b(19\d{2}|20\d{2}|21\d{2})\b", s)]
    return list(sorted(set(ys)))

def _record_years(rec: Dict[str,str]) -> List[int]:
    ys=set()
    d=_parse_date_any(rec.get("date_primary",""))
    if d:
        ys.add(d.year)
    ys.update(_years_from_text(rec.get("issue","")))
    ys.update(_years_from_text(rec.get("title","")))
    ys.update(_years_from_text(rec.get("text","")))
    ys.update(_years_from_text(rec.get("url","")))
    return sorted(ys)

# ---- 代表日（best date）ユーティリティ ----
def _best_date(rec: Dict[str, Any]) -> Tuple[dt.date, Dict[str, Any]]:
    dpri = _parse_date_any(rec.get("date_primary",""))
    if dpri:
        return dpri, {"kind":"date_primary", "value": rec.get("date_primary","")}

    def _iso_to_date(s: str) -> Optional[dt.date]:
        if not s: return None
        try: return dt.datetime.fromisoformat(s.replace("Z","+00:00")).date()
        except: return None

    le = _iso_to_date(rec.get("last_edited_time",""))
    if le: return le, {"kind":"last_edited_time", "value": rec.get("last_edited_time","")}

    cr = _iso_to_date(rec.get("created_time",""))
    if cr: return cr, {"kind":"created_time", "value": rec.get("created_time","")}

    years = _record_years(rec)
    if years:
        y = max(years)
        return dt.date(y,1,1), {"kind":"year_inferred", "value": y, "years_inferred": years}

    return dt.date(1970,1,1), {"kind":"fallback_1970", "value":"1970-01-01"}

_RANGE_SEP = r"(?:-|–|—|~|〜|～|\.{2})"

def _parse_year_from_query(q_raw: str) -> Tuple[str, Optional[int], Optional[int]]:
    q_n = _nfkc((q_raw or "")).strip()
    if not q_n:
        return "", None, None
    parts = q_n.replace("　"," ").split()
    last = parts[-1] if parts else ""
    m1 = re.fullmatch(r"(19|20|21)\d{2}", last)
    if m1:
        base = " ".join(parts[:-1]).strip()
        return (base, int(last), None)
    m2 = re.fullmatch(rf"((?:19|20|21)\d{2})\s*{_RANGE_SEP}\s*((?:19|20|21)\d{2})", last)
    if m2:
        y1, y2 = int(m2.group(1)), int(m2.group(2))
        if y1 > y2: y1, y2 = y2, y1
        base = " ".join(parts[:-1]).strip()
        return (base, None, (y1, y2))
    return (q_n, None, None)

# ---- 共通ランキング処理 ----
def _search_ranked_all(records: List[Dict[str,str]], q: str) -> Tuple[List[Dict[str,Any]], List[str]]:
    base_terms=jp_terms(q)
    if not base_terms: return [], []
    terms_all=expand_terms_with_fold_and_dict(base_terms)
    hl_terms = make_highlight_terms(base_terms)
    scored=[(score_record(r,terms_all,q),r) for r in records]
    scored=[(s,r) for s,r in scored if s>0.0]
    if not scored: return [], hl_terms
    scored.sort(key=lambda x:x[0],reverse=True)
    ranked=[r for _,r in scored]
    return ranked, hl_terms

def score_record(rec: Dict[str,str], q_terms_all: List[str], q_raw: str) -> float:
    def low(s): return (s or "").lower()
    def fk(s):  return fold_kana(s or "")

    title,text,issue=rec.get("title",""),rec.get("text",""),rec.get("issue","")
    author=rec.get("author","")
    category=rec.get("category","")
    dpri = rec.get("date_primary","")

    title_f, text_f, issue_f = fk(title), fk(text), fk(issue)
    author_f, dpri_f = fk(author), fk(dpri)
    category_f = fk(category)

    score=0.0; matched=False
    for t in q_terms_all:
        tlow  = t.lower()
        tfold = fk(t)

        if tlow in low(title):   score+=W_TITLE;  matched=True
        if tlow in low(text):    score+=W_TEXT;   matched=True
        if tlow in low(author):  score+=W_AUTHOR; matched=True
        if tlow in low(issue):   score+=W_ISSUE;  matched=True
        if tlow in low(dpri):    score+=W_DATES;  matched=True
        if tlow in low(category): score+=W_CATEGORY; matched=True

        if tfold in title_f:     score+=W_TITLE*0.95;  matched=True
        if tfold in text_f:      score+=W_TEXT*0.95;   matched=True
        if tfold in author_f:    score+=W_AUTHOR*0.95; matched=True
        if tfold in issue_f:     score+=W_ISSUE*0.95;  matched=True
        if tfold in dpri_f:      score+=W_DATES*0.95;  matched=True
        if tfold in category_f:  score+=W_CATEGORY*0.95; matched=True

        # 日本語短語の暴発を抑止したファジー（英語のみ・>=3文字）
        if not matched and fuzzy_substring_match(tfold, text_f):
            score += BONUS_FUZZY; matched = True

    ql=_nfkc(q_raw).lower()
    if ql and (ql in low(title) or ql in low(text) or fk(ql) in text_f or fk(ql) in title_f):
        score+=BONUS_PHRASE; matched=True

    if matched:
        ref=_parse_date_any(dpri)
        if ref:
            years=max(0.0,(dt.date.today()-ref).days/365.25)
            score+=max(0.0,1.0-(years*0.1))*0.8
        if issue: score+=0.1
        if dpri:  score+=0.1

    if ("議事録" in category) and ("議事録" not in q_raw):
        score-=PENALTY_GIJIROKU

    return score if matched else 0.0

# ---- Notion検索（Notionが使えるとき）----
def search_notion_advanced(q_raw: str,
                           year: Optional[int]=None,
                           year_from: Optional[int]=None,
                           year_to: Optional[int]=None) -> Tuple[List[Dict[str,Any]], List[str]]:
    pages=notion_query_all()
    if not pages: return [], []
    records=[_extract_properties(p) for p in pages]
    ranked, hl_terms = _search_ranked_all(records, q_raw)
    if not ranked: return [], hl_terms
    ranked = [r for r in ranked if _matches_year(r, year, year_from, year_to)]
    return ranked, hl_terms

# ---- JSONL検索（kb.jsonlを使うとき）----
def search_kb_advanced(q_raw: str,
                       year: Optional[int]=None,
                       year_from: Optional[int]=None,
                       year_to: Optional[int]=None) -> Tuple[List[Dict[str,Any]], List[str]]:
    records, _, _ = load_kb(force=False)
    if not records:
        return [], []
    ranked, hl_terms = _search_ranked_all(records, q_raw)
    if not ranked: return [], hl_terms
    ranked = [r for r in ranked if _matches_year(r, year, year_from, year_to)]
    return ranked, hl_terms

def _matches_year(rec: Dict[str,str], year: Optional[int], y_from: Optional[int], y_to: Optional[int]) -> bool:
    if year is None and y_from is None and y_to is None:
        return True
    ys = _record_years(rec)
    if not ys:
        return False
    if year is not None:
        return year in ys
    lo = y_from if y_from is not None else -10**9
    hi = y_to   if y_to   is not None else  10**9
    return any(lo <= y <= hi for y in ys)

def _paginate(items: List[Dict[str,Any]], page: int, page_size: int) -> Tuple[List[Dict[str,Any]], bool]:
    total = len(items)
    start = (page-1)*page_size
    end   = start + page_size
    if start >= total:
        return [], False
    slice_ = items[start:end]
    has_more = end < total
    return slice_, has_more

# =============================
# API
# =============================
@app.get("/health")
def health():
    # kb 側の状況を取得（軽量）
    _, kb_sha, kb_lines = load_kb(force=False) if KB_URL or os.path.exists(KB_PATH) else ([], "", 0)
    return JSONResponse({
        "ok": True,
        "has_token": bool(NOTION_TOKEN),
        "has_dbid": bool(NOTION_DATABASE_ID),
        "has_kb": bool(kb_lines > 0),
        "kb_lines": kb_lines,
        "kb_sha256": kb_sha[:40] if kb_sha else None,
        "primary_date_keys": primary_date_keys(),
        "now": dt.datetime.now().isoformat(timespec="seconds"),
    }, headers=JSON_HEADERS)

@app.get("/version")
def version():
    return JSONResponse({"version": app.version}, headers=JSON_HEADERS)

@app.get("/diag")
def diag(q: str = Query("", description="動作確認用（年抽出も確認）")):
    base_q, y, yr = _parse_year_from_query(q)
    y_from, y_to = (yr or (None, None))
    return JSONResponse({
        "query_echo": {"raw": q, "nfkc": _nfkc(q), "base_q": base_q, "year": y, "year_from": y_from, "year_to": y_to},
        "help": {
            "year_tail": "コンテスト ２０２３ / コンテスト 2023",
            "year_range_tail": "剪定 1999-2001 / 1999..2001 / 1999〜2001 / １９９９～２００１",
            "params": "/api/search?q=苔&page=2&page_size=5"
        }
    }, headers=JSON_HEADERS)

@app.get("/api/search")
def api_search(
    q: str = Query("", description="検索ワード（末尾に年/年範囲も可：例『苔 2001』『剪定 1999〜2001』）"),
    top_k: int = Query(5, ge=1, le=50, description="互換用（ページ送り利用時は無視）"),
    year: Optional[int] = Query(None, description="年（4桁）"),
    year_from: Optional[int] = Query(None, description="開始年（4桁）"),
    year_to: Optional[int] = Query(None, description="終了年（4桁）"),
    page: int = Query(1, ge=1, description="ページ番号（1始まり）"),
    page_size: int = Query(5, ge=1, le=50, description="1ページの件数（既定5）"),
    order: str = Query("relevance", description="latest | relevance"),
    debug: int = Query(0, ge=0, le=1, description="1で代表日の根拠を返す"),
):
    try:
        if not q.strip():
            return JSONResponse(
                {"items": [], "total_hits": 0, "page": page, "page_size": page_size,
                 "has_more": False, "next_page": None, "error": None},
                headers=JSON_HEADERS
            )
        # 末尾の年/範囲を自動解釈（全角→半角）
        base_q, y_tail, yr_tail = _parse_year_from_query(q)
        y = year if year is not None else y_tail
        yf, yt = year_from, year_to
        if yr_tail is not None:
            yf = yf if yf is not None else yr_tail[0]
            yt = yt if yt is not None else yr_tail[1]

        # ---- データソース選択：Notion優先・無ければkb.jsonl ----
        if NOTION_TOKEN and NOTION_DATABASE_ID:
            ranked, hl_terms = search_notion_advanced(base_q, y, yf, yt)
        else:
            ranked, hl_terms = search_kb_advanced(base_q, y, yf, yt)

        if not ranked:
            return JSONResponse(
                {"items": [], "total_hits": 0, "page": 1, "page_size": page_size,
                 "has_more": False, "next_page": None, "error": None},
                headers=JSON_HEADERS
            )

        # ---- 代表日での並べ替え（ページング前）----
        decorated = []
        for i, r in enumerate(ranked):
            bd, info = _best_date(r)
            decorated.append((i, bd, info, r))

        if order.lower() == "latest":
            decorated.sort(key=lambda t: t[0])                # まず元の順（関連度）を固定
            decorated.sort(key=lambda t: t[1], reverse=True)  # 代表日で降順
        else:
            decorated.sort(key=lambda t: t[0])                # 従来どおり（関連度）

        ordered = [r for _, _, _, r in decorated]
        total_hits = len(ordered)

        # ---- ページング ----
        page_items, has_more = _paginate(ordered, page, page_size)

        # ---- レスポンス（UIは元のまま：ハイライト/本文にヒット 表示）----
        items=[]
        def head_h(r):
            head = _make_head(r)
            return _highlight_terms(head, hl_terms)

        # 代表日根拠の参照用マップ（debug用）
        base_map = {id(r): (d, info) for (_, d, info, r) in decorated}

        for idx, r in enumerate(page_items):
            head_hl = head_h(r)
            text_raw = r.get("text","")
            shaped = _apply_head_and_excerpt(r, head_hl, text_raw, is_first_in_page=(idx==0), hl_terms=hl_terms)

            payload = {
                "title": shaped.get("title",""),
                "content": shaped.get("text",""),
                "url": shaped.get("url",""),
                "source": shaped.get("url",""),
                "rank": (ranked.index(r) + 1) if r in ranked else None
            }
            if debug == 1:
                bd, info = base_map.get(id(r), (dt.date(1970,1,1), {"kind":"fallback_1970"}))
                payload["debug_date_used"] = {
                    "date_used": bd.isoformat(),
                    "kind": info.get("kind"),
                    "value": info.get("value"),
                    "years_inferred": info.get("years_inferred", _record_years(r)),
                    "date_primary": r.get("date_primary",""),
                    "last_edited_time": r.get("last_edited_time",""),
                    "created_time": r.get("created_time",""),
                }
            items.append(payload)

        return JSONResponse({
            "items": items,
            "total_hits": total_hits,
            "page": page,
            "page_size": page_size,
            "has_more": has_more,
            "next_page": (page+1) if has_more else None,
            "order_used": order.lower(),
            "error": None
        }, headers=JSON_HEADERS)

    except Exception as e:
        return JSONResponse({
            "items": [],
            "total_hits": 0,
            "page": 1,
            "page_size": page_size,
            "has_more": False,
            "next_page": None,
            "error": f"{type(e).__name__}: {e}"
        }, headers=JSON_HEADERS)

@app.get("/api/chat")
def api_chat(q:str=Query("", description="質問文（会話用）"),
             top_k:int=Query(5, ge=1, le=10)):
    msg = ("[INFO]\n現在、チャット機能はメンテナンスのため一時停止中です。\n"
           "検索システムをご利用ください。")
    return JSONResponse({"text": msg, "items": []}, headers=JSON_HEADERS)

@app.get("/")
def root():
    return PlainTextResponse("Mini Rose Search API is running.\n",
                             headers={"content-type":"text/plain; charset=utf-8"})
# ==== 最下部追加：診断・確認用エンドポイント ===============================

@app.get("/health2")
def health2():
    """kb.jsonl の読み込み確認"""
    _, kb_sha, kb_lines = load_kb(force=False) if KB_URL or os.path.exists(KB_PATH) else ([], "", 0)
    return JSONResponse({
        "ok": True,
        "has_kb": bool(kb_lines > 0),
        "kb_size": kb_lines,
        "kb_sha256": (kb_sha[:40] if kb_sha else None),
        "kb_url": KB_URL,
        "kb_path": KB_PATH,
        "version_hint": "jsonl-health2"
    }, headers=JSON_HEADERS)

@app.post("/kb/reload")
def kb_reload():
    """kb.jsonl の再読み込み"""
    _, kb_sha, kb_lines = load_kb(force=True)
    return JSONResponse({
        "reloaded": True,
        "has_kb": bool(kb_lines > 0),
        "kb_size": kb_lines,
        "kb_sha256": (kb_sha[:40] if kb_sha else None),
        "kb_url": KB_URL,
        "kb_path": KB_PATH
    }, headers=JSON_HEADERS)

@app.get("/diag2")
def diag2():
    """簡易診断（KB の有無など）"""
    _, kb_sha, kb_lines = load_kb(force=False) if KB_URL or os.path.exists(KB_PATH) else ([], "", 0)
    return JSONResponse({
        "version_hint": "jsonl-diag2",
        "kb_url": KB_URL,
        "has_kb": bool(kb_lines > 0),
        "kb_size": kb_lines,
        "kb_sha256": (kb_sha[:40] if kb_sha else None),
    }, headers=JSON_HEADERS)

# ==== 追加ここまで ==========================================================
