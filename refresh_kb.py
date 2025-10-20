# refresh_kb.py — Notion → kb.jsonl 変換（title型のみ・フォールバックなし）
# version: 2025-10-20 strict-title

import os
import json
import requests
import datetime as dt
from typing import Dict, Any, List

# ===== Notion 認証 =====
NOTION_TOKEN = os.getenv("NOTION_TOKEN", "")
# 既存仕様（NOTION_DB_ID）を優先し、無ければ NOTION_DATABASE_ID を使う
NOTION_DATABASE_ID = os.getenv("NOTION_DB_ID", "") or os.getenv("NOTION_DATABASE_ID", "")

# ===== 列名（任意） =====
# ※ 指定しなくてもOK。指定した場合は、その列が「type=='title'」のときだけ採用します。
FIELD_TITLE  = os.getenv("FIELD_TITLE", "").strip()

# そのほか任意の列（必要に応じて環境変数で上書き可）
FIELD_AUTHOR = os.getenv("FIELD_AUTHOR", "講師/著者")
FIELD_URL    = os.getenv("FIELD_URL", "出典URL")
FIELD_TEXT   = os.getenv("FIELD_TEXT", "講習会等内容")
FIELD_ISSUE  = os.getenv("FIELD_ISSUE", "会報号")
FIELD_DATE   = os.getenv("FIELD_DATE", "開催日/発行日")

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

def _plain_from_list(arr: Any) -> str:
    """title / rich_text 用: plain_text を連結"""
    if not isinstance(arr, list):
        return ""
    return "".join([x.get("plain_text", "") for x in arr]).strip()

def extract_text_from_prop(prop: Dict[str, Any]) -> str:
    """プロパティから文字列を抽出（title / rich_text / select 等に対応）"""
    t = prop.get("type")
    if t in ("title", "rich_text"):
        return _plain_from_list(prop.get(t, []))
    if t == "date":
        d = prop.get("date") or {}
        return d.get("start", "") or ""
    if t == "select":
        s = prop.get("select") or {}
        return s.get("name", "") or ""
    if t == "multi_select":
        return ",".join([x.get("name", "") for x in prop.get("multi_select", [])]).strip()
    if t == "url":
        return prop.get("url") or ""
    if t == "number":
        n = prop.get("number", None)
        return "" if n is None else str(n)
    if t == "people":
        return ",".join([x.get("name", "") for x in prop.get("people", [])]).strip()
    # それ以外は素直に文字列化
    return str(prop.get(t, "")).strip()

def pick_title_key(props: Dict[str, Any]) -> str:
    """
    title型のプロパティ名を返す。
    - FIELD_TITLE が設定され、その列が title 型なら最優先。
    - そうでなければ props から type=='title' を探索。
    - 見つからなければ空文字。
    """
    if FIELD_TITLE and FIELD_TITLE in props and props[FIELD_TITLE].get("type") == "title":
        return FIELD_TITLE
    for k, v in props.items():
        if v.get("type") == "title":
            return k
    return ""

def fetch_all_pages(database_id: str) -> List[Dict[str, Any]]:
    """Notion DB をページネーション付きで全件取得"""
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    body = {"page_size": 100}
    results: List[Dict[str, Any]] = []
    while True:
        r = requests.post(url, headers=HEADERS, json=body, timeout=60)
        r.raise_for_status()
        js = r.json()
        results.extend(js.get("results", []))
        if not js.get("has_more"):
            break
        body["start_cursor"] = js.get("next_cursor")
    return results

def build_records(pages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """kb.jsonl に書き出すレコード群を組み立て"""
    records: List[Dict[str, Any]] = []

    if not pages:
        return records
    props0 = pages[0].get("properties", {})
    title_key = pick_title_key(props0)
    print(f"[INFO] title_key={repr(title_key)} (type='title' only)")

    for page in pages:
        props = page.get("properties", {})

        # --- title（必須: title型のみ使用。フォールバックなし） ---
        title_val = ""
        if title_key and title_key in props and props[title_key].get("type") == "title":
            title_val = extract_text_from_prop(props[title_key])

        # --- 任意の他フィールド ---
        def get(field_name: str) -> str:
            if field_name in props:
                return extract_text_from_prop(props[field_name])
            return ""

        rec = {
            "issue":        get(FIELD_ISSUE),
            "date_primary": get(FIELD_DATE),
            "author":       get(FIELD_AUTHOR),
            "title":        title_val,   # ← 定義（空は空のまま）
            "text":         get(FIELD_TEXT),
            "url":          get(FIELD_URL),
        }
        records.append(rec)
    return records

def save_jsonl(records: List[Dict[str, Any]], path: str = "kb.jsonl") -> None:
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"[OK] wrote {len(records)} records to {path}")

def main() -> None:
    if not NOTION_TOKEN or not NOTION_DATABASE_ID:
        dbid_len_old = len(os.getenv("NOTION_DB_ID", "") or "")
        dbid_len_new = len(os.getenv("NOTION_DATABASE_ID", "") or "")
        raise SystemExit(
            f"❌ Notion認証不足: NOTION_TOKEN={bool(NOTION_TOKEN)} "
            f"/ NOTION_DB_ID.len={dbid_len_old} / NOTION_DATABASE_ID.len={dbid_len_new}"
        )

    print("[INFO] fetching pages from Notion…")
    pages = fetch_all_pages(NOTION_DATABASE_ID)
    print(f"[INFO] fetched {len(pages)} pages")

    records = build_records(pages)
    save_jsonl(records)
    print("[DONE]", dt.datetime.now().isoformat())

if __name__ == "__main__":
    main()
