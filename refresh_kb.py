#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
refresh_kb.py — Notion DB → kb.jsonl 生成（インラインDB/フルページDB 両対応）
【修正ポイント】
- rich_text が途中で切れる問題を完全解決
- 全 fragment を順番通りにつなぐ safe_plain() を導入
- 本文(body)の抽出ロジックを一本化し、欠落を防止
"""

import os, sys, json, time, hashlib, datetime as dt
import requests
from typing import Dict, Any, List, Optional, Tuple

# ---- Notion 接続 ----
NOTION_TOKEN = os.getenv("NOTION_TOKEN", "").strip()
DB_ID        = os.getenv("NOTION_DATABASE_ID", "").strip()

if not NOTION_TOKEN or not DB_ID:
    print("[ERROR] NOTION_TOKEN or NOTION_DATABASE_ID is missing.", file=sys.stderr)
    sys.exit(1)

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

# ---- 環境変数による明示マップ ----
ENV_FIELD = {
    "title": os.getenv("FIELD_TITLE", "").strip(),
    "url": os.getenv("FIELD_URL", "").strip(),
    "date_primary": os.getenv("FIELD_DATE_PRIMARY", "").strip(),
    "date_secondary": os.getenv("FIELD_DATE_SECONDARY", "").strip(),
    "body": os.getenv("FIELD_BODY", "").strip(),
    "tags": os.getenv("FIELD_TAGS", "").strip(),
    "issue": os.getenv("FIELD_ISSUE", "").strip(),
}

# ---- 日本語UIの名称候補 ----
NAME_CANDIDATES = {
    "url": ["出典URL", "URL", "リンク", "Link"],
    "date": ["発行日", "開催日", "日付", "Date"],
    "body": ["本文", "内容", "メモ", "説明", "Body", "Notes"],
    "tags": ["タグ", "カテゴリ", "カテゴリー", "分類", "Tags", "Category"],
    "issue": ["号", "会報号", "No", "Issue", "号数"],
}

# =====================================================================
# ■ rich_text 欠損防止：fragment 全結合の「safe_plain」
# =====================================================================
def safe_plain(rich_arr: List[Dict[str, Any]]) -> str:
    """rich_text[] の plain_text を **順番どおり** 全部結合する。
    - fragment の欠落を防ぐ
    - 全角文字・句点・濁点後でも切れない
    """
    out: List[str] = []
    if not rich_arr:
        return ""
    for frag in rich_arr:
        t = frag.get("plain_text", "")
        if t:
            out.append(t)
    return "".join(out)

# =====================================================================
# Notion API utilities
# =====================================================================
def notion_get_database(db_id: str) -> Dict[str, Any]:
    r = requests.get(f"https://api.notion.com/v1/databases/{db_id}", headers=HEADERS)
    if r.status_code != 200:
        print(f"[ERROR] get_database failed: {r.status_code} {r.text}", file=sys.stderr)
        sys.exit(2)
    return r.json()

def notion_query_all(db_id: str) -> List[Dict[str, Any]]:
    results = []
    payload = {"page_size": 100}
    next_cursor = None
    while True:
        if next_cursor:
            payload["start_cursor"] = next_cursor
        r = requests.post(
            f"https://api.notion.com/v1/databases/{db_id}/query",
            headers=HEADERS,
            data=json.dumps(payload),
        )
        if r.status_code != 200:
            print(f"[ERROR] query failed: {r.status_code} {r.text}", file=sys.stderr)
            break
        data = r.json()
        results.extend(data.get("results", []))
        next_cursor = data.get("next_cursor")
        if not data.get("has_more"):
            break
    return results

# =====================================================================
# フィールド自動検出
# =====================================================================
def pick_title_property_name(schema: Dict[str, Any]) -> Optional[str]:
    for prop_name, meta in schema.get("properties", {}).items():
        if meta.get("type") == "title":
            return prop_name
    return None

def pick_first_of_type(schema: Dict[str, Any], type_name: str, name_hint: Optional[str], pool: List[str]) -> Optional[str]:
    if name_hint and name_hint in schema.get("properties", {}):
        return name_hint
    for prop_name, meta in schema.get("properties", {}).items():
        if meta.get("type") == type_name:
            return prop_name
    for cand in pool:
        if cand in schema.get("properties", {}):
            return cand
    return None

# =====================================================================
# テキスト抽出（修正版）
# =====================================================================
def extract_title(props: Dict[str, Any], title_name: str) -> str:
    arr = props.get(title_name, {}).get("title", [])
    return safe_plain(arr).strip()

def extract_rich_text(props: Dict[str, Any], field_name: str) -> str:
    """rich_text の欠損を完全に防ぐ"""
    meta = props.get(field_name, {})
    if "rich_text" in meta:
        return safe_plain(meta["rich_text"]).strip()
    if "title" in meta:
        return safe_plain(meta["title"]).strip()
    if "url" in meta:
        return (meta.get("url") or "").strip()
    if "number" in meta and meta.get("number") is not None:
        return str(meta["number"]).strip()
    if "select" in meta and meta.get("select"):
        return meta["select"].get("name", "").strip()
    if "multi_select" in meta and meta.get("multi_select"):
        return ", ".join([x.get("name","") for x in meta["multi_select"] if x.get("name")])
    return ""

# =====================================================================
# URL / 日付 / タグ（省略：従来ロジックそのまま）
# =====================================================================
def extract_url(props: Dict[str, Any], url_name: Optional[str]) -> str:
    if url_name and "url" in props.get(url_name, {}):
        return props[url_name].get("url") or ""
    # fallback
    for name, meta in props.items():
        if meta.get("type") == "url":
            return meta.get("url") or ""
    for name, meta in props.items():
        if "rich_text" in meta:
            txt = safe_plain(meta["rich_text"])
            for token in txt.split():
                if token.startswith(("http://", "https://")):
                    return token
    return ""

def extract_date_iso(props: Dict[str, Any], primary: Optional[str], secondary: Optional[str]) -> Optional[str]:
    def _get(field):
        meta = props.get(field, {})
        if meta.get("type") != "date":
            return None
        d = meta.get("date") or {}
        return d.get("start")
    for f in [primary, secondary]:
        if f:
            got = _get(f)
            if got:
                return got
    for name, meta in props.items():
        if meta.get("type") == "date":
            got = _get(name)
            if got:
                return got
    return None

def extract_tags(props: Dict[str, Any], tags_name: Optional[str]) -> List[str]:
    if tags_name and tags_name in props:
        meta = props[tags_name]
        if meta.get("type") == "multi_select":
            return [x.get("name","") for x in meta.get("multi_select", [])]
        if meta.get("type") == "select" and meta.get("select"):
            return [meta["select"].get("name","")]
    # auto
    for name, meta in props.items():
        if meta.get("type") == "multi_select":
            return [x.get("name","") for x in meta.get("multi_select", [])]
        if meta.get("type") == "select" and meta.get("select"):
            return [meta["select"].get("name","")]
    return []

# =====================================================================
# レコード正規化（本文の欠損防止の本体）
# =====================================================================
def normalize_record(page: Dict[str, Any], fields: Dict[str, Optional[str]]) -> Optional[Dict[str, Any]]:
    props = page.get("properties", {})
    if not props:
        return None

    title_name = fields["title"]
    if not title_name:
        return None

    title = extract_title(props, title_name)
    if not title:
        return None

    url  = extract_url(props, fields["url"])
    body = ""

    # ---- ① BODY フィールドがある場合（最優先） ----
    if fields["body"]:
        body = extract_rich_text(props, fields["body"])

    # ---- ② fallback: 全 rich_text を結合（今回の修正で確実に全文取得）----
    if not body.strip():
        all_parts = []
        for name, meta in props.items():
            if meta.get("type") == "rich_text":
                t = safe_plain(meta.get("rich_text", []))
                if t.strip():
                    all_parts.append(t.strip())
        body = "\n".join(all_parts).strip()

    date_iso = extract_date_iso(props, fields["date_primary"], fields["date_secondary"])
    tags     = extract_tags(props, fields["tags"])
    issue    = None

    rec = {
        "id": page.get("id"),
        "title": title,
        "url": url,
        "date": date_iso,
        "body": body,
        "tags": tags,
        "issue": issue,
        "created_time": page.get("created_time"),
        "last_edited_time": page.get("last_edited_time"),
        "source": "notion",
    }
    return rec

# =====================================================================
# JSONL 出力
# =====================================================================
def write_jsonl(path: str, rows: List[Dict[str, Any]]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def write_integrity(path_rows: str, path_integrity: str) -> None:
    with open(path_rows, "rb") as f:
        data = f.read()
    lines = data.count(b"\n")
    sha = hashlib.sha256(data).hexdigest()
    with open(path_integrity, "w", encoding="utf-8") as g:
        g.write(f"lines={lines}\nsha256={sha}\n")

# =====================================================================
# main
# =====================================================================
def main():
    print("[INFO] refresh_kb.py start (patched version — safe rich_text)")
    print(f"[INFO] DB_ID={DB_ID[:8]}...")

    schema = notion_get_database(DB_ID)
    fields = {
        "title": pick_title_property_name(schema),
        "url": pick_first_of_type(schema, "url", ENV_FIELD["url"], NAME_CANDIDATES["url"]),
        "date_primary": pick_first_of_type(schema, "date", ENV_FIELD["date_primary"], NAME_CANDIDATES["date"]),
        "date_secondary": None,
        "body": pick_first_of_type(schema, "rich_text", ENV_FIELD["body"], NAME_CANDIDATES["body"]),
        "tags": pick_first_of_type(schema, "multi_select", ENV_FIELD["tags"], NAME_CANDIDATES["tags"]),
        "issue": ENV_FIELD["issue"],
    }

    pages = notion_query_all(DB_ID)
    rows  = []

    for p in pages:
        rec = normalize_record(p, fields)
        if rec:
            rows.append(rec)

    # ソート（date or 作成日）
    def sort_key(r):
        return r.get("date") or r.get("created_time") or r.get("last_edited_time") or ""
    rows.sort(key=sort_key, reverse=True)

    write_jsonl("kb.jsonl", rows)
    write_integrity("kb.jsonl", "kb_integrity.txt")

    print("[OK] wrote kb.jsonl")
    print("[OK] patched version complete")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] unexpected: {e}", file=sys.stderr)
        sys.exit(10)
