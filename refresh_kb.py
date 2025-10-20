#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
refresh_kb.py — Notion DB → kb.jsonl 生成（インラインDB/フルページDB 両対応）
- インラインDBでもフルページDBでも動作（Notion APIはDB IDが同じ扱い）
- 列名の日本語差異を自動検出（envで明示も可）
- title / date / url / body を堅牢に抽出
- 生成物: kb.jsonl（UTF-8 / 1行1レコード）
環境変数（必要/任意）:
  NOTION_TOKEN            : 必須（Notion統合のシークレット）
  NOTION_DATABASE_ID      : 必須（32桁のDB ID）
  FIELD_TITLE             : 任意（既定：自動検出 "type=title"）
  FIELD_URL               : 任意（候補: 出典URL, URL 等）※type=url優先
  FIELD_DATE_PRIMARY      : 任意（候補: 発行日, 開催日 等）※type=date優先
  FIELD_DATE_SECONDARY    : 任意（あれば補助日付）
  FIELD_BODY              : 任意（候補: 本文, メモ, 内容 等）※rich_text優先
  FIELD_TAGS              : 任意（候補: タグ 等）※multi_select/select優先
  FIELD_ISSUE             : 任意（候補: 号, 会報号 等）※number/text両対応
出力:
  kb.jsonl / kb_integrity.txt（行数とsha256）
"""

import os, sys, json, time, hashlib, datetime as dt
import requests
from typing import Dict, Any, List, Optional, Tuple

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

# ---- 環境変数による明示マップ（任意） ----
ENV_FIELD = {
    "title": os.getenv("FIELD_TITLE", "").strip(),
    "url": os.getenv("FIELD_URL", "").strip(),
    "date_primary": os.getenv("FIELD_DATE_PRIMARY", "").strip(),
    "date_secondary": os.getenv("FIELD_DATE_SECONDARY", "").strip(),
    "body": os.getenv("FIELD_BODY", "").strip(),
    "tags": os.getenv("FIELD_TAGS", "").strip(),
    "issue": os.getenv("FIELD_ISSUE", "").strip(),
}

# ---- 推定時の名称候補（日本語UI想定） ----
NAME_CANDIDATES = {
    "url": ["出典URL", "URL", "リンク", "Link"],
    "date": ["発行日", "開催日", "日付", "Date"],
    "body": ["本文", "内容", "メモ", "説明", "Body", "Notes"],
    "tags": ["タグ", "カテゴリ", "カテゴリー", "分類", "Tags", "Category"],
    "issue": ["号", "会報号", "No", "Issue", "号数"],
}

def notion_get_database(db_id: str) -> Dict[str, Any]:
    r = requests.get(f"https://api.notion.com/v1/databases/{db_id}", headers=HEADERS)
    if r.status_code != 200:
        print(f"[ERROR] get_database failed: {r.status_code} {r.text}", file=sys.stderr)
        sys.exit(2)
    return r.json()

def notion_query_all(db_id: str) -> List[Dict[str, Any]]:
    """DB全件をページングで取得（インライン/フルページ共通）"""
    results = []
    payload = {"page_size": 100}
    next_cursor = None
    while True:
        if next_cursor:
            payload["start_cursor"] = next_cursor
        r = requests.post(f"https://api.notion.com/v1/databases/{db_id}/query",
                          headers=HEADERS, data=json.dumps(payload))
        if r.status_code != 200:
            print(f"[ERROR] query failed: {r.status_code} {r.text}", file=sys.stderr)
            break
        data = r.json()
        results.extend(data.get("results", []))
        next_cursor = data.get("next_cursor")
        if not data.get("has_more"):
            break
    return results

def pick_title_property_name(schema: Dict[str, Any]) -> Optional[str]:
    for prop_name, meta in schema.get("properties", {}).items():
        if meta.get("type") == "title":
            return prop_name
    return None

def pick_first_of_type(schema: Dict[str, Any], type_name: str, name_hint: Optional[str], name_pool: List[str]) -> Optional[str]:
    # 1) 明示指定あれば優先
    if name_hint and name_hint in schema.get("properties", {}):
        return name_hint
    # 2) 型一致のプロパティを優先
    for prop_name, meta in schema.get("properties", {}).items():
        if meta.get("type") == type_name:
            return prop_name
    # 3) 候補名で一致するもの
    for cand in name_pool:
        if cand in schema.get("properties", {}):
            return cand
    return None

def to_plain_text(rich: List[Dict[str, Any]]) -> str:
    out = []
    for r in rich or []:
        t = r.get("plain_text")
        if t:
            out.append(t)
    return "".join(out).strip()

def extract_title(props: Dict[str, Any], title_name: str) -> str:
    # title プロパティは type=title 固定
    title_arr = props.get(title_name, {}).get("title", [])
    return to_plain_text(title_arr)

def extract_rich_text(props: Dict[str, Any], field_name: str) -> str:
    meta = props.get(field_name, {})
    t = ""
    if "rich_text" in meta:
        t = to_plain_text(meta.get("rich_text", []))
    elif "title" in meta:
        t = to_plain_text(meta.get("title", []))
    elif "url" in meta and isinstance(meta.get("url"), str):
        t = meta.get("url") or ""
    elif "number" in meta and meta.get("number") is not None:
        t = str(meta.get("number"))
    elif "select" in meta and meta.get("select"):
        t = meta["select"].get("name", "")
    elif "multi_select" in meta and meta.get("multi_select"):
        t = ", ".join([x.get("name","") for x in meta["multi_select"] if x.get("name")])
    elif "people" in meta and meta.get("people"):
        t = ", ".join([p.get("name","") for p in meta["people"] if p.get("name")])
    elif "email" in meta and meta.get("email"):
        t = meta["email"]
    elif "phone_number" in meta and meta.get("phone_number"):
        t = meta["phone_number"]
    return (t or "").strip()

def extract_url(props: Dict[str, Any], url_name: Optional[str]) -> str:
    if url_name and "url" in props.get(url_name, {}):
        return props[url_name].get("url") or ""
    # fallback: rich_text/タイトルからURLらしき文字列を拾う
    for name, meta in props.items():
        if meta.get("type") == "url":
            return meta.get("url") or ""
    # rich_textにhttpが含まれていれば拾う（安全のため先頭一つ）
    for name, meta in props.items():
        if "rich_text" in meta:
            txt = to_plain_text(meta["rich_text"])
            if "http://" in txt or "https://" in txt:
                # 粗く先頭URLを抽出
                for token in txt.split():
                    if token.startswith("http://") or token.startswith("https://"):
                        return token.strip()
    return ""

def extract_date_iso(props: Dict[str, Any], primary: Optional[str], secondary: Optional[str]) -> Optional[str]:
    def _get_date(field: str) -> Optional[str]:
        meta = props.get(field, {})
        if not meta or meta.get("type") != "date":
            return None
        val = meta.get("date") or {}
        if not val:
            return None
        # start だけ使う（end は期間用）
        s = val.get("start")
        if s:
            # すでにISOのはず（Notion API）
            return s
        return None
    # 1) 明示指定の優先
    for f in [primary, secondary]:
        if f:
            got = _get_date(f)
            if got:
                return got
    # 2) 型= date の最初
    for name, meta in props.items():
        if meta.get("type") == "date":
            got = _get_date(name)
            if got:
                return got
    return None

def extract_tags(props: Dict[str, Any], tags_name: Optional[str]) -> List[str]:
    # select / multi_select を優先
    def _tags_from(meta: Dict[str, Any]) -> List[str]:
        if meta.get("type") == "multi_select":
            return [x.get("name","") for x in meta.get("multi_select", []) if x.get("name")]
        if meta.get("type") == "select" and meta.get("select"):
            return [meta["select"].get("name","")]
        return []
    if tags_name and tags_name in props:
        return _tags_from(props[tags_name])
    # 自動検出
    for name, meta in props.items():
        if meta.get("type") in ("multi_select","select"):
            got = _tags_from(meta)
            if got:
                return got
    return []

def extract_issue(props: Dict[str, Any], issue_name: Optional[str]) -> Optional[str]:
    name_order = []
    if issue_name: name_order.append(issue_name)
    name_order.extend(NAME_CANDIDATES["issue"])
    for name in name_order:
        meta = props.get(name, {})
        if not meta:
            continue
        t = meta.get("type")
        if t == "number" and meta.get("number") is not None:
            return str(meta["number"])
        if t == "rich_text":
            s = to_plain_text(meta.get("rich_text", []))
            if s:
                return s
        if t == "title":
            s = to_plain_text(meta.get("title", []))
            if s:
                return s
        if t == "select" and meta.get("select"):
            return meta["select"].get("name","")
        if t == "multi_select" and meta.get("multi_select"):
            return ", ".join([x.get("name","") for x in meta["multi_select"] if x.get("name")])
    return None

def detect_field_names(schema: Dict[str, Any]) -> Dict[str, Optional[str]]:
    props = schema.get("properties", {})
    # title は必ず1つ（type=title）
    title_name = ENV_FIELD["title"] or pick_title_property_name(schema)
    url_name   = pick_first_of_type(schema, "url", ENV_FIELD["url"], NAME_CANDIDATES["url"])
    # date は primary/secondary 考慮
    date_primary   = ENV_FIELD["date_primary"] or pick_first_of_type(schema, "date", None, NAME_CANDIDATES["date"])
    date_secondary = ENV_FIELD["date_secondary"] or None
    # body は rich_text/長文候補から
    body_name  = ENV_FIELD["body"] or pick_first_of_type(schema, "rich_text", None, NAME_CANDIDATES["body"])
    tags_name  = ENV_FIELD["tags"] or pick_first_of_type(schema, "multi_select", None, NAME_CANDIDATES["tags"]) \
                 or pick_first_of_type(schema, "select", None, NAME_CANDIDATES["tags"])
    issue_name = ENV_FIELD["issue"] or None

    return {
        "title": title_name,
        "url": url_name,
        "date_primary": date_primary,
        "date_secondary": date_secondary,
        "body": body_name,
        "tags": tags_name,
        "issue": issue_name,
    }

def normalize_record(page: Dict[str, Any], fields: Dict[str, Optional[str]]) -> Optional[Dict[str, Any]]:
    props = page.get("properties", {})
    if not props:
        return None

    title_name = fields["title"]
    if not title_name:
        # title が見つからないページはスキップ
        return None

    title = extract_title(props, title_name).strip()
    if not title:
        # タイトル空はスキップ（最低1文字必要）
        return None

    url  = extract_url(props, fields["url"]).strip()
    body = ""
    # body が指定/検出できなければ、他のrich_textを寄せ集めて本文化
    if fields["body"]:
        body = extract_rich_text(props, fields["body"])
    if not body:
        # 全rich_textを結合して本文に（長文優先）
        rich_all = []
        for name, meta in props.items():
            if meta.get("type") == "rich_text":
                s = to_plain_text(meta.get("rich_text", []))
                if s:
                    rich_all.append(s)
        body = "\n".join(rich_all).strip()

    date_iso = extract_date_iso(props, fields["date_primary"], fields["date_secondary"])
    tags     = extract_tags(props, fields["tags"])
    issue    = extract_issue(props, fields["issue"])

    # created/last_edited も拾っておく（並び替えに使える）
    created   = page.get("created_time")
    edited    = page.get("last_edited_time")
    page_id   = page.get("id")

    rec = {
        "id": page_id,
        "title": title,
        "url": url,
        "date": date_iso,  # ISO8601 or None
        "body": body,
        "tags": tags,
        "issue": issue,
        "created_time": created,
        "last_edited_time": edited,
        "source": "notion",
    }
    return rec

def write_jsonl(path: str, rows: List[Dict[str, Any]]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def write_integrity(path_rows: str, path_integrity: str) -> None:
    # 行数とsha256
    with open(path_rows, "rb") as f:
        data = f.read()
    lines = data.count(b"\n")
    sha = hashlib.sha256(data).hexdigest()
    with open(path_integrity, "w", encoding="utf-8") as g:
        g.write(f"lines={lines}\nsha256={sha}\n")

def main():
    print("[INFO] refresh_kb.py start (INLINE/FP compatible)")
    print(f"[INFO] DB_ID={DB_ID[:8]}... (masked)")
    # 1) スキーマ取得
    schema = notion_get_database(DB_ID)
    fields = detect_field_names(schema)
    print("[INFO] detected fields:", json.dumps(fields, ensure_ascii=False))

    if not fields["title"]:
        print("[ERROR] title(type=title) property not found. Please ensure the leftmost 'Name' column exists.", file=sys.stderr)
        sys.exit(3)

    # 2) 全件クエリ
    pages = notion_query_all(DB_ID)
    print(f"[INFO] fetched pages: {len(pages)}")

    # 3) 正規化
    rows = []
    skipped = 0
    for p in pages:
        rec = normalize_record(p, fields)
        if rec:
            rows.append(rec)
        else:
            skipped += 1
    print(f"[INFO] normalized rows: {len(rows)} (skipped={skipped})")

    # 4) ソート（date or created_time の降順）
    def sort_key(r):
        t = r.get("date") or r.get("created_time") or r.get("last_edited_time") or ""
        return t
    rows.sort(key=sort_key, reverse=True)

    # 5) 出力
    out_jsonl = "kb.jsonl"
    out_int   = "kb_integrity.txt"
    write_jsonl(out_jsonl, rows)
    write_integrity(out_jsonl, out_int)

    print(f"[OK] wrote {out_jsonl} and {out_int}")
    print("[OK] INLINE MODE ACTIVE — DB構造に依存せず取得完了（title必須）")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[WARN] interrupted by user", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        print(f"[ERROR] unexpected: {e}", file=sys.stderr)
        sys.exit(10)
