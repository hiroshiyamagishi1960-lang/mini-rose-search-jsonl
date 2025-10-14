#!/usr/bin/env python3
# refresh_kb.py — Notion → JSONL 変換（毎日自動更新用）

import os, sys, json, time, hashlib, re, io
from datetime import datetime, timezone
from typing import Dict, Any, List
import requests

NOTION_TOKEN       = os.getenv("NOTION_TOKEN", "")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID", "")

FIELD_TITLE  = os.getenv("FIELD_TITLE",  "タイトル")
FIELD_AUTHOR = os.getenv("FIELD_AUTHOR", "著者")
FIELD_URL    = os.getenv("FIELD_URL",    "出典URL")
FIELD_TEXT   = os.getenv("FIELD_TEXT",   "本文")
FIELD_ISSUE  = os.getenv("FIELD_ISSUE",  "会報号")
FIELD_DATE   = os.getenv("FIELD_DATE",   "代表日付")

KB_PATH = os.getenv("KB_PATH", "kb.jsonl")
BK_PATH = "kb_backup.jsonl"
INTEGRITY_PATH = "kb_integrity.txt"

NOTION_API = "https://api.notion.com/v1/databases/{db}/query"
HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

def _n_text(prop: Dict[str, Any]) -> str:
    if not prop:
        return ""
    if "title" in prop:
        return "".join(t.get("plain_text","") for t in prop.get("title", [])).strip()
    if "rich_text" in prop:
        return "".join(t.get("plain_text","") for t in prop.get("rich_text", [])).strip()
    if "url" in prop and isinstance(prop.get("url"), str):
        return prop["url"].strip()
    if "select" in prop and prop["select"]:
        return prop["select"].get("name","").strip()
    if "multi_select" in prop and prop["multi_select"]:
        return " ".join(t.get("name","") for t in prop["multi_select"]).strip()
    if "date" in prop and prop["date"]:
        return (prop["date"].get("start") or "").strip()
    if "people" in prop and prop["people"]:
        return " ".join(p.get("name","") or p.get("id","") for p in prop["people"]).strip()
    if "number" in prop and prop["number"] is not None:
        return str(prop["number"])
    if "email" in prop and prop["email"]:
        return prop["email"].strip()
    if "phone_number" in prop and prop["phone_number"]:
        return prop["phone_number"].strip()
    if "checkbox" in prop:
        return "true" if prop["checkbox"] else "false"
    if "formula" in prop and prop["formula"]:
        f = prop["formula"];  return str(next(iter(f.values()), ""))
    return ""

def _extract(record: Dict[str, Any]) -> Dict[str, Any]:
    props = record.get("properties", {})
    title   = _n_text(props.get(FIELD_TITLE)  or props.get("タイトル") or props.get("Name") or {})
    author  = _n_text(props.get(FIELD_AUTHOR) or props.get("著者") or {})
    url     = _n_text(props.get(FIELD_URL)    or props.get("出典URL") or {})
    text    = _n_text(props.get(FIELD_TEXT)   or props.get("本文") or {})
    issue   = _n_text(props.get(FIELD_ISSUE)  or props.get("会報号") or {})
    date_p  = _n_text(props.get(FIELD_DATE)   or props.get("代表日付") or props.get("日付") or {})

    if not url:
        url = record.get("url", "")

    date_primary = ""
    if date_p:
        m = re.match(r"^(\d{4}-\d{2}-\d{2})", date_p)
        date_primary = m.group(1) if m else date_p.replace("/", "-")

    return {
        "issue": issue or "",
        "date_primary": date_primary,
        "author": author or "",
        "title": title or "",
        "text": text or "",
        "url": url or ""
    }

def fetch_all_pages(database_id: str) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    has_more, cursor = True, None
    while has_more:
        payload = {"page_size": 100, **({"start_cursor": cursor} if cursor else {})}
        r = requests.post(NOTION_API.format(db=database_id), headers=HEADERS, json=payload, timeout=60)
        if r.status_code >= 500:
            ok = False
            for _ in range(3):
                time.sleep(2)
                r = requests.post(NOTION_API.format(db=database_id), headers=HEADERS, json=payload, timeout=60)
                if r.ok: ok = True; break
            if not ok: r.raise_for_status()
        elif not r.ok:
            r.raise_for_status()
        data = r.json()
        results.extend(data.get("results", []))
        has_more = data.get("has_more", False)
        cursor = data.get("next_cursor")
    return results

def write_jsonl(rows: List[Dict[str, Any]], path: str) -> None:
    if os.path.exists(path):
        try: os.replace(path, BK_PATH)
        except Exception: pass
    with io.open(path, "w", encoding="utf-8", newline="\n") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n")

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""): h.update(chunk)
    return h.hexdigest()

def main() -> int:
    if not NOTION_TOKEN or not NOTION_DATABASE_ID:
        print("ERROR: NOTION_TOKEN / NOTION_DATABASE_ID が未設定です。", file=sys.stderr)
        return 2
    pages = fetch_all_pages(NOTION_DATABASE_ID)
    records: List[Dict[str, Any]] = []
    valid = 0
    for p in pages:
        try:
            rec = _extract(p)
            if rec.get("title") or rec.get("text"):
                records.append(rec); valid += 1
        except Exception as e:
            print(f"WARN: skip record: {e}", file=sys.stderr)
            continue
    write_jsonl(records, KB_PATH)
    digest = sha256_file(KB_PATH)
    with io.open(INTEGRITY_PATH, "w", encoding="utf-8") as g:
        g.write(f"lines={valid}\nsha256={digest}\nupdated_utc={datetime.now(timezone.utc).isoformat()}\n")
    print(f"OK: Valid lines={valid}, sha256={digest[:12]}...")
    return 0

if __name__ == "__main__":
    sys.exit(main())
