#!/usr/bin/env python3
import os, sys, json, re, argparse, hashlib, unicodedata, time
from typing import Dict, Any, List, Tuple, Optional
import requests, yaml

NOTION_VER = "2022-06-28"

def nfkc(s: str) -> str:
    return unicodedata.normalize("NFKC", s)

def norm_text(s: str, keep_newlines: bool) -> str:
    if s is None: return ""
    t = nfkc(str(s))
    t = t.replace("\u3000", " ")
    if keep_newlines:
        # \r\n→\n、連続改行は最大2つまで
        t = t.replace("\r\n", "\n").replace("\r", "\n")
        t = re.sub(r"[ \t]+", " ", t)
        t = re.sub(r"\n{3,}", "\n\n", t)
        return t.strip()
    else:
        t = re.sub(r"[\r\n\t]+", " ", t)
        t = re.sub(r"\s+", " ", t)
        return t.strip()

def sha256_of_file(path: str) -> str:
    sha = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1<<20), b""):
            sha.update(b)
    return sha.hexdigest()

def notion_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VER,
        "Content-Type": "application/json",
    }

def fetch_db_schema(token: str, db_id: str) -> Dict[str, Any]:
    r = requests.get(f"https://api.notion.com/v1/databases/{db_id}", headers=notion_headers(token), timeout=30)
    r.raise_for_status()
    return r.json()

def query_all_pages(token: str, db_id: str) -> List[Dict[str, Any]]:
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    headers = notion_headers(token)
    results: List[Dict[str, Any]] = []
    payload: Dict[str, Any] = {"page_size": 100}
    while True:
        r = requests.post(url, headers=headers, json=payload, timeout=60)
        r.raise_for_status()
        data = r.json()
        results.extend([x for x in data.get("results", []) if not x.get("archived")])
        if data.get("has_more") and data.get("next_cursor"):
            payload["start_cursor"] = data["next_cursor"]
        else:
            break
    return results

def fetch_page_blocks(token: str, page_id: str) -> List[Dict[str, Any]]:
    # 1階層目＋簡易的に子も辿る（深追いはしない）
    headers = notion_headers(token)
    url = f"https://api.notion.com/v1/blocks/{page_id}/children?page_size=100"
    blocks: List[Dict[str, Any]] = []
    next_cursor = None
    while True:
        u = url + (f"&start_cursor={next_cursor}" if next_cursor else "")
        r = requests.get(u, headers=headers, timeout=60)
        r.raise_for_status()
        data = r.json()
        for b in data.get("results", []):
            blocks.append(b)
            if b.get("has_children"):
                # 1段だけ子を取得
                child_id = b["id"]
                try:
                    cr = requests.get(f"https://api.notion.com/v1/blocks/{child_id}/children?page_size=100",
                                      headers=headers, timeout=60)
                    cr.raise_for_status()
                    blocks.extend(cr.json().get("results", []))
                except Exception:
                    pass
        if data.get("has_more") and data.get("next_cursor"):
            next_cursor = data["next_cursor"]
        else:
            break
    return blocks

def rich_text_to_str(rt_list: List[Dict[str, Any]]) -> str:
    return "".join([x.get("plain_text","") for x in (rt_list or [])])

def extract_title_property_name(schema: Dict[str, Any]) -> Optional[str]:
    props = (schema or {}).get("properties", {})
    for name, meta in props.items():
        if (meta or {}).get("type") == "title":
            return name
    return None

def get_prop_value(page: Dict[str, Any], name: str) -> Optional[Dict[str, Any]]:
    props = page.get("properties", {})
    return props.get(name)

def pick_first_available(page: Dict[str, Any], candidates: List[str]) -> Tuple[str, Optional[Dict[str, Any]]]:
    for name in candidates:
        pv = get_prop_value(page, name)
        if pv is not None:
            return name, pv
    return "", None

def property_to_text(prop: Dict[str, Any]) -> str:
    if not prop: return ""
    t = prop.get("type")
    if t == "title":
        return rich_text_to_str(prop.get("title", []))
    if t == "rich_text":
        return rich_text_to_str(prop.get("rich_text", []))
    if t == "url":
        return prop.get("url") or ""
    if t == "select":
        o = prop.get("select") or {}
        return o.get("name") or ""
    if t == "multi_select":
        arr = prop.get("multi_select") or []
        return " ".join([(x or {}).get("name","") for x in arr])
    if t == "people":
        arr = prop.get("people") or []
        return " ".join([(x or {}).get("name","") for x in arr])
    if t == "date":
        d = prop.get("date") or {}
        return (d.get("start") or "")  # ISO8601
    if t == "number":
        n = prop.get("number")
        return "" if n is None else str(n)
    if t == "email":
        return prop.get("email") or ""
    if t == "phone_number":
        return prop.get("phone_number") or ""
    if t == "files":
        return ""  # 使わない
    return ""

def notion_page_url(page_id: str) -> str:
    # ハイフン除去版
    nid = page_id.replace("-", "")
    return f"https://www.notion.so/{nid}"

def blocks_to_text(blocks: List[Dict[str, Any]]) -> str:
    out_lines: List[str] = []
    for b in blocks:
        t = b.get("type")
        data = b.get(t, {}) if t else {}
        if t in ("paragraph", "quote", "callout"):
            out_lines.append(rich_text_to_str(data.get("rich_text", [])))
        elif t in ("heading_1","heading_2","heading_3","toggle"):
            out_lines.append(rich_text_to_str(data.get("rich_text", [])))
        elif t in ("bulleted_list_item","numbered_list_item","to_do"):
            out_lines.append("• " + rich_text_to_str(data.get("rich_text", [])))
        # 他のタイプは無視（表や画像など）
    # 連続空行整理は後段の normalize に任せる
    return "\n".join([x for x in out_lines if x is not None])

def build_record(page: Dict[str, Any],
                 schema: Dict[str, Any],
                 mapping: Dict[str, Any],
                 keep_newlines: bool,
                 do_nfkc: bool,
                 collapse_blanklines: bool) -> Dict[str, Any]:
    rec: Dict[str, Any] = {}

    # ---- title（型で検出） ----
    title_prop_name = extract_title_property_name(schema)
    title_text = ""
    if title_prop_name:
        title_text = property_to_text(get_prop_value(page, title_prop_name))
    # ---- url ----
    url_props = mapping["fields"]["url"].get("prefer", [])
    _, url_prop = pick_first_available(page, url_props)
    url_text = property_to_text(url_prop) if url_prop else ""
    if not url_text:
        url_text = notion_page_url(page["id"])

    # ---- text ----
    text_props = mapping["fields"]["text"].get("prefer", [])
    _, text_prop = pick_first_available(page, text_props)
    if text_prop:
        text_text = property_to_text(text_prop)
    else:
        text_text = ""  # fallback: 後で page blocks
    # ---- date / author / category / issue ----
    def pick_text(key: str) -> str:
        names = mapping["fields"].get(key, {}).get("prefer", [])
        _, pv = pick_first_available(page, names)
        return property_to_text(pv) if pv else ""

    date_text     = pick_text("date")
    author_text   = pick_text("author")
    category_text = pick_text("category")
    issue_text    = pick_text("issue")

    # Fallback to page blocks if text empty
    if not text_text and mapping["fields"]["text"].get("fallback") == "page_blocks":
        try:
            blks = fetch_page_blocks(token=os.environ["_NOTION_TOKEN"], page_id=page["id"])
            text_text = blocks_to_text(blks)
        except Exception as e:
            text_text = ""

    # 正規化
    def maybe_nfkc(x: str) -> str:
        return nfkc(x) if do_nfkc else x

    title_text    = maybe_nfkc(title_text).strip()
    url_text      = url_text.strip()
    if collapse_blanklines:
        text_text = norm_text(text_text, keep_newlines=True)
    else:
        text_text = norm_text(text_text, keep_newlines=keep_newlines)
    date_text     = maybe_nfkc(date_text).strip()
    author_text   = maybe_nfkc(author_text).strip()
    category_text = maybe_nfkc(category_text).strip()
    issue_text    = maybe_nfkc(issue_text).strip()

    rec["title"]    = title_text
    rec["text"]     = text_text
    rec["url"]      = url_text
    if date_text:     rec["date"]     = date_text
    if author_text:   rec["author"]   = author_text
    if category_text: rec["category"] = category_text
    if issue_text:    rec["issue"]    = issue_text
    # 互換のため date_primary を空で入れておく（任意）
    rec.setdefault("date_primary", "")

    return rec

def validate_rows(rows: List[Dict[str, Any]], checks: Dict[str, Any]) -> None:
    if checks.get("no_empty_title", False):
        empty = sum(1 for r in rows if not (r.get("title") or "").strip())
        if empty > 0:
            print(f"[ERROR] Empty title count: {empty}", file=sys.stderr)
            sys.exit(1)
    for rule in checks.get("title_contains", []):
        needle = rule.get("needle", "")
        minv   = int(rule.get("min", 1))
        cnt = sum(1 for r in rows if needle in (r.get("title") or ""))
        print(f"[INFO] title contains '{needle}': {cnt}")
        if cnt < minv:
            print(f"[ERROR] '{needle}' count {cnt} < required {minv}", file=sys.stderr)
            sys.exit(1)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-id", required=True)
    ap.add_argument("--token", required=True)
    ap.add_argument("--mapping", default="kb_mapping.yaml")
    ap.add_argument("--out", default="kb.jsonl")
    ap.add_argument("--validate-title-contains", default="")
    args = ap.parse_args()

    with open(args.mapping, "r", encoding="utf-8") as f:
        mapping = yaml.safe_load(f)

    os.environ["_NOTION_TOKEN"] = args.token  # fetch_page_blocks 用

    # Notion 取得
    schema = fetch_db_schema(args.token, args.db_id)
    pages  = query_all_pages(args.token, args.db_id)
    print(f"[INFO] fetched pages: {len(pages)}")

    keep_newlines      = bool(mapping.get("normalize", {}).get("keep_newlines", True))
    do_nfkc            = bool(mapping.get("normalize", {}).get("nfkc", True))
    collapse_blanklines= bool(mapping.get("normalize", {}).get("collapse_blanklines", True))

    rows: List[Dict[str, Any]] = []
    for p in pages:
        rec = build_record(
            page=p, schema=schema, mapping=mapping,
            keep_newlines=keep_newlines, do_nfkc=do_nfkc, collapse_blanklines=collapse_blanklines
        )
        rows.append(rec)

    # 追加のCLIバリデーション（任意）
    if args.validate_title_contains:
        m = re.match(r"^(.*)>=(\d+)$", args.validate_title_contains)
        if m:
            needle, minstr = m.group(1), m.group(2)
            mapping.setdefault("validate", {}).setdefault("title_contains", []).append(
                {"needle": needle, "min": int(minstr)}
            )

    # 検証
    validate_rows(rows, mapping.get("validate", {}))

    # 書き出し（JSONL）
    with open(args.out, "w", encoding="utf-8") as wf:
        for r in rows:
            wf.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n")

    print(f"[INFO] wrote {len(rows)} rows to {args.out}")
    print(f"[INFO] sha256: {sha256_of_file(args.out)}")

if __name__ == "__main__":
    main()
