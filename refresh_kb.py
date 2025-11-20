#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
refresh_kb.py — Notion DB → kb.jsonl 生成（インラインDB/フルページDB 両対応）

- インラインDBでもフルページDBでも動作（Notion APIはDB IDが同じ扱い）
- 列名の日本語差異を緩く吸収（envで明示も可）
- title / date / url / body を抽出し、検索用の JSONL を生成
- 生成物: kb.jsonl（UTF-8 / 1行1レコード）

★今回の追加ポイント
- Notion の「講師/著者」「資料区分」「出典」を取得し、
  本文末尾に
    【講師/著者】…
    【資料区分】…
    【出典】…
  を追記する。
- 【出典】については、元の文字列に加えて
    ・数字を半角にした版
    ・数字を全角にした版
  を重複なしで並べる。
  例）「会報６８号」 → 「会報６８号 / 会報68号」
      「会報68号」   → 「会報68号 / 会報６８号」
- これにより、「会報６８号」「会報68号」のどちらで検索してもヒットしやすくする。
- app.py 側のロジックは一切変更しない。

環境変数（必要/任意）:
  NOTION_TOKEN            : 必須（Notion統合のシークレット）
  NOTION_DATABASE_ID      : 必須（32桁のDB ID）
  FIELD_TITLE             : 任意（既定：自動検出 "type=title"）
  FIELD_BODY              : 任意（既定：候補から自動検出）
  FIELD_DATE              : 任意（既定：type=date の最初の列）
  FIELD_URL               : 任意（既定：候補: 出典URL, URL 等）
  FIELD_TAGS              : 任意（既定：候補: タグ, Tags 等）
  FIELD_AUTHOR            : 任意（既定：候補: 講師/著者 等）
  FIELD_CATEGORY          : 任意（既定：候補: 資料区分 等）
  FIELD_SOURCE            : 任意（既定：候補: 出典 等）
  FIELD_ISSUE             : 任意（既定：候補: 会報号, 号 等）
"""

import os
import json
import sys
import re
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple

import requests

NOTION_API_BASE = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"


# ----------------------------------------------------------------------
# 共通ユーティリティ
# ----------------------------------------------------------------------
def getenv(name: str, default: Optional[str] = None, required: bool = False) -> Optional[str]:
    value = os.getenv(name, default)
    if required and not value:
        print(f"[ERROR] 環境変数 {name} が設定されていません。", file=sys.stderr)
        sys.exit(1)
    return value or None


def join_rich_text(blocks: List[Dict[str, Any]]) -> str:
    parts: List[str] = []
    for b in blocks:
        if "plain_text" in b:
            parts.append(b["plain_text"])
        elif "text" in b and isinstance(b["text"], dict) and "content" in b["text"]:
            parts.append(b["text"]["content"])
    return "".join(parts).strip()


def notion_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json; charset=utf-8",
    }


def query_database_all(token: str, database_id: str) -> List[Dict[str, Any]]:
    """Notion データベースを最後まで query して全行を返す。"""
    url = f"{NOTION_API_BASE}/databases/{database_id}/query"
    headers = notion_headers(token)

    results: List[Dict[str, Any]] = []
    payload: Dict[str, Any] = {}
    cursor: Optional[str] = None

    while True:
        if cursor:
            payload["start_cursor"] = cursor
        resp = requests.post(url, headers=headers, data=json.dumps(payload))
        if resp.status_code != 200:
            print("[ERROR] Notion query failed:", resp.status_code, resp.text, file=sys.stderr)
            sys.exit(1)
        data = resp.json()
        results.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")

    return results


# ----------------------------------------------------------------------
# 数字の全角/半角変換ヘルパ
# ----------------------------------------------------------------------
def to_halfwidth_digits(s: str) -> str:
    """全角数字（０〜９）を半角（0-9）に変換する。その他の文字はそのまま。"""
    if not s:
        return s
    res: List[str] = []
    for ch in s:
        code = ord(ch)
        if 0xFF10 <= code <= 0xFF19:
            res.append(chr(code - 0xFF10 + ord("0")))
        else:
            res.append(ch)
    return "".join(res)


def to_fullwidth_digits(s: str) -> str:
    """半角数字（0-9）を全角（０〜９）に変換する。その他の文字はそのまま。"""
    if not s:
        return s
    res: List[str] = []
    for ch in s:
        if "0" <= ch <= "9":
            res.append(chr(ord(ch) - ord("0") + 0xFF10))
        else:
            res.append(ch)
    return "".join(res)


# ----------------------------------------------------------------------
# プロパティ抽出ヘルパ
# ----------------------------------------------------------------------
def get_first_property_of_type(properties: Dict[str, Any], prop_type: str) -> Optional[Tuple[str, Dict[str, Any]]]:
    for name, p in properties.items():
        if p.get("type") == prop_type:
            return name, p
    return None


def get_property_by_candidates(
    properties: Dict[str, Any],
    candidates: List[str],
    allowed_types: Optional[List[str]] = None,
) -> Optional[Tuple[str, Dict[str, Any]]]:
    for cand in candidates:
        if cand in properties:
            p = properties[cand]
            if allowed_types is None or p.get("type") in allowed_types:
                return cand, p
    return None


def extract_text_prop(p: Dict[str, Any]) -> str:
    t = p.get("type")
    if t == "title":
        return join_rich_text(p.get("title", []))
    if t == "rich_text":
        return join_rich_text(p.get("rich_text", []))
    if t == "select":
        opt = p.get("select")
        if opt:
            return opt.get("name", "").strip()
        return ""
    if t == "multi_select":
        opts = p.get("multi_select", [])
        return " / ".join(o.get("name", "").strip() for o in opts if o.get("name"))
    if t == "url":
        return p.get("url") or ""
    return ""


def extract_date_prop(p: Dict[str, Any]) -> Optional[str]:
    if p.get("type") != "date":
        return None
    d = p.get("date")
    if not d:
        return None
    start = d.get("start")
    if not start:
        return None
    # YYYY-MM-DD または ISO 形式を YYYY-MM-DD に揃える
    try:
        if len(start) >= 10:
            return start[:10]
    except Exception:
        return None
    return None


def extract_tags_prop(p: Dict[str, Any]) -> List[str]:
    if p.get("type") != "multi_select":
        return []
    return [o.get("name", "").strip() for o in p.get("multi_select", []) if o.get("name")]


def pick_property(
    properties: Dict[str, Any],
    env_name: str,
    default_candidates: List[str],
    allowed_types: Optional[List[str]] = None,
) -> Optional[Tuple[str, Dict[str, Any]]]:
    # 1) env で明示されている場合は最優先
    env_value = getenv(env_name)
    if env_value and env_value in properties:
        p = properties[env_value]
        if allowed_types is None or p.get("type") in allowed_types:
            return env_value, p

    # 2) 候補名から探す
    cand = get_property_by_candidates(properties, default_candidates, allowed_types)
    if cand:
        return cand

    # 3) allowed_types が指定されていれば、その type の最初のプロパティ
    if allowed_types:
        for name, p in properties.items():
            if p.get("type") in allowed_types:
                return name, p

    return None


# ----------------------------------------------------------------------
# 1ページ分のレコード作成
# ----------------------------------------------------------------------
TITLE_CANDIDATES = ["タイトル", "Title", "名前", "Name"]
BODY_CANDIDATES = ["講習会等内容", "本文", "内容", "Notes", "note", "メモ"]
DATE_CANDIDATES = ["開催日／発行日", "開催日", "発行日", "日付", "Date"]
URL_CANDIDATES = ["出典URL", "URL", "Url", "Link"]
TAGS_CANDIDATES = ["タグ", "Tags", "Tag"]

AUTHOR_CANDIDATES = ["講師／著者", "講師/著者", "講師", "著者", "Author", "作者"]
CATEGORY_CANDIDATES = ["資料区分", "区分", "カテゴリ", "Category", "種別"]
SOURCE_CANDIDATES = ["出典", "Source", "媒体"]
ISSUE_CANDIDATES = ["会報号", "号", "Issue"]


def make_record(page: Dict[str, Any]) -> Dict[str, Any]:
    props: Dict[str, Any] = page.get("properties", {})

    # タイトル
    title_name, title_prop = pick_property(
        props, "FIELD_TITLE", TITLE_CANDIDATES, allowed_types=["title"]
    ) or (None, None)
    title = extract_text_prop(title_prop) if title_prop else ""

    # 本文
    body_name, body_prop = pick_property(
        props, "FIELD_BODY", BODY_CANDIDATES, allowed_types=["rich_text", "title"]
    ) or (None, None)
    body_text = extract_text_prop(body_prop) if body_prop else ""

    # 日付
    date_name, date_prop = pick_property(
        props, "FIELD_DATE", DATE_CANDIDATES, allowed_types=["date"]
    ) or (None, None)
    date_label = extract_date_prop(date_prop) if date_prop else None
    date_sort = 0
    if date_label:
        try:
            date_sort = int(date_label.replace("-", ""))
        except Exception:
            date_sort = 0

    # URL（出典URL）
    url_name, url_prop = pick_property(
        props, "FIELD_URL", URL_CANDIDATES, allowed_types=["url"]
    ) or (None, None)
    url = extract_text_prop(url_prop) if url_prop else ""
    if not url:
        # Notion ページの URL を最後の手段として使う
        url = page.get("url", "")

    # タグ
    tags_name, tags_prop = pick_property(
        props, "FIELD_TAGS", TAGS_CANDIDATES, allowed_types=["multi_select"]
    ) or (None, None)
    tags = extract_tags_prop(tags_prop) if tags_prop else []

    # ★ メタ情報（講師/著者・資料区分・出典・会報号）
    author_name, author_prop = pick_property(
        props, "FIELD_AUTHOR", AUTHOR_CANDIDATES,
        allowed_types=["rich_text", "select", "multi_select"]
    ) or (None, None)
    author = extract_text_prop(author_prop) if author_prop else ""

    category_name, category_prop = pick_property(
        props, "FIELD_CATEGORY", CATEGORY_CANDIDATES,
        allowed_types=["rich_text", "select", "multi_select"]
    ) or (None, None)
    category = extract_text_prop(category_prop) if category_prop else ""

    source_name, source_prop = pick_property(
        props, "FIELD_SOURCE", SOURCE_CANDIDATES,
        allowed_types=["rich_text", "select", "multi_select"]
    ) or (None, None)
    source = extract_text_prop(source_prop) if source_prop else ""

    issue_name, issue_prop = pick_property(
        props, "FIELD_ISSUE", ISSUE_CANDIDATES,
        allowed_types=["rich_text", "select", "multi_select"]
    ) or (None, None)
    issue_label = extract_text_prop(issue_prop) if issue_prop else ""

    issue_sort = 0
    if issue_label:
        m = re.search(r"(\d+)", issue_label)
        if m:
            try:
                issue_sort = int(m.group(1))
            except Exception:
                issue_sort = 0

    # ★ 本文末尾にメタ情報を追記する
    meta_lines: List[str] = []
    if author:
        meta_lines.append(f"【講師/著者】{author}")
    if category:
        meta_lines.append(f"【資料区分】{category}")
    if source:
        # 出典は「元 / 半角数字版 / 全角数字版」の重複なしリストを作る
        variants: List[str] = []
        for v in [source, to_halfwidth_digits(source), to_fullwidth_digits(source)]:
            if v and v not in variants:
                variants.append(v)
        meta_lines.append("【出典】" + " / ".join(variants))

    if meta_lines:
        if body_text:
            body_text = body_text.rstrip() + "\n\n" + "\n".join(meta_lines)
        else:
            body_text = "\n".join(meta_lines)

    # Notion メタ
    created_time = page.get("created_time")
    last_edited_time = page.get("last_edited_time")

    record: Dict[str, Any] = {
        "id": page.get("id"),
        "title": title,
        # 本文は body / content 両方のキーに入れておく（後方互換のため）
        "body": body_text,
        "content": body_text,
        "url": url,
        "tags": tags,

        "author": author,
        "author_label": author,
        "category": category,
        "category_label": category,
        "source": source,
        "source_label": source,
        "issue": issue_label,
        "issue_label": issue_label,

        "date": date_label,
        "date_label": date_label,

        "date_sort": date_sort,
        "issue_sort": issue_sort,

        "created_time": created_time,
        "last_edited_time": last_edited_time,
    }

    return record


# ----------------------------------------------------------------------
# メイン処理
# ----------------------------------------------------------------------
def main() -> None:
    token = getenv("NOTION_TOKEN", required=True)
    database_id = getenv("NOTION_DATABASE_ID", required=True)

    print("[INFO] Notion DB から記事を取得中...", file=sys.stderr)
    pages = query_database_all(token, database_id)
    print(f"[INFO] 取得件数: {len(pages)} 件", file=sys.stderr)

    records: List[Dict[str, Any]] = []
    for page in pages:
        try:
            rec = make_record(page)
            if rec.get("title") or rec.get("body"):
                records.append(rec)
        except Exception as e:
            print("[WARN] レコード変換に失敗しました:", e, file=sys.stderr)

    # kb.jsonl に書き出し
    out_path = "kb.jsonl"
    with open(out_path, "w", encoding="utf-8", newline="\n") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False))
            f.write("\n")

    print(f"[INFO] 書き出し完了: {out_path} （{len(records)} 件）", file=sys.stderr)


if __name__ == "__main__":
    main()
