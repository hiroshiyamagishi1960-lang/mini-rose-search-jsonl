#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
refresh_kb.py — Notion DB → kb.jsonl 生成（講師/著者・資料区分・出典を本文に追記＋添付ファイル情報）

- Notion データベースから全レコードを取得
- プロパティからタイトル / 本文 / 講師/著者 / 資料区分 / 会報号 / 日付 / URL / 添付ファイル を抽出
- 本文の末尾にメタ情報を追記してから kb.jsonl に 1 行 1 レコードで書き出し
- 出典（会報号）は「会報６８号 / 会報68号」のように全角・半角を両方記録
- 添付ファイルは files 配列として、「どのページの」「どのプロパティの」「何番目のファイルか」を記録
"""

import os
import json
from typing import Any, Dict, List, Optional

from notion_client import Client

# ── 環境変数（Actions から渡ってくる値） ──────────────────────────────

NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
DB_ID = os.environ.get("NOTION_DATABASE_ID") or os.environ.get("NOTION_DB_ID")

FIELD_TITLE = os.environ.get("FIELD_TITLE", "").strip()
FIELD_AUTHOR = os.environ.get("FIELD_AUTHOR", "").strip()
FIELD_URL = os.environ.get("FIELD_URL", "").strip()
FIELD_TEXT = os.environ.get("FIELD_TEXT", "").strip()
FIELD_ISSUE = os.environ.get("FIELD_ISSUE", "").strip()
FIELD_DATE = os.environ.get("FIELD_DATE", "").strip()

# 各フィールドの候補名（環境変数優先 → 日本語プロパティ名の候補）
TITLE_CANDS = [FIELD_TITLE, "タイトル", "Name", "名前", "題名"]
BODY_CANDS = [FIELD_TEXT, "講習会等内容", "本文", "内容", "記事", "テキスト"]
AUTHOR_CANDS = [FIELD_AUTHOR, "講師/著者", "講師", "著者", "作者", "筆者"]
CATEGORY_CANDS = ["資料区分", "区分", "カテゴリ", "カテゴリー"]
SOURCE_CANDS = [FIELD_ISSUE, "会報号", "出典", "出典元"]
DATE_CANDS = [FIELD_DATE, "開催日/発行日", "開催日", "発行日", "日付", "Date"]
URL_CANDS = [FIELD_URL, "出典URL", "URL", "url", "リンク"]

# 全角・半角数字の変換テーブル
DIGITS_FULL = "０１２３４５６７８９"
DIGITS_HALF = "0123456789"
TO_FULL = str.maketrans(DIGITS_HALF, DIGITS_FULL)
TO_HALF = str.maketrans(DIGITS_FULL, DIGITS_HALF)


def to_halfwidth_digits(s: str) -> str:
    """全角数字 → 半角数字"""
    return s.translate(TO_HALF)


def to_fullwidth_digits(s: str) -> str:
    """半角数字 → 全角数字"""
    return s.translate(TO_FULL)


def make_both_width_label(raw: str) -> str:
    """
    出典（会報号など）を
      - 会報６８号 だけなら「会報６８号 / 会報68号」
      - 会報68号 だけなら「会報68号 / 会報６８号」
    のように両方書く。
    もともと両方入っている場合や数字が無い場合は raw のまま。
    """
    raw = (raw or "").strip()
    if not raw:
        return ""
    has_full = any(ch in DIGITS_FULL for ch in raw)
    has_half = any(ch in DIGITS_HALF for ch in raw)
    alt: Optional[str] = None
    if has_full and not has_half:
        alt = to_halfwidth_digits(raw)
    elif has_half and not has_full:
        alt = to_fullwidth_digits(raw)
    if alt and alt != raw:
        return f"{raw} / {alt}"
    return raw


def choose_prop(props: Dict[str, Any], cands: List[Optional[str]]) -> Optional[str]:
    """プロパティ名の候補リストの中から、実際に存在するものを 1 つ選ぶ。"""
    for name in cands:
        if not name:
            continue
        if name in props:
            return name
    return None


def get_rich_text_value(prop: Dict[str, Any]) -> str:
    """title / rich_text プロパティをプレーンテキストに変換"""
    t = prop.get("type")
    if t == "title":
        arr = prop.get("title") or []
    elif t == "rich_text":
        arr = prop.get("rich_text") or []
    elif t == "people":
        # people 型は今回は使わない
        return ""
    else:
        # それ以外は可能な範囲で文字列化
        return str(prop.get(t)) if t and t in prop else ""
    out: List[str] = []
    for span in arr:
        text = span.get("plain_text")
        if text:
            out.append(text)
    return "".join(out)


def get_date_value(prop: Dict[str, Any]) -> str:
    """date プロパティから 'YYYY-MM-DD' を取り出す"""
    if prop.get("type") != "date":
        return ""
    v = prop.get("date") or {}
    start = v.get("start")
    if not start:
        return ""
    # 2024-01-02T00:00:00+09:00 → 2024-01-02
    return start[:10]


def extract_fields(page: Dict[str, Any]) -> Dict[str, str]:
    """1 ページ分から必要なフィールドを全部抜き出す"""
    props = page.get("properties", {})

    title_name = choose_prop(props, TITLE_CANDS)
    author_name = choose_prop(props, AUTHOR_CANDS)
    body_name = choose_prop(props, BODY_CANDS)
    cat_name = choose_prop(props, CATEGORY_CANDS)
    src_name = choose_prop(props, SOURCE_CANDS)
    date_name = choose_prop(props, DATE_CANDS)
    url_name = choose_prop(props, URL_CANDS)

    title = get_rich_text_value(props[title_name]) if title_name else ""
    author = get_rich_text_value(props[author_name]) if author_name else ""
    body = get_rich_text_value(props[body_name]) if body_name else ""
    category = get_rich_text_value(props[cat_name]) if cat_name else ""
    source_raw = get_rich_text_value(props[src_name]) if src_name else ""
    date_label = get_date_value(props[date_name]) if date_name else ""

    # URL はプロパティ優先、なければ Notion ページ URL
    url = ""
    if url_name and url_name in props:
        url = get_rich_text_value(props[url_name])
    if not url:
        url = page.get("url", "")

    # 会報号を「全角/半角の両方」に整形
    source_label = make_both_width_label(source_raw)

    # ─ 本文にメタ情報を追記 ─
    meta_lines: List[str] = []
    if author:
        meta_lines.append(f"【講師/著者】{author}")
    if category:
        meta_lines.append(f"【資料区分】{category}")
    if source_label:
        meta_lines.append(f"【出典】{source_label}")

    body_full = (body or "").rstrip()
    if meta_lines:
        meta_block = "\n".join(meta_lines)
        if body_full:
            body_full = body_full + "\n\n" + meta_block
        else:
            body_full = meta_block

    return {
        "title": title or "",
        "body": body_full or "",
        "author": author or "",
        "category": category or "",
        "source": source_raw or "",
        "source_label": source_label or "",
        "date": date_label or "",
        "url": url or "",
    }


def extract_files(page: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    1ページ分から添付ファイル（Files & media）プロパティをすべて抜き出す。

    - properties 内で type == "files" のものを対象にする
    - 各ファイルについて：
        name    : ファイル名（Notion側の表示名）
        page_id : このページのID
        property: プロパティ名（例：「ファイル」「写真・PDF」など）
        index   : そのプロパティ内での順番（0始まり）
    - URL は「すぐ期限切れ」なのでここでは保存しない。
      表示時に /file API から Notion に取りに行く。
    """
    props = page.get("properties", {}) or {}
    page_id = page.get("id")
    out: List[Dict[str, Any]] = []

    if not page_id:
        return out

    for prop_name, prop in props.items():
        if not isinstance(prop, dict):
            continue
        if prop.get("type") != "files":
            continue
        files = prop.get("files") or []
        if not isinstance(files, list):
            continue
        for idx, f in enumerate(files):
            if not isinstance(f, dict):
                continue
            name = f.get("name") or ""
            if not name:
                # name が無いことはあまりないが、念のため URL から補う
                url = ""
                if f.get("type") == "file":
                    url = (f.get("file") or {}).get("url") or ""
                elif f.get("type") == "external":
                    url = (f.get("external") or {}).get("url") or ""
                name = url or f"ファイル{idx+1}"

            out.append(
                {
                    "name": name,
                    "page_id": page_id,
                    "property": prop_name,
                    "index": idx,
                }
            )
    return out


def fetch_all_pages(client: Client, db_id: str) -> List[Dict[str, Any]]:
    """対象データベースから全ページを取得"""
    results: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    while True:
        kwargs: Dict[str, Any] = {"database_id": db_id, "page_size": 100}
        if cursor:
            kwargs["start_cursor"] = cursor
        resp = client.databases.query(**kwargs)
        results.extend(resp.get("results", []))
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
    return results


def main() -> None:
    if not NOTION_TOKEN or not DB_ID:
        raise SystemExit("NOTION_TOKEN / NOTION_DATABASE_ID が設定されていません。")

    client = Client(auth=NOTION_TOKEN)
    pages = fetch_all_pages(client, DB_ID)

    print(f"[INFO] Notion DB から記事を取得中...")
    print(f"[INFO] 取得件数: {len(pages)} 件")

    out_path = "kb.jsonl"
    written = 0

    with open(out_path, "w", encoding="utf-8") as f:
        for page in pages:
            fields = extract_fields(page)
            files = extract_files(page)

            rec = {
                "id": page.get("id"),
                "title": fields["title"],
                "content": fields["body"],
                "body": fields["body"],
                "author": fields["author"],
                "category": fields["category"],
                "category_label": fields["category"] or "",
                "source": fields["source"],
                "source_label": fields["source_label"],
                "date": fields["date"],
                "url": fields["url"],
                # 添付ファイル情報（UI で /file?fid=... に変換して使う）
                "files": files,
            }
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            written += 1

    print(f"[INFO] 書き出し完了: {out_path} ({written} 件)")


if __name__ == "__main__":
    main()
