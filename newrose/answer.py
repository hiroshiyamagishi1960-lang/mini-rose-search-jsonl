#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import sys
from pathlib import Path
from typing import List, Dict, Any
import textwrap
import re

# ----------------------
# KB I/O
# ----------------------
def load_kb(kb_path: str) -> List[Dict[str, Any]]:
    p = Path(kb_path)
    if not p.exists():
        sys.exit(f"KBファイルが見つかりません: {kb_path}")
    items = []
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                items.append(obj)
            except Exception as e:
                print(f"[WARN] JSONLの1行を読めませんでした: {e}", file=sys.stderr)
    if not items:
        print("[WARN] KBに記事がありません。", file=sys.stderr)
    return items

# 安全に取り出し
def g(o: Dict[str, Any], k: str, default: str = "") -> str:
    v = o.get(k, default)
    return v if isinstance(v, str) else default

def show_article(items: List[Dict[str, Any]], art_id: str) -> None:
    hit = None
    for o in items:
        if g(o, "id") == art_id:
            hit = o
            break
    if not hit:
        sys.exit(f"該当記事が見つかりません: id={art_id}")

    title = g(hit, "title", "")
    date  = g(hit, "date", "")
    issue = g(hit, "issue", "")
    content = g(hit, "content", "")

    header = f"""【タイトル】 {title}
【日付】 {date}    【会報】 {issue}    【id】 {art_id}
{"-"*48}
"""
    print(header)
    if content:
        print(content)
    else:
        print("（本文未格納。Notionから本文付きでスナップショットしてください）")

    print("\n【出典】")
    print(f"{title}／{date}／会報／id:{art_id}")

# ----------------------
# とても簡易な検索 & 生成ダミー
# ----------------------
def tokenize(s: str) -> List[str]:
    # 超簡易：日本語はスペースで切れないので、ひらがな/カタカナ/漢字/英数を連続で拾う
    return re.findall(r"[0-9A-Za-zぁ-んァ-ヶ一-龠々ー]+", s)

def search_kb(items: List[Dict[str, Any]], query: str, topk: int = 5) -> List[Dict[str, Any]]:
    q_tokens = tokenize(query)
    scores = []
    for o in items:
        text = " ".join([g(o,"title",""), g(o,"content","")])
        t_tokens = tokenize(text)
        # スコア：共通トークンの個数
        score = sum(t_tokens.count(t) for t in q_tokens)
        if score > 0:
            scores.append((score, o))
    scores.sort(key=lambda x: x[0], reverse=True)
    return [o for _, o in scores[:topk]]

def synth_answer(items: List[Dict[str, Any]], question: str) -> None:
    hits = search_kb(items, question, topk=5)
    print("【質問】", question)
    print()
    if not hits:
        print("【最新の答え】")
        print("KBに関連しそうな記事が見つかりませんでした。条件を具体化するか、KBへ記事を追加してから再実行してください。")
        return

    print("【最新の答え（暫定）】")
    # すごく簡易な要約：上位ヒットの冒頭数行を拾う
    bullet = []
    for i, o in enumerate(hits, 1):
        content = g(o, "content", "")
        snippet = textwrap.shorten(content.replace("\n","　"), width=120, placeholder="…") if content else "（本文未格納）"
        bullet.append(f"{i}. {g(o,'title','（無題）')}：{snippet}")
    print("\n".join(bullet[:3]))
    print()
    print("【根拠（KB）】")
    for o in hits[:5]:
        print(f"- {g(o,'title')}／{g(o,'date')}／id:{g(o,'id')}")

def plan_answer(items: List[Dict[str, Any]], question: str) -> None:
    hits = search_kb(items, question, topk=5)
    print("【依頼】", question)
    print()
    print("【計画（ドラフト）】")
    steps = [
        "1) 条件の明確化（対象・目的・時期・制約）",
        "2) KBの近似事例を確認（同条件・同目的）",
        "3) 必要資材と注意点の洗い出し",
        "4) 当日の手順（開始〜終了までの段取り）",
        "5) リスクと代替案（高温/低温・時間不足・資材欠品時）",
        "6) 記録テンプレ（観察項目／日付／写真／タグ）"
    ]
    print("\n".join(steps))
    print()
    print("【参考（KB）】")
    if hits:
        for o in hits[:5]:
            print(f"- {g(o,'title')}／{g(o,'date')}／id:{g(o,'id')}")
    else:
        print("（関連記事が見つかりませんでした）")

# ----------------------
# CLI
# ----------------------
def main():
    ap = argparse.ArgumentParser(prog="answer.py")
    ap.add_argument("--kb", required=True, help="KB(JSONL)")
    ap.add_argument("--id", help="show で表示する記事ID")
    ap.add_argument("--mode", choices=["show","synth","plan"], default="show")
    ap.add_argument("--ask", help="synth/plan 用の自由質問文")
    args = ap.parse_args()

    items = load_kb(args.kb)

    if args.mode == "show":
        if not args.id:
            sys.exit("show モードでは --id が必要です。")
        show_article(items, args.id)
        return

    if args.mode == "synth":
        q = args.ask or ""
        if not q.strip():
            sys.exit("synth モードでは --ask が必要です。")
        synth_answer(items, q)
        return

    if args.mode == "plan":
        q = args.ask or ""
        if not q.strip():
            sys.exit("plan モードでは --ask が必要です。")
        plan_answer(items, q)
        return

if __name__ == "__main__":
    main()

