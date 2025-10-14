import argparse, json, sys, os
from datetime import datetime

def load_kb(path):
    records = []
    if not os.path.exists(path):
        return records
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line=line.strip()
            if not line: 
                continue
            try:
                rec=json.loads(line)
                records.append(rec)
            except Exception:
                continue
    return records

def norm(s): 
    return (s or "").lower()

def get_text(rec):
    parts=[]
    for k in ["content","text","summary","note","body"]:
        if k in rec and rec[k]:
            parts.append(str(rec[k]))
    return " / ".join(parts)

def get_date(rec):
    for k in ["date","updated","created"]:
        if k in rec and rec[k]:
            try:
                # YYYY-MM-DD などを想定
                return datetime.fromisoformat(str(rec[k])[:10])
            except Exception:
                pass
    return datetime.min

def score(rec, tokens):
    text = norm(get_text(rec))
    tags = norm(" ".join(rec.get("tags", []))) if isinstance(rec.get("tags"), list) else norm(str(rec.get("tags","")))
    bag = text + " " + tags
    hit = sum(1 for t in tokens if t and t in bag)
    # 日付が新しいほど微加点
    dt = get_date(rec)
    return hit*1000 + int(dt.timestamp() if dt!=datetime.min else 0)

def pick_matches(kb, ask):
    tokens = [t.strip().lower() for t in ask.split() if t.strip()]
    if not tokens: 
        return sorted(kb, key=get_date, reverse=True)[:7]
    scored = [(score(r, tokens), r) for r in kb]
    scored.sort(key=lambda x: x[0], reverse=True)
    return [r for s,r in scored if s>0][:7] or [r for s,r in scored][:3]

def fmt_id(rec, key): 
    val = rec.get(key)
    return str(val) if val is not None else "-"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--kb", required=True)
    ap.add_argument("--ask", required=True)
    args = ap.parse_args()

    kb = load_kb(args.kb)
    matches = pick_matches(kb, args.ask)

    # セクション出力（既定Bレベルの枠組み。具体値はKB由来）
    print("[最新の答え]")
    if matches:
        top = matches[0]
        # 可能ならKB中の要点をそのまま表示（具体値はKB側に依存）
        print(get_text(top) or "（KBに要点テキストが見つかりませんでした）")
    else:
        print("（KBが空です。Notionからスナップショットを作成してください）")

    print("\n[根拠]")
    if matches:
        top = matches[0]
        print("title:", fmt_id(top, "title"), "/ issue:", fmt_id(top, "issue"), "/ date:", fmt_id(top, "date"), "/ id:", fmt_id(top, "id"))
    else:
        print("-")

    print("\n[過去（年表3–7件）]")
    if matches:
        for rec in matches[:7]:
            date = fmt_id(rec, "date")
            issue= fmt_id(rec, "issue")
            who  = fmt_id(rec, "speaker")
            ttl  = fmt_id(rec, "title")
            print(f"{date}／会報{issue}／{who}／{ttl}")
    else:
        print("-")

    print("\n[補足/代替/リスク]")
    # KBに note / risk 等があれば表示、なければダッシュ
    if matches and any(k in matches[0] for k in ("note","risk","caution")):
        rec = matches[0]
        print(rec.get("note") or rec.get("risk") or rec.get("caution"))
    else:
        print("-")

    print("\n[次の一手]")
    # 操作提案は最小限（仕様変更禁止方針に従い、KB外の一般論は出さない）
    print("1) Notion→最新記録を確認（必要ならKBスナップショットを更新）")
    print("2) 質問を具体化（号数・日付・場所・鉢サイズなど具体値を含める）")

    print("\n[出典]")
    if matches:
        srcs=[]
        for rec in matches[:3]:
            srcs.append(f"{fmt_id(rec,'title')}／{fmt_id(rec,'date')}／会報{fmt_id(rec,'issue')}／id:{fmt_id(rec,'id')}")
        print(" ; ".join(srcs))
    else:
        print("-")

if __name__ == "__main__":
    sys.exit(main())
