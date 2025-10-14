import argparse, re
from typing import List, Optional
from datetime import datetime
from .snapshot_loader import load_snapshot
from .types import KbRecord

NUM_PAT = re.compile(r"[0-9０-９]+|[%％]|倍|℃|号|ml|ｍｌ|回")

def _score(rec: KbRecord, terms: List[str]) -> int:
    hay = " ".join([rec.title] + rec.tags + rec.content_chunks)
    score = 0
    for t in terms:
        if t in hay:
            score += 3
    if rec.date:
        try:
            dt = datetime.fromisoformat(rec.date)
            score += int(dt.timestamp() // (24*3600)) % 100
        except Exception:
            pass
    return score

def _first_numeric_line(chunks: List[str]) -> Optional[str]:
    for c in chunks:
        if NUM_PAT.search(c):
            return c.strip()
    return chunks[0].strip() if chunks else None

def main():
    ap = argparse.ArgumentParser(description="NewRose KB-only query (builds a draft answer)")
    ap.add_argument("--kb", required=True, help="path to JSONL snapshot")
    ap.add_argument("--query", required=True, help="free text query (e.g., 'ベランダ 8号 施肥')")
    ap.add_argument("--must-tags", action="append", default=[], help="require these tags (repeatable)")
    ap.add_argument("--max-evidence", type=int, default=3)
    ap.add_argument("--max-timeline", type=int, default=5)
    args = ap.parse_args()

    recs = load_snapshot(args.kb)
    terms = [t for t in args.query.split() if t.strip()]

    if args.must_tags:
        recs = [r for r in recs if all(mt in r.tags for mt in args.must_tags)]
    if terms:
        recs = [r for r in recs if any(t in (r.title + " " + " ".join(r.tags) + " " + " ".join(r.content_chunks)) for t in terms)]
    recs_sorted = sorted(recs, key=lambda r: _score(r, terms), reverse=True)

    latest_text = "［KB未記載］"
    evidence_lines: List[str] = []
    timeline_lines: List[str] = []
    sources_lines: List[str] = []

    if recs_sorted:
        top = recs_sorted[0]
        lt = _first_numeric_line(top.content_chunks)
        latest_text = lt if lt else "［KB未記載］"

        for r in recs_sorted[:args.max_evidence]:
            ex = _first_numeric_line(r.content_chunks)
            if not ex:
                continue
            meta = f"〔{r.title}｜{r.date or '—'}｜{','.join(r.tags)}｜{r.url or r.id}〕"
            evidence_lines.append(f"- 「{ex}」{meta}")

        for r in recs_sorted[:args.max_timeline]:
            summ = (r.content_chunks[0].strip() if r.content_chunks else "").replace("\\n"," ")
            if len(summ) > 40:
                summ = summ[:40] + "…"
            timeline_lines.append(f"- {r.date or '—'}｜{r.title} — {summ}")

        for r in recs_sorted:
            sources_lines.append(f"- {r.title}｜{r.date or '—'}｜{','.join(r.tags)}｜{r.url or r.id}")

    print("［最新の答え（詳細）］")
    print(latest_text)
    print("\n［根拠（KB抜粋）］")
    print("\n".join(evidence_lines) if evidence_lines else "（該当抜粋なし）")
    print("\n［過去の記録（年表）］")
    print("\n".join(timeline_lines) if timeline_lines else "（該当履歴なし）")
    print("\n［補足 / 代替 / リスク］")
    print("（必要に応じて追記）")
    print("\n［次の一手 / オプション］")
    print("・原文を開く / 人物:○○ 限定 / 季節:夏だけ")
    print("\n［出典一覧］")
    print("\n".join(sources_lines) if sources_lines else "（該当出典なし）")

if __name__ == "__main__":
    main()
