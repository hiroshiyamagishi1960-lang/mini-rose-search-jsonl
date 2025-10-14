import argparse, os
from .snapshot_loader import load_snapshot
from .contracts import render_answer_template

def main():
    ap = argparse.ArgumentParser(description="NewRose minimal CLI")
    ap.add_argument("--kb", required=True, help="path to JSONL snapshot")
    ap.add_argument("--dry-run", action="store_true", help="print stats and exit")
    args = ap.parse_args()

    if not os.path.exists(args.kb):
        print(f"[ERROR] snapshot not found: {args.kb}")
        raise SystemExit(1)

    records = load_snapshot(args.kb)
    print(f"[OK] loaded {len(records)} KB records from {args.kb}")

    if args.dry_run:
        for r in records[:3]:
            print(f"- {r.id} | {r.title} | {r.date} | tags={len(r.tags)} chunks={len(r.content_chunks)}")
        print("\n--- Answer Template Preview ---")
        print(render_answer_template())

if __name__ == "__main__":
    main()
