import json
from typing import List
from .types import KbRecord

def load_snapshot(jsonl_path: str) -> List[KbRecord]:
    records: List[KbRecord] = []
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            obj = json.loads(line)
            rec = KbRecord(
                id=obj.get("id",""),
                title=obj.get("title",""),
                date=obj.get("date"),
                tags=obj.get("tags",[]) or [],
                url=obj.get("url"),
                content_chunks=obj.get("content_chunks",[]) or [],
            )
            records.append(rec)
    return records
