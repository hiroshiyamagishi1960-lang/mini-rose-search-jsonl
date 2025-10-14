# shared/kb_store.py
import json, os
from typing import List, Dict, Any, Tuple

KB_PATH = os.getenv("KB_PATH", "kb.jsonl")  # KBファイルの場所（環境変数＝アプリに渡す設定）

def _load_kb() -> List[Dict[str, Any]]:
    """KBファイルを読み込む（1行1レコードのJSONL形式（扱いやすいログ形式））"""
    items: List[Dict[str, Any]] = []
    if not os.path.exists(KB_PATH):
        return items
    with open(KB_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                items.append(json.loads(line))
            except Exception:
                # 壊れた行は無視（安全第一）
                pass
    return items

def kb_search(query: str, top_k: int = 5) -> Tuple[List[Dict[str, Any]], str]:
    """KBから検索して上位 top_k 件を返す。返り値は（結果リスト, KB更新日時）"""
    kb = _load_kb()
    q = (query or "").lower()

    scored: List[Tuple[int, Dict[str, Any]]] = []
    for rec in kb:
        text = (rec.get("title", "") + " " + rec.get("body", "")).lower()
        score = text.count(q) if q else 0
        if score > 0:
            scored.append((score, rec))

    scored.sort(key=lambda x: x[0], reverse=True)
    results = [rec for _, rec in scored[:max(1, top_k)]]

    # 鮮度（いつ作ったKBか）
    fresh = max((rec.get("updated_at", "") for rec in kb if rec.get("updated_at")), default="")

    return results, fresh
