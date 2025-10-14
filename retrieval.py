# retrieval.py（Notion DB 直検索）
from typing import List, Dict, Any
from shared.notion_db_search import search_database
def search_kb(query: str, k: int = 50) -> List[Dict[str, Any]]:
    return search_database(query, min(k, 50))
