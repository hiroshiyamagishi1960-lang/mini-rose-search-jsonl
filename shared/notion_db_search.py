# shared/notion_db_search.py
import os
from typing import Any, Dict, List, Optional
from notion_client import Client
from dotenv import load_dotenv
load_dotenv()
TOKEN = os.getenv("NOTION_TOKEN")
DBID  = os.getenv("NOTION_DATABASE_ID")
client: Optional[Client] = Client(auth=TOKEN) if TOKEN else None
CANDS = {
    "title": ["タイトル","名前","Name","題名"],
    "issue": ["会報","会報号","号","Issue"],
    "published": ["発行日","会報等日付","投稿日","Published","Date"],
    "event_date": ["開催日","Event","日付"],
    "author": ["著者","講師","Author"],
    "body": ["本文","講義内容","内容","Body","概要","説明","Description"]
}
def _take_text(v: Dict[str,Any]):
    if not v: return None
    t=v.get("type")
    if t in ("title","rich_text"):
        arr=v.get(t,[])
        s="".join([x.get("plain_text","") for x in arr]).strip()
        return s or None
    if t=="date":
        d=v.get("date") or {}
        return (d.get("start") or "") or None
    return None
def _get_title(props: Dict[str,Any]) -> str:
    for _,v in props.items():
        if v.get("type")=="title":
            arr=v.get("title",[])
            s="".join([x.get("plain_text","") for x in arr]).strip()
            if s: return s
    for name in CANDS["title"]:
        s=_take_text(props.get(name))
        if s: return s
    return ""
def _find_first(props: Dict[str,Any], keys):
    for k in keys:
        s=_take_text(props.get(k))
        if s: return s
    return None
def _extract(page: Dict[str,Any]) -> Dict[str,Any]:
    props=page.get("properties",{})
    return {
        "title": _get_title(props) or page.get("id","")[:8],
        "url": page.get("url",""),
        "source": "Notion",
        "issue": _find_first(props, CANDS["issue"]),
        "published": _find_first(props, CANDS["published"]),
        "event_date": _find_first(props, CANDS["event_date"]),
        "author": _find_first(props, CANDS["author"]),
        "body": _find_first(props, CANDS["body"]),
    }
def search_database(query: str, top_k: int=10) -> List[Dict[str,Any]]:
    if not client or not DBID:
        return []
    ors=[]
    for keylist in (CANDS["title"], CANDS["body"], CANDS["author"]):
        for name in keylist:
            ors.append({"property": name, "rich_text": {"contains": query}})
    filt={"or": ors} if ors else None
    res=client.databases.query(database_id=DBID, page_size=min(max(top_k,1),100), filter=filt)
    return [_extract(pg) for pg in res.get("results",[])]
