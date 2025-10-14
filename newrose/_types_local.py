from dataclasses import dataclass, field
from typing import List, Optional

@dataclass
class KbRecord:
    id: str
    title: str
    date: Optional[str]
    tags: List[str]
    url: Optional[str]
    content_chunks: List[str] = field(default_factory=list)
