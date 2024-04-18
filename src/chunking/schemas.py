from typing import Optional, Any

from pydantic import BaseModel


class Metadata(BaseModel):
    title: str
    datasource: Optional[str]  # eg. SharePoint, HKM, RKM
    chunk_id: int  # 0,1,2...
    doc_id: str  # document identifier


class Chunk(BaseModel):
    metadata: Metadata
    content: str
    embedding: Any
