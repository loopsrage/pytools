from datetime import datetime
from enum import Enum
from typing import Optional
from uuid import uuid4

from pydantic import BaseModel, Field, HttpUrl


class ScrapeStatus(str, Enum):
    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"


class ScrapeRequest(BaseModel):
    url: HttpUrl
    wait_until: str = Field(default="networkidle")


class ScrapeResult(BaseModel):
    scrape_id: str
    url: str
    status: ScrapeStatus
    title: Optional[str] = None
    text: Optional[str] = None
    content_hash: Optional[str] = None
    is_duplicate: bool = False
    similarity_to_existing: Optional[float] = None
    created_at: datetime
    stored_path: Optional[str] = None
    error: Optional[str] = None


class ScrapeListItem(BaseModel):
    scrape_id: str
    url: str
    status: ScrapeStatus
    title: Optional[str] = None
    is_duplicate: bool = False
    created_at: datetime
