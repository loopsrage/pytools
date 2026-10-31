import hashlib
from datetime import datetime
from enum import Enum
from typing import Optional
from uuid import uuid4

from pydantic import BaseModel, Field, HttpUrl, model_validator


class TopicStatus(str, Enum):
    ACTIVE = "active"
    PAUSED = "paused"
    BLOCKED = "blocked"  # robots.txt blocks scraping


def url_hash(url: str) -> str:
    """Deterministic SHA-256 hash of a URL for page identity."""
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def content_hash(text: str) -> str:
    """SHA-256 hash of page text content for change detection."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


class Topic(BaseModel):
    topic_id: str = Field(default_factory=lambda: str(uuid4()))
    name: str  # the site/domain being scraped
    url: HttpUrl
    status: TopicStatus = TopicStatus.ACTIVE
    scrape_interval_seconds: int = Field(default=3600)
    last_scraped: Optional[datetime] = None
    robots_txt_allowed: bool = True
    created_at: datetime = Field(default_factory=datetime.utcnow)
    metadata: dict = Field(default_factory=dict)


class Page(BaseModel):
    page_id: str = Field(default_factory=lambda: str(uuid4()))
    topic_id: str
    url: HttpUrl
    url_hash: Optional[str] = None  # deterministic SHA-256 of the URL
    title: Optional[str] = None
    content_hash: Optional[str] = None  # SHA-256 of the page text
    text: Optional[str] = None
    stored_path: Optional[str] = None
    scraped_at: datetime = Field(default_factory=datetime.utcnow)

    @model_validator(mode="after")
    def _compute_hashes(self) -> "Page":
        if self.url_hash is None:
            self.url_hash = url_hash(str(self.url))
        if self.content_hash is None and self.text is not None:
            self.content_hash = content_hash(self.text)
        return self


class Artifact(BaseModel):
    artifact_id: str = Field(default_factory=lambda: str(uuid4()))
    topic_id: str
    page_id: Optional[str] = None
    kind: str  # 'csv', 'json', 'document'
    stored_path: str
    row_count: Optional[int] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)


class SearchResult(BaseModel):
    search_id: str = Field(default_factory=lambda: str(uuid4()))
    term: str
    qdrant_point_id: str
    title: Optional[str] = None
    url: Optional[str] = None
    topic_id: Optional[str] = None
    score: Optional[float] = None
    snippet: Optional[str] = None
    new: bool = True
    seen: bool = False
    searched_at: datetime = Field(default_factory=datetime.utcnow)
