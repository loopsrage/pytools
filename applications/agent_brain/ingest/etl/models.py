from datetime import datetime
from enum import Enum
from typing import List, Optional
from uuid import uuid4

from pydantic import BaseModel, Field


class JobStatus(str, Enum):
    UPLOADED = "uploaded"
    VALIDATED = "validated"
    TRANSFORMED = "transformed"
    SCHEMA_INFERRED = "schema_inferred"
    DEDUPED = "deduped"
    COMPLETED = "completed"
    FAILED = "failed"


class Job(BaseModel):
    job_id: str = Field(default_factory=lambda: str(uuid4()))
    status: JobStatus = JobStatus.UPLOADED
    filename: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    result: Optional[dict] = None
    error: Optional[str] = None


class TransformRequest(BaseModel):
    promoted_columns: List[str] = Field(default_factory=list)
    json_columns: List[str] = Field(default_factory=list)


class DedupeRequest(BaseModel):
    column: str
    threshold: float = Field(default=85.0, ge=0, le=100)


class SchemaResponse(BaseModel):
    model_name: str
    fields: dict[str, str]
    json_schema: dict


class JobListItem(BaseModel):
    job_id: str
    status: JobStatus
    filename: str
    created_at: datetime
