import datetime
import hashlib

from meta_models.created_updated import CreatedUpdated
from meta_models.working import Work, work_stage_index, Working
from postgreslib.engine import Base
from postgreslib.upsert import upsert_entry
from pydantic import BaseModel
from sqlalchemy import Column, Integer, String, DateTime, func, Index, cast, text, select, update, or_, LargeBinary
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declared_attr


class FileData(CreatedUpdated):
    name = Column(String)
    hash = Column(String)
    subtype = Column(Integer, default=0)
    data = Column(LargeBinary, info={"compression": "lz4"})

def file_data_index(cls):
    if not hasattr(cls, "hash"):
        return None

    return Index(f"idx_unique_hash", cls.hash, unique=True)

class UploadedFiles(Base, FileData, Work):
    __tablename__ = "files"
    id = Column(Integer, primary_key=True)

    @classmethod
    def vector(cls):
        pass

    @declared_attr
    def __table_args__(cls):
        return (
            file_data_index(cls),
            work_stage_index(cls, "initial"),
            work_stage_index(cls, "pending"),
        )

def upsert_file(session, name, contents, working: Working, **kwargs):
    hsh = hashlib.md5(contents).hexdigest()
    upsert_entry(session=session,
                 name=name,
                 hash=hsh,
                 model=UploadedFiles,
                 index_elements=["hash"],
                 data=contents,
                 working=working.model_dump(mode="json"),
                 **kwargs)

