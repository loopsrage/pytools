import datetime

from sqlalchemy import Column, func, DateTime


class CreatedUpdated:
    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        default=datetime.datetime.now
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        default=datetime.datetime.now
    )