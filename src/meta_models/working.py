import datetime

from psycopg2.extensions import JSONB
from pydantic import BaseModel
from sqlalchemy import Column, Index, text, cast, DateTime, Integer, func, select, or_, update


class Working(BaseModel):
    stage: str
    scheduled_for: datetime.datetime | None = None
    work_start: datetime.datetime | None = None
    work_end: datetime.datetime | None = None
    last_seen: datetime.datetime | None = None
    retries: int = 0

class Work:
    working = Column(JSONB, default=dict, info={"compression": "lz4"})

def work_stage_index(cls, stage):
    if not hasattr(cls, "working"):
        return None

    return Index(
        f"idx_{stage}_stage",
        cls.working["stage"].astext,
        text("(working ->> 'scheduled_for') DESC"),
        text("(working ->> 'work_start') DESC"),
        postgresql_where=(cls.working["stage"].astext == stage)
    )


def select_work(session, model, stage,  limit, retries, set_stage = None):
    if not set_stage:
        set_stage = "working"

    if not hasattr(model, "working"):
        raise AttributeError(f"attribute working missing from model.")

    scheduled_for_expr = cast(model.working["scheduled_for"].astext, DateTime(timezone=True))
    work_start_expr = cast(model.working["work_start"].astext, DateTime(timezone=True))
    retries_expr = func.coalesce(cast(model.working["retries"].astext, Integer), 0)

    stmt = (
        select(model)
        .where(
            model.working["stage"].astext == stage,
            retries_expr < retries,
            scheduled_for_expr <= func.now(),
            or_(
                work_start_expr.is_(None),
                work_start_expr == None,
                work_start_expr < func.now() - text("INTERVAL '1 minute'")
            )
        )
        .order_by(
            scheduled_for_expr.desc(),
        )
        .limit(limit)
        .with_for_update(skip_locked=True)
    )

    result = session.execute(stmt).scalars().all()
    section = [s.id for s in result]
    tn = datetime.datetime.now(datetime.timezone.utc)
    if len(section) > 0:
        session.execute(
            update(model)
            .where(model.id.in_(section))
            .values(working=model.working + func.jsonb_build_object(
                "stage", set_stage,
                "work_start", tn.isoformat(),
                "last_seen", tn.isoformat()
            ))
        )
        session.commit()

    return result
