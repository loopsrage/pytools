import json

from sqlalchemy import func, cast
from sqlalchemy.dialects.postgresql import insert, JSONB
from sqlalchemy.orm import Session

from postgreslib.engine import Base
from postgreslib.util import extract_model_kwargs, get_all_unique_columns


def upsert_entry(session: Session, model: type[Base], index_elements: list[str], force_update: set[str] | None = None, **kwargs):
    force_update = force_update or set()
    valid_kwargs = extract_model_kwargs(model, **kwargs)

    stmt = insert(model).values(**valid_kwargs)
    update_dict = {}
    for k, v in valid_kwargs.items():
        if k in index_elements:
            continue

        col_attr = getattr(model, k)

        if k in force_update:
            update_dict[k] = v
        else:
            if isinstance(col_attr.type, JSONB):
                update_val = cast(v, JSONB)
            else:
                update_val = v

            update_dict[k] = func.coalesce(col_attr, update_val)

    if not update_dict:
        return

    update_stmt = stmt.on_conflict_do_update(
        index_elements=index_elements,
        set_=update_dict
    )

    try:
        session.execute(update_stmt)
        session.commit()
    except Exception:
        session.rollback()
        raise