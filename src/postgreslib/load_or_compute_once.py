import asyncio
import re
from asyncio import iscoroutinefunction

from sqlalchemy import func, cast
from sqlalchemy.dialects.postgresql import insert, JSONB

from postgreslib.datagrid_adapter import to_binary
from postgreslib.engine import Base
from src.postgreslib.util import  extract_model_kwargs


async def load_or_compute_once(model: type[Base], column, callback, filter_value, filter_column, **kwargs):
    session = kwargs.get("session")
    valid_kwargs = extract_model_kwargs(model, **kwargs)

    actual_filter_col = getattr(model, filter_column)

    check_field = getattr(model, column)
    if isinstance(actual_filter_col.type, JSONB):
        condition = actual_filter_col.contains(filter_value)
    else:
        condition = (actual_filter_col == filter_value)

    existing_val = (session.query(check_field)
                    .filter(condition)
                    .scalar())

    if existing_val is not None:
        return existing_val

    if iscoroutinefunction(callback):
        new_value = await callback(**kwargs)
    else:
        new_value = await asyncio.to_thread(callback(**kwargs))

    if new_value is None:
        raise ValueError(f"Callback for {column} returned None")

    if isinstance(new_value, str) and "```" in new_value:
        new_value = re.sub(r"```(?:yaml|json)?|```", "", new_value).strip()

    new_value = to_binary(new_value)

    valid_kwargs[column] = new_value
    insert_vals = {
        k: (cast(v, JSONB) if isinstance(v, (dict, list)) else v)
        for k, v in valid_kwargs.items()
    }

    update_dict = {}
    for k, v in valid_kwargs.items():
        if isinstance(v, (dict, list)):
            update_dict[k] = func.coalesce(getattr(model, k), cast(v, JSONB))
        else:
            update_dict[k] = func.coalesce(getattr(model, k), v)

    stmt = insert(model).values(**insert_vals)
    update_stmt = stmt.on_conflict_do_update(
        index_elements=["item_id"],
        set_=update_dict
    ).returning(check_field)

    try:
        result = session.execute(update_stmt)
        session.commit()
        return result.scalar()
    except Exception as e:
        session.rollback()
        raise e