import re

from sqlalchemy import select, func, and_, or_, true
from sqlalchemy.orm import Session

from postgreslib.engine import Base

def is_any_of(field: str, args: list[int]):
    return {
        "items": [
            {
                "field": field,
                "operator": "isAnyOf",
                "value": [*args]
            }
        ],
        "logicOperator": "and"
    }

def to_csv_file(data, meta: object=None):
    if not meta:
        meta = {}

    return to_binary(data, subtype=20, meta={**meta})

def to_field_value_table(data, meta: object=None):
    if not meta:
        meta = {}

    return to_binary(data, subtype=19, meta={**meta})

def to_data_viewer(data, meta: object=None, read_only: bool = True):
    if not meta:
        meta = {}

    subtype = 0
    if not read_only:
        subtype = 16

    return to_binary(data, subtype=subtype, meta={**meta})


def to_binary(data, subtype=0, meta: object =None):
    if data is None:
        return None

    if isinstance(data, str) and "```" in data:
        data = re.sub(r"```(?:yaml|json|jsonb)?|```", "", data).strip()

    return {
        **meta,
        "Data": data,
        "Subtype": subtype
    }

def mui_datagrid_select_jsonb_array(
        session,
        parent_model,
        parent_id,
        jsonb_column_path,
        filter_model=None,
        sort_model=None,
        offset=0,
        limit=100
):
    t = (
        func.jsonb_array_elements(jsonb_column_path)
        .table_valued("value", with_ordinality="n")
        .render_derived(name="t")
    )

    stmt = (
        select(t.c.n, t.c.value)
        .select_from(parent_model)
        .join(t, true())
        .where(parent_model.id == parent_id)
    )
    if filter_model and "items" in filter_model:
        filters = []
        for item in filter_model["items"]:
            col_name = item["field"]
            operator = item["operator"]
            value = item.get("value")

            target = t.c.value[col_name].astext

            if operator == "contains" and value:
                filters.append(target.ilike(f"%{value}%"))
            elif operator == "equals" and value is not None:
                filters.append(target == str(value))
            elif operator == "startsWith" and value:
                filters.append(target.ilike(f"{value}%"))
            elif operator == "isAnyOf" and isinstance(value, list):
                filters.append(target.in_([str(v) for v in value]))

        if filters:
            linker = and_ if filter_model.get("logicOperator") == "and" else or_
            stmt = stmt.where(linker(*filters))

    count_stmt = select(func.count()).select_from(stmt.subquery())
    total_count = session.execute(count_stmt).scalar()

    if sort_model:
        for sort_item in sort_model:
            col = t.c.value[sort_item["field"]].astext
            stmt = stmt.order_by(col.desc() if sort_item["sort"] == "desc" else col.asc())
    else:
        stmt = stmt.order_by(t.c.n.asc())

    stmt = stmt.offset(offset).limit(limit)
    results = session.execute(stmt).all()

    rows = [{"line": r.n, **r.value} for r in results]
    return {"rows": rows, "total": total_count}

def mui_datagrid_select_many(
        session: Session,
        model: type[Base],
        filter_model: dict = None,
        sort_model: list = None,
        search: str = '',
        offset: int = 0,
        limit: int = 100,
        search_vector = None
):
    stmt = select(model)
    if search and search.strip():
        query = func.phraseto_tsquery('english', search)
        if search_vector is not None:
            stmt = stmt.where(search_vector.op('@@')(query))

    if filter_model and "items" in filter_model:
        filters = []
        for item in filter_model["items"]:
            col_name = item["field"]
            operator = item["operator"]
            value = item.get("value")

            column = getattr(model, col_name)

            if operator == "contains" and value:
                filters.append(column.ilike(f"%{value}%"))
            elif operator == "equals" and value is not None:
                filters.append(column == value)
            elif operator == "startsWith" and value:
                filters.append(column.ilike(f"{value}%"))
            elif operator == "isAnyOf" and isinstance(value, list):
                filters.append(column.in_(value))

        if filters:
            linker = and_ if filter_model.get("logicOperator") == "and" else or_
            stmt = stmt.where(linker(*filters))


    count_stmt = select(func.count()).select_from(stmt.subquery())
    total_count = session.execute(count_stmt).scalar()

    if sort_model:
        order_clauses = []
        for sort_item in sort_model:
            column = getattr(model, sort_item["field"])
            if sort_item["sort"] == "desc":
                order_clauses.append(column.desc().nullslast())
            else:
                order_clauses.append(column.asc().nullslast())
        stmt = stmt.order_by(*order_clauses)
    else:
        stmt = stmt.order_by(model.id.asc())

    stmt = stmt.offset(offset).limit(limit)
    rows = session.execute(stmt).scalars().all()
    return {"rows": rows, "total": total_count}