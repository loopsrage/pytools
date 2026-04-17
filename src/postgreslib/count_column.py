from sqlalchemy import func
from sqlalchemy.orm import Session

from src.postgreslib.engine import Base


def count_column(session: Session, model: type[Base], column_name, order_column=None):
    col_attr = getattr(model, column_name)
    if not col_attr:
        raise Exception(f"{col_attr} is not defined.")

    if order_column is None:
        order_column = "id"

    order_attr = getattr(model, order_column)
    if not order_attr:
        raise Exception(f"{order_attr} was not found.")

    return session.query(
        col_attr,
        func.count(order_attr).label('count')
    ).group_by(
        col_attr
    ).order_by(
        func.count(order_attr).desc()
    ).all()

