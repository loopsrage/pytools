from sqlalchemy import inspect, String, Text
from sqlalchemy.dialects.postgresql import JSONB

from postgreslib.engine import Base

def get_pk_names(model: type[Base]):
    return [key.name for key in inspect(model).primary_key]

def get_all_unique_columns(model: type[Base]):
    columns = set()
    columns.update(key.name for key in inspect(model).primary_key)
    for constraint in model.__table__.constraints:
        if hasattr(constraint, "columns"):
            columns.update(c.name for c in constraint.columns)

    for index in model.__table__.indexes:
        if index.unique:
            columns.update(c.name for c in index.columns)

    columns.update(c.name for c in model.__table__.columns if c.unique)
    return list(columns)

def extract_model_kwargs(model: type[Base], **kwargs):
    valid_columns = inspect(model).mapper.column_attrs.keys()
    return {k: v for k, v in kwargs.items() if k in valid_columns}

def get_searchable_columns(model):
    mapper = inspect(model)
    return [
        column for column in mapper.columns
        if isinstance(column.type, (String, Text))
    ]

def to_dict(obj):
    return {
        c.name: getattr(obj, c.name)
        for c in obj.__table__.columns
        if not isinstance(c.type, JSONB)
    }

def get_fts_expression(model):
    for index in model.__table__.indexes:
        if index.name == "idx_windchill_fts":
            return index.expressions[0]
    return None