import re
import urllib.parse

from pydantic_settings import BaseSettings
from sqlalchemy import create_engine, inspect, Table, MetaData, bindparam, text


class AzureSqlConfig(BaseSettings):
    driver: str
    server: str
    database: str
    username: str
    password: str

    def con_str(self):
        params = self.model_dump()
        cs = (
            f"Driver={params['driver']};"
            f"Server=tcp:{params['server']},1433;"
            f"Database={params['database']};"
            f"UID={params['username']};"
            f"PWD={params['password']};"
            "TrustServerCertificate=yes;"
            "Connection Timeout=30;"
        )
        return cs

class AzureSql:
    engine = None
    def __init__(self, config: AzureSqlConfig):
        params = urllib.parse.quote_plus(config.con_str())
        self.engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

    def inspect_tables(self, schema: str):
        inspector = inspect(self.engine)
        return inspector.get_table_names(schema)

    def inspect_columns(self, table: str, schema: str):
        inspector = inspect(self.engine)
        return inspector.get_columns(table, schema)

    def table_metadata(self, table: str, schema: str) -> Table:
        metadata_obj = MetaData()
        return Table(table, metadata_obj, autoload_with=self.engine, schema=schema)

    def search(self, table: str, primary_key: str = "id", columns: str = "*", order_col: str = None, joins: list = None, filters=None, limit: int = 100, last_item: int = 0):
        tc = f"TOP {limit}" if limit is not None else ""
        query_parts = [f"SELECT {tc} {columns} FROM [{table}] AS base"]
        if joins:
            query_parts.extend(joins)

        query_parts.append(f"WHERE base.[{primary_key}] > :last_id")

        sql_params = {"last_id": last_item}
        bind_params = []
        if filters:
            for key, value in filters.items():
                clean_param = re.sub(r'[^a-zA-Z0-9_]', '', key)
                unique_param = f"f_{clean_param}"
                col_name = key if "." in key else f"base.[{key}]"

                if isinstance(value, (list, tuple)):
                    query_parts.append(f"AND {col_name} IN :{unique_param}")
                    bind_params.append(bindparam(unique_param, expanding=True))
                else:
                    query_parts.append(f"AND {col_name} = :{unique_param}")
                sql_params[unique_param] = value

        if order_col:
            query_parts.append(f"ORDER BY [{order_col}]")

        full_query = text(" ".join(query_parts))

        if bind_params:
            full_query = full_query.bindparams(*bind_params)

        with self.engine.connect() as conn:
            res = conn.execute(full_query, sql_params)
            return [dict(r) for r in res.mappings().all()]