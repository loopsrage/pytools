import unittest
from unittest import mock
from sqlalchemy import create_engine, text

from awslib.sql import AwsSql, AwsSqlConfig


class TestAwsSqlConfig(unittest.TestCase):

    def test_con_str(self):
        cfg = AwsSqlConfig(
            driver="{ODBC Driver 18 for SQL Server}",
            server="sql.example.com",
            database="prod_db",
            username="sa",
            password="secret",
            region_name="us-east-1",
        )
        cs = cfg.con_str()
        assert "Server=tcp:sql.example.com,1433;" in cs
        assert "Database=prod_db;" in cs
        assert cfg.region_name == "us-east-1"


class TestAwsSql(unittest.TestCase):

    def _db(self):
        db = AwsSql.__new__(AwsSql)
        db.engine = create_engine("sqlite:///:memory:")
        with db.engine.begin() as conn:
            conn.execute(text("CREATE TABLE [items] ([Item] INTEGER PRIMARY KEY, [Description] TEXT)"))
            conn.execute(text("INSERT INTO [items] ([Item], [Description]) VALUES (1, 'A'), (2, 'B'), (3, 'C')"))
        return db

    def test_inspect_tables(self):
        db = self._db()
        assert "items" in db.inspect_tables(schema=None)

    def test_inspect_columns(self):
        db = self._db()
        cols = [c["name"] for c in db.inspect_columns("items", schema=None)]
        assert cols == ["Item", "Description"]

    def test_table_metadata(self):
        db = self._db()
        tbl = db.table_metadata("items", schema=None)
        assert tbl.name == "items"

    def test_search_all(self):
        db = self._db()
        rows = db.search(table="items", primary_key="Item", columns="*", order_col="Item", limit=None, last_item=0)
        assert rows == [
            {"Item": 1, "Description": "A"},
            {"Item": 2, "Description": "B"},
            {"Item": 3, "Description": "C"},
        ]

    def test_search_filters_list(self):
        db = self._db()
        rows = db.search(
            table="items",
            primary_key="Item",
            columns="*",
            order_col="Item",
            filters={"Item": [2, 3]},
            limit=None,
            last_item=1,
        )
        assert rows == [{"Item": 2, "Description": "B"}, {"Item": 3, "Description": "C"}]

    def test_search_limit(self):
        db = self._db()
        rows = db.search(table="items", primary_key="Item", columns="*", order_col="Item", limit=None, last_item=2)
        assert rows == [{"Item": 3, "Description": "C"}]


if __name__ == "__main__":
    unittest.main()
