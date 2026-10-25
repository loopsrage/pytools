import unittest
from sqlalchemy import create_engine, text

from azurelib.sql import AzureSql, AzureSqlConfig


class TestAzureSql(unittest.TestCase):

    def test_azure_sql_config_and_search(self):
        cfg = AzureSqlConfig(
            driver="{ODBC Driver 18 for SQL Server}",
            server="sql.example.com",
            database="prod_db",
            username="sa",
            password="secret",
        )
        cs = cfg.con_str()
        assert "Server=tcp:sql.example.com,1433;" in cs
        assert "Database=prod_db;" in cs

        db = AzureSql.__new__(AzureSql)
        db.engine = create_engine("sqlite:///:memory:")
        with db.engine.begin() as conn:
            conn.execute(text("CREATE TABLE [items] ([Item] INTEGER PRIMARY KEY, [Description] TEXT)"))
            conn.execute(text("INSERT INTO [items] ([Item], [Description]) VALUES (1, 'A'), (2, 'B'), (3, 'C')"))

        assert "items" in db.inspect_tables(schema=None)
        cols = [c["name"] for c in db.inspect_columns("items", schema=None)]
        assert cols == ["Item", "Description"]
        tbl = db.table_metadata("items", schema=None)
        assert tbl.name == "items"

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


if __name__ == "__main__":
    unittest.main()
