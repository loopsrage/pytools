import os
import unittest

from dotenv import load_dotenv
from psycopg import AsyncConnection
from psycopg.rows import DictRow, dict_row
from psycopg_pool import AsyncConnectionPool

from src.indexes.connection_index.connection_index import ConnectionIndex
from src.settings.helper import restore, setting

idx = ConnectionIndex()
load_dotenv()
restore(os.getenv("ENV_FILE"))

class Test(unittest.IsolatedAsyncioTestCase):

    async def test_connection_index(self):
        def get_pool(dsn: str) :
            return AsyncConnectionPool(
                conninfo=dsn,
                min_size=5, max_size=20,
                connection_class=AsyncConnection[DictRow],
                kwargs={"autocommit": True, "row_factory": dict_row}
            )
        pool = get_pool(setting("LocalDatabase", "DSN"))
        try:
            idx.register_connections({
                "postgres": pool
            })
            x = 0
            ld = []
            while x < 100:

                connection, was_loaded = idx.connection("postgres")
                if not was_loaded:
                    print("MOCK OPEN")

                ld.append(was_loaded)
                x=x+1
            assert ld[0]
        finally:
            await pool.close()



    async def test_connection_lazy(self):
        def get_pool(dsn: str) :
            return AsyncConnectionPool(
                conninfo=dsn,
                min_size=5, max_size=20,
                connection_class=AsyncConnection[DictRow],
                kwargs={"autocommit": True, "row_factory": dict_row}
            )

        pool = get_pool(setting("LocalDatabase", "DSN"))
        try:
            x = 0
            ld = []
            while x < 100:
                connection, was_loaded = idx.connection("postgres", pool)
                if not was_loaded:
                    print("MOCK OPEN")

                ld.append(was_loaded)
                x=x+1
            assert not ld[0]
            assert all(ld[1:])
        finally:
            await pool.close()