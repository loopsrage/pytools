import sqlite3
import unittest

from indexes.connection_index.connection_index import ConnectionIndex


class TestConnectionIndex(unittest.TestCase):

    def test_connection_index_register_and_load(self):
        idx = ConnectionIndex()
        conn = sqlite3.connect(":memory:")
        try:
            idx.register_connections({"sqlite": conn})
            ld = []
            for _ in range(20):
                connection, was_loaded = idx.connection("sqlite")
                assert connection is conn
                ld.append(was_loaded)
            assert all(ld)
        finally:
            conn.close()

    def test_connection_lazy(self):
        idx = ConnectionIndex()
        conn = sqlite3.connect(":memory:")
        try:
            ld = []
            for _ in range(20):
                connection, was_loaded = idx.connection("sqlite_lazy", conn)
                assert connection is conn
                ld.append(was_loaded)
            assert not ld[0]
            assert all(ld[1:])
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
