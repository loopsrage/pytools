import unittest

from optuna_backend_mongo.optuna_backend import MongoJournalBackend


class InMemoryCursor:
    def __init__(self, docs: list[dict]):
        self._docs = docs

    def sort(self, key: str, direction: int):
        self._docs.sort(key=lambda d: d[key], reverse=(direction < 0))
        return self

    def __iter__(self):
        return iter(self._docs)


class InMemoryCollection:
    def __init__(self):
        self.docs = []

    def insert_one(self, doc: dict):
        self.docs.append(doc)

    def find(self, query: dict):
        gte = query["log_id"]["$gte"]
        matched = [d for d in self.docs if d["log_id"] >= gte]
        return InMemoryCursor(matched)

    def count_documents(self, query: dict) -> int:
        return len(self.docs)


class TestOptunaBackend(unittest.TestCase):

    def test_journal_backend_operations(self):
        backend = MongoJournalBackend.__new__(MongoJournalBackend)
        backend.collection = InMemoryCollection()

        assert backend.get_next_log_id() == 0
        backend.append_logs([{"op": "create_study"}, {"op": "set_param", "x": 1}])
        assert backend.get_next_log_id() == 2
        assert backend.read_logs(0) == [{"op": "create_study"}, {"op": "set_param", "x": 1}]
        assert backend.read_logs(1) == [{"op": "set_param", "x": 1}]


if __name__ == "__main__":
    unittest.main()
