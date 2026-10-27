import threading
import unittest

from concurrent_collections import ConcurrentDictionary

from thread_safe.index import Index


class MyTestCase(unittest.TestCase):
    def test_index_initialization(self):
        idx = Index()
        idx.new("users")
        assert "users" in idx.map
        assert isinstance(idx.map["users"], ConcurrentDictionary)

    def test_store_and_load(self):
        idx = Index()
        idx.store_in_index("users", "id_1", {"name": "Alice"})

        # Verify it was stored correctly
        result = idx.load_from_index("users", "id_1")
        assert result["name"] == "Alice"

    def test_delete_logic(self):
        idx = Index()
        idx.store_in_index("users", "id_1", "Alice")
        idx.delete_from_index("users", "id_1")

        assert idx.load_from_index("users", "id_1") is None

    def test_load_or_store_atomic(self):
        idx = Index()

        # First time: stores the value
        val, loaded = idx.load_or_store_in_index("cache", "key1", "first_value")
        assert val == "first_value"
        assert loaded is False

        # Second time: loads the existing value, ignores the new one
        val, loaded = idx.load_or_store_in_index("cache", "key1", "ignored_value")
        assert val == "first_value"
        assert loaded is True

    def test_high_concurrency_race(self):
        idx = Index()
        index_name = "race_test"
        key = "shared_key"

        results = []
        results_lock = threading.Lock()

        def worker(worker_id):
            # Every thread tries to store its own ID
            val, loaded = idx.load_or_store_in_index(index_name, key, worker_id)
            with results_lock:
                results.append((val, loaded))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(100)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Analysis:
        # Exactly one thread should have 'loaded=False'
        stored_count = len([r for r in results if r[1] is False])
        assert stored_count == 1
        assert len(results) == 100

        # All threads should see the same value (the one from the winner)
        winning_val = next(r[0] for r in results if r[1] is False)
        for val, loaded in results:
            assert val == winning_val

    def test_delete_index_and_list_indexes(self):
        idx = Index()
        idx.store_in_index("ns1", "k1", "v1")
        idx.store_in_index("ns2", "k2", "v2")
        assert set(idx.list_indexes()) == {"ns1", "ns2"}
        idx.delete_index("ns1")
        with self.assertRaises(KeyError):
            idx.load_from_index("ns1", "k1")
        assert idx.load_from_index("ns2", "k2") == "v2"
        idx.delete_index("ns2")
        assert list(idx.list_indexes()) == []
    def test_safe_iteration_during_writes(self):
        idx = Index()
        idx.new("stream")

        # Populate initial data
        for i in range(100):
            idx.store_in_index("stream", i, f"val_{i}")

        def aggressive_writer():
            for i in range(100, 200):
                idx.store_in_index("stream", i, f"val_{i}")

        # Start writing in background
        writer_thread = threading.Thread(target=aggressive_writer)
        writer_thread.start()

        # Iterate while writing is happening
        # In ConcurrentDictionary, this returns a point-in-time snapshot
        items = list(idx.range_index("stream"))

        writer_thread.join()

        # The iterator should have captured at least the initial 100 items
        assert len(items) >= 100

    def test_reflect_tools_to_json(self):
        import json

        def sample_tool(x: int, scale: float = 2.0) -> float:
            """Scales integer input by float factor."""
            return float(x) * scale

        idx = Index()
        idx.store_in_index("tools", "sample_tool", sample_tool)
        idx.store_in_index("scalars", "PI", 3.14159)

        reflected = json.loads(idx.reflect_to_json())
        assert reflected["partition_count"] == 2
        tool_meta = reflected["partitions"]["tools"]["entries"]["sample_tool"]
        assert tool_meta["kind"] == "callable"
        assert tool_meta["return_annotation"] == "float"
        assert len(tool_meta["parameters"]) == 2
        assert tool_meta["parameters"][0]["name"] == "x"
        assert tool_meta["parameters"][0]["required"] is True
        assert tool_meta["parameters"][1]["name"] == "scale"
        assert tool_meta["parameters"][1]["required"] is False


if __name__ == '__main__':
    unittest.main()
