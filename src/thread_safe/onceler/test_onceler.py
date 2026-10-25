import asyncio
import threading
import unittest
from atomicx import AtomicInt

from thread_safe.onceler import Onceler


class TestOnceler(unittest.IsolatedAsyncioTestCase):

    def test_store_once_single_and_concurrent(self):
        once = Onceler()
        call_count = AtomicInt(0)

        def compute():
            call_count.add(1)
            return {"computed": 42}

        results = []
        lock = threading.Lock()

        def worker():
            res, loaded = once.store_once("ns", "k1", compute)
            with lock:
                results.append((res, loaded))

        threads = [threading.Thread(target=worker) for _ in range(25)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert call_count.load() == 1
        assert len(results) == 25
        assert sum(1 for _, loaded in results if not loaded) == 1
        assert sum(1 for _, loaded in results if loaded) == 24
        for res, _ in results:
            assert res == {"computed": 42}

    def test_store_once_none_and_exception(self):
        once = Onceler()
        calls = AtomicInt(0)

        def compute_none():
            calls.add(1)
            return None

        val1, loaded1 = once.store_once("ns", "none_key", compute_none)
        val2, loaded2 = once.store_once("ns", "none_key", compute_none)
        assert val1 is None and loaded1 is False
        assert val2 is None and loaded2 is True
        assert calls.load() == 1

        def compute_err():
            raise ValueError("boom")

        with self.assertRaises(ValueError):
            once.store_once("ns", "err_key", compute_err)
        with self.assertRaises(ValueError):
            once.store_once("ns", "err_key", compute_err)

    async def test_astore_once_concurrent(self):
        once = Onceler()
        calls = AtomicInt(0)

        async def acompute():
            await asyncio.sleep(0.01)
            calls.add(1)
            return "async_val"

        outcomes = await asyncio.gather(
            *[once.astore_once("ans", "k1", acompute) for _ in range(20)]
        )
        assert calls.load() == 1
        assert sum(1 for _, loaded in outcomes if not loaded) == 1
        assert sum(1 for _, loaded in outcomes if loaded) == 19
        for val, _ in outcomes:
            assert val == "async_val"


if __name__ == "__main__":
    unittest.main()
