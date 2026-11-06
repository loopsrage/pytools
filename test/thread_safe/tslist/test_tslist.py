import pickle
import threading
import unittest

from thread_safe.tslist.lock_list import ADD_FAILED as LOCK_ADD_FAILED, LockList
from thread_safe.tslist.tslist import ADD_FAILED as TS_ADD_FAILED, TsList


class TestTsListAndLockList(unittest.TestCase):

    def test_tslist_operations(self):
        lst = TsList(1, 2, None)
        assert len(lst) == 3
        assert lst.count() == 3
        assert lst[0] == 1
        assert lst[2] is None
        assert lst.at(2) is None
        assert lst.at(99) is None

        with self.assertRaises(IndexError):
            _ = lst[3]
        with self.assertRaises(IndexError):
            _ = lst[-1]

        assert lst.add() == TS_ADD_FAILED
        prev = lst.add(4, 5)
        assert prev == 3
        assert len(lst) == 5

        lst.append(6)
        assert lst.all() == [1, 2, None, 4, 5, 6]
        assert lst.to_list() == [1, 2, None, 4, 5, 6]
        assert list(lst) == [1, 2, None, 4, 5, 6]

        lst.set(0, 10)
        assert lst[0] == 10

        restored = pickle.loads(pickle.dumps(lst))
        assert len(restored) == 6
        assert restored.all() == [10, 2, None, 4, 5, 6]

        lst.reset("a", "b")
        assert len(lst) == 2
        assert lst.all() == ["a", "b"]

    def test_lock_list_operations(self):
        lst = LockList("x", None, "z")
        assert len(lst) == 3
        assert lst.count() == 3
        assert lst[0] == "x"
        assert lst[1] is None
        assert lst.at(1) is None
        assert lst.at(10) is None

        with self.assertRaises(IndexError):
            _ = lst[3]
        with self.assertRaises(IndexError):
            _ = lst[-1]

        assert lst.add() == LOCK_ADD_FAILED
        prev = lst.add("w")
        assert prev == 3
        lst.append("q")
        assert lst.all() == ["x", None, "z", "w", "q"]
        assert lst.to_list() == ["x", None, "z", "w", "q"]
        assert list(lst) == ["x", None, "z", "w", "q"]

        lst.set(2, "Z")
        assert lst[2] == "Z"

        restored = pickle.loads(pickle.dumps(lst))
        assert len(restored) == 5
        assert restored.all() == ["x", None, "Z", "w", "q"]

    def test_concurrent_appends(self):
        ts_lst = TsList()
        lk_lst = LockList()

        def worker(tid: int):
            for i in range(50):
                ts_lst.append((tid, i))
                lk_lst.append((tid, i))

        threads = [threading.Thread(target=worker, args=(t,)) for t in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(ts_lst) == 500
        assert len(lk_lst) == 500
        assert len(set(ts_lst.all())) == 500
        assert len(set(lk_lst.all())) == 500


if __name__ == "__main__":
    unittest.main()
