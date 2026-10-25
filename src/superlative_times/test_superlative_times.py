import datetime
import threading
import unittest

from superlative_times.superlative_times import SuperlativeTimes


class TestSuperlativeTimes(unittest.TestCase):

    def test_first_and_last_times(self):
        st = SuperlativeTimes()
        t_mid = datetime.datetime(2025, 6, 1, 12, 0, 0)
        t_early = datetime.datetime(2025, 1, 1, 0, 0, 0)
        t_late = datetime.datetime(2025, 12, 31, 23, 59, 59)

        st.set_times(t_mid)
        st.set_times(t_early)
        st.set_times(t_late)

        assert st.first == int(t_early.timestamp())
        assert st.last == int(t_late.timestamp())
        assert st.first_time == datetime.datetime.fromtimestamp(int(t_early.timestamp()))
        assert st.last_time == datetime.datetime.fromtimestamp(int(t_late.timestamp()))

    def test_concurrent_updates(self):
        st = SuperlativeTimes()
        base_ts = 1700000000

        def worker(offset: int):
            dt = datetime.datetime.fromtimestamp(base_ts + offset)
            st.set_times(dt)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(100)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert st.first == base_ts
        assert st.last == base_ts + 99


if __name__ == "__main__":
    unittest.main()
