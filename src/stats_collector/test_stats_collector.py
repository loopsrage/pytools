import datetime
import unittest

from queue_controller.queueData import QueueData
from stats_collector.stats_collector import Stats, aggregate_action, once


class TestStatsCollector(unittest.TestCase):

    def test_stats_and_aggregate_action(self):
        st = Stats()
        assert st.counter() == 0
        st.add_counter(5)
        assert st.counter() == 5

        t1 = datetime.datetime(2025, 1, 1, 0, 0, 0)
        t2 = datetime.datetime(2025, 6, 1, 0, 0, 0)
        st.seen_time(t2)
        st.seen_time(t1)
        assert st.super_times().first == int(t1.timestamp())
        assert st.super_times().last == int(t2.timestamp())

        # Test aggregate_action uses Onceler singleton
        aggregate_action(QueueData())
        singleton_stats, loaded = once.store_once("STATS", "CREATE", Stats)
        assert loaded is True
        assert singleton_stats.counter() >= 1


if __name__ == "__main__":
    unittest.main()
