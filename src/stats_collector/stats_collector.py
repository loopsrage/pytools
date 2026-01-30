import datetime

from src.superlative_times.superlative_times import SuperlativeTimes
from src.thread_safe.index import Index
from src.queue_controller.queueData import QueueData
from src.thread_safe.onceler import Onceler

once = Onceler()

class Stats:
    _stats: Index = None

    def __init__(self):
        self._stats = Index()
        self._stats.store_in_index("stats", "superlative_times", SuperlativeTimes())


    def super_times(self) -> SuperlativeTimes:
        return self._stats.load_from_index("stats", "superlative_times")

    def seen_time(self, x: datetime.datetime) -> None:
        st = self.super_times()
        st.set_times(x)

    def counter(self) -> int:
        return self._stats.load_from_index("stats", "counter") or 0

    def set_counter(self, to: int) -> None:
        self._stats.store_in_index("stats", "counter", to)

    def add_counter(self, by: int) -> None:
        self.set_counter(self.counter() + by)

def new_stats() -> Stats:
    return Stats()

def aggregate_action(queue_data: QueueData) -> None:
    st: Stats = once.store_once("STATS", "CREATE", new_stats)
    st.seen_time(datetime.datetime.now())
    if st.counter() % 50 == 0:
        print(st.counter(), st.super_times().first_time, st.super_times().last_time)
    st.add_counter(1)
