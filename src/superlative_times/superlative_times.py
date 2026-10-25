import datetime
import threading


class SuperlativeTimes:
    _lock: threading.Lock
    first: int = None
    last: int = None

    def __init__(self):
        self._lock = threading.Lock()

    def set_first_time(self, x: datetime.datetime) -> None:
        with self._lock:
            ts = int(x.timestamp())
            if self.first is None or ts < self.first:
                self.first = ts

    def set_last_time(self, x: datetime.datetime) -> None:
        with self._lock:
            ts = int(x.timestamp())
            if self.last is None or ts > self.last:
                self.last = ts

    @property
    def last_time(self) -> datetime.datetime:
        with self._lock:
            return datetime.datetime.fromtimestamp(self.last)

    @property
    def first_time(self):
        with self._lock:
            return datetime.datetime.fromtimestamp(self.first)

    def set_times(self, x: datetime.datetime) -> None:
        self.set_first_time(x)
        self.set_last_time(x)