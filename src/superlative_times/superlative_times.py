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
            if self.first is None or  self.first < int(x.timestamp()):
                self.first = int(x.timestamp())

    def set_last_time(self, x: datetime.datetime) -> None:
            if self.last is None or self.last > int(x.timestamp()) :
                self.last = int(x.timestamp())

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