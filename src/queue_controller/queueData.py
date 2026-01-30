import threading
import uuid
from typing import Any
from collections.abc import MutableMapping

from src.thread_safe.index import Index
from src.thread_safe.tslist import TsList

ERRORS_KEY = "error"

class QueueData(MutableMapping):
    _derivative: str = ""
    _index: Index = None
    _trace: TsList = None
    _trace_durations: TsList = None
    _lock: threading.RLock = None
    _uuid: uuid.UUID = None

    def __init__(self):
        self._index = Index().new("")
        self._trace = TsList()
        self._trace_durations = TsList()
        self._lock = threading.RLock()
        self._uuid = uuid.uuid4()

    def __setitem__(self, key, value):
        self.set_attribute(key, value)

    def __delitem__(self, key):
        self._index.delete_from_index(self.derivative, key)

    def __iter__(self):
        return iter(self.kwargs())

    def __len__(self):
        return len(self.kwargs())

    def __getitem__(self, key: Any) -> Any:
        val = self.attribute(key)
        if val is None: raise KeyError(key)
        return val

    def __getstate__(self):
        """Prepare the object for serialization by removing non-serializable fields."""
        state = self.__dict__.copy()
        if "_lock" in state:
            del state["_lock"]
        return state

    def __setstate__(self, state):
        """Restore the object and recreate the thread lock."""
        self.__dict__.update(state)
        self._lock = threading.RLock()

    def load_or_store_attribute(self, attribute, value):
        return self._index.load_or_store_in_index(self.derivative, attribute, value)

    def load_or_store_attribute_from_derivative(self, derivative, attribute, value):
        return self._index.load_or_store_in_index(derivative, attribute, value)

    def set_error(self, error: Exception) -> None:
        self._index.store_in_index(self.derivative, ERRORS_KEY, error)

    def set_attribute(self, attribute: Any, value: Any) -> None:
        self._index.store_in_index(self.derivative, attribute, value)

    def set_attribute_derivative(self, attribute: Any, value: Any, derivative: str) -> None:
        self._index.store_in_index(derivative, attribute, value)

    def kwargs(self) -> dict:
        """Safe snapshot for **kwargs unpacking."""
        all_output = {}
        with self._lock:
            # Even if index is safe, we lock the iteration to prevent
            # the derivative changing mid-loop.
            for i in self._index.list_indexes():
                for key, value in self._index.range_index(i):
                    all_output[key] = value
        return all_output

    def attribute(self, attribute: str) -> Any:
        return self._index.load_from_index(self.derivative, attribute)

    def attributes(self, *names):
        with self._lock:
            return {n: self.attribute(n) for n in names}.values()

    def attribute_from_derivative(self, attribute: str, derivative: str) -> Any:
        return self._index.load_from_index(derivative, attribute)

    def append_trace(self, identity: str) -> None:
        self._trace.add(identity)

    def append_duration(self, duration: float):
        self._trace_durations.add(duration)

    def trace(self) -> list[str]:
        return self._trace.all()

    def trace_duration(self) -> list[str]:
        return self._trace_durations.all()

    @property
    def derivative(self) -> str:
        with self._lock:
            return self._derivative or ""

    @derivative.setter
    def derivative(self, value: str):
        with self._lock:
            self._derivative = value

    def copy_derivative(self, derivative: str) -> 'QueueData':
        new_queue_data = QueueData()
        with self._lock:
            new_queue_data._index = self._index
            current_trace = [self._uuid.hex]
            new_queue_data._derivative = derivative

        new_queue_data._trace.add(*current_trace)
        return new_queue_data


