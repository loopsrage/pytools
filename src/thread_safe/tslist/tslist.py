
import threading
from atomicx import AtomicInt

ADD_FAILED = -1

class TsList:
    """
    Thread-safe list using atomic counters for index tracking and
    a lock for data structure stability.
    """
    def __init__(self, *initial):
        self.lock = threading.Lock()
        self.data = list(initial)
        # Initialize the atomic counter with the starting length
        self._count = AtomicInt(len(self.data))

    def __get_state__(self):
        state = self.__dict__.copy()
        # Atomic objects and Locks cannot be pickled; store as raw int
        state["_count_val"] = self._count.load()
        del state["lock"]
        del state["_count"]
        return state

    def __set_state__(self, state):
        count_val = state.pop("_count_val", 0)
        self.__dict__.update(state)
        self.lock = threading.Lock()
        self._count = AtomicInt(count_val)

    def __len__(self) -> int:
        # Atomic load is faster than acquiring a lock
        return self._count.load()

    def count(self) -> int:
        return self.__len__()

    def add(self, *items) -> int:
        """
        Uses atomic fetch-and-add logic to reserve indices.
        Returns the count before items were added.
        """
        num_items = len(items)
        if num_items == 0:
            return ADD_FAILED

        with self.lock:
            # Atomic fetch_add returns the value BEFORE the addition
            prev_count = self._count.add(num_items)
            self.data.extend(items)
            return prev_count

    def append(self, item):
        """Single item append using atomic increment."""
        with self.lock:
            self._count.add(1)
            self.data.append(item)

    def at(self, position: int):
        """Atomic check of bounds before accessing data."""
        # Check current count atomically before attempting locked read
        if 0 <= position < self._count.load():
            with self.lock:
                # Re-check inside lock for safety
                if position < len(self.data):
                    return self.data[position]
        return None

    def set(self, position: int, value):
        if 0 <= position < self._count.load():
            with self.lock:
                if position < len(self.data):
                    self.data[position] = value

    def all(self) -> list:
        with self.lock:
            return list(self.data)

    def to_list(self) -> list:
        return self.all()

    def __getitem__(self, item):
        val = self.at(item)
        if val is None:
            raise IndexError("list index out of range")
        return val

    def __iter__(self):
        return iter(self.all())