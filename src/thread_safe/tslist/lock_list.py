import threading

# Constants to help match the Go function return types if needed
ADD_FAILED = -1

class LockList:
    """
    Python equivalent of a thread-safe list using threading.RLock for
    read-write mutual exclusion.
    """
    def __init__(self, *initial):
        self.lock = threading.Lock()
        self.data = [*initial]

    def __get_state__(self):
        state = self.__dict__.copy()
        del state["lock"]
        return state

    def __get_item__(self, item):
        self.at(item)

    def __set_state__(self, state):
        self.__dict__.update(state)
        self.lock = threading.Lock()

    def __len__(self) -> int:
        with self.lock:
            return len(self.data)

    def __getitem__(self, item):
        """Allows instance[i] syntax for accessing elements."""
        val = self.at(item)
        if val is None and (not isinstance(item, int) or item >= len(self) or item < 0):
            raise IndexError("list index out of range")
        return val

    def append(self, item):
        """Adds a single item to the end of the list in a thread-safe manner."""
        with self.lock:
            self.data.append(item)

    def count(self) -> int:
        """Returns the number of elements in the list (as an integer/int64)."""
        return self.__len__()

    def add(self, *items) -> int:
        """Appends items to the list. Returns the previous count, or -1 if no items provided."""
        if not items:
            return ADD_FAILED

        with self.lock:
            # Capture the current index (Go's return value 'c') before appending
            current_count = len(self.data)
            self.data.extend(items)
            return current_count

    def set(self, position: int, value):
        """Sets a value at a specific position if the position is valid."""
        with self.lock:
            if 0 <= position < len(self.data):
                self.data[position] = value

    def at(self, position: int):
        """Retrieves an element at a specific position, or None if out of bounds."""
        with self.lock:
            if 0 <= position < len(self.data):
                return self.data[position]
            return None # Go returns nil/interface{}, Python returns None

    def all(self) -> list:
        """Returns a shallow copy of all elements in the list."""
        with self.lock:
            # Return a copy to ensure thread safety for the *caller's* iteration
            return list(self.data)

    def to_list(self) -> list:
        """Returns the underlying data for JSON serialization."""
        return self.all()

    def __iter__(self):
        return iter(self.all())

