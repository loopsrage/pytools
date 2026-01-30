import threading
from typing import Union, Any, Self, Generator

class CIndex:
    """
    A thread-safe manager for multiple nested dictionaries (indexes).
    Uses threading.Lock to ensure concurrent safety across operations,
    mimicking the behavior of Go's sync.Map.
    """

    def __init__(self):
        # The main storage dictionary mapping index names (str) to nested dictionaries (dict)
        self.map: dict = {}
        self.lock = threading.Lock()
        self.index_locks: dict[str, threading.Lock] = {}

    def keys(self, index_name: str) -> list[Any]:
        """Returns a thread-safe snapshot of all keys in a specific index."""
        index_data, index_lock = self.get_index_and_lock(index_name)
        if index_data is None:
            raise KeyError(f"index '{index_name}' does not exist")

        with index_lock:
            return list(index_data.keys())

    def get_index_and_lock(self, index_name: str) -> tuple[dict | None, threading.Lock ]:
        """Helper to safely retrieve the specific index dict and its dedicated lock."""
        with self.lock:
            index_data = self.map.get(index_name)
            index_lock = self.index_locks.get(index_name)

        return index_data, index_lock

    def new(self, index_name: str) -> Self:
        """Creates a new index map."""
        with self.lock:
            if index_name not in self.map:
                self.map[index_name] = {}
                self.index_locks[index_name] = threading.Lock()
        return self

    def load_index(self, index_name: str) -> dict | None:
        """Loads a specific index dictionary (without acquiring its lock)."""
        with self.lock:
            return self.map.get(index_name)

    def store_in_index(self, index_name: str, key, value) -> Union[Any, None]:
        """Stores a key-value pair within a specific index."""
        index_data, index_lock = self.get_index_and_lock(index_name)
        if index_data is None or index_lock is None:
            # If index doesn't exist, create it first (Go behavior)
            self.new(index_name)
            index_data, index_lock = self.get_index_and_lock(index_name)

        with index_lock:
            index_data[key] = value

    def load_or_store_in_index(self, index_name: str, key, value) -> Union[Any, bool]:
        """Loads the value for a key, or stores the new value if the key is absent."""
        index_data, index_lock = self.get_index_and_lock(index_name)
        if index_data is None or index_lock is None:
            self.new(index_name)
            index_data, index_lock = self.get_index_and_lock(index_name)

        with index_lock:
            if key in index_data:
                return index_data[key], True
            index_data[key] = value
            return value, False


    def load_from_index(self, index_name: str, key) -> Union[Any, None]:
        """Loads a value from an index by key."""
        index_data, index_lock = self.get_index_and_lock(index_name)
        if index_data is None:
            raise KeyError(f"index '{index_name}' does not exist")

        with index_lock:
            return index_data.get(key)

    def range_index(self, index_name: str) -> Generator[tuple[Any, Any], Any, None]:
        """Iterates over all key-value pairs in an index, applying a function."""
        index_data, index_lock = self.get_index_and_lock(index_name)
        if index_data is None:
            raise KeyError(f"index '{index_name}' does not exist")

        with index_lock:
            items = index_data.items()

        for key, value in list(items):
            yield key, value


    def delete_index(self, index_name: str) -> None:
        """Deletes an entire index map."""
        with self.lock:
            if index_name in self.map:
                del self.map[index_name]
            if index_name in self.index_locks:
                del self.index_locks[index_name]

    def delete_from_index(self, index_name: str, key) -> None:
        """Deletes a key-value pair from a specific index."""
        index_data, index_lock = self.get_index_and_lock(index_name)
        if index_data is None:
            raise KeyError(f"index '{index_name}' does not exist")

        with index_lock:
            if key in index_data:
                del index_data[key]

    def list_indexes(self) -> list[str]:
        """Returns a list of all index names."""
        with self.lock:
            # Range is equivalent to iterating over keys in a dict
            return list(self.map.keys())