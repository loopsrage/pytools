from concurrent_collections import ConcurrentDictionary
from typing import Any, Generator

class Index:
    """
    A thread-safe manager for multiple nested dictionaries using
    ConcurrentDictionary. Mimics Go's sync.Map for arbitrary objects.
    """
    def __init__(self):
        # Main map stores sub-index ConcurrentDictionary objects
        self.map = ConcurrentDictionary()

    def new(self, index_name: str) -> 'ConcurrentIndex':
        """Creates a new nested concurrent dictionary atomically."""
        # put_if_absent returns the existing value or adds the new one
        self.map.put_if_absent(index_name, ConcurrentDictionary())
        return self

    def store_in_index(self, index_name: str, key: Any, value: Any):
        """Stores a key-value pair. Sub-index creation is handled atomically."""
        if index_name not in self.map:
            self.new(index_name)

        target_index = self.map[index_name]
        # Atomic assignment (standard [] syntax is supported but assign_atomic is clearer)
        target_index.assign_atomic(key, value)

    def load_or_store_in_index(self, index_name: str, key: Any, value: Any) -> tuple[Any, bool]:
        """Equivalent to Go's LoadOrStore. Returns (value, loaded_flag)."""
        if index_name not in self.map:
            self.new(index_name)

        target_index = self.map[index_name]
        # Returns existing value if present, or None if the new value was stored
        existing = target_index.put_if_absent(key, value)

        if existing is not None:
            return existing, True
        return value, False

    def load_from_index(self, index_name: str, key: Any) -> Any:
        """Atomically retrieves a value from an index."""
        target_index = self.map.get(index_name)
        if target_index is None:
            raise KeyError(f"index '{index_name}' does not exist")
        return target_index.get(key)

    def range_index(self, index_name: str) -> Generator[tuple[Any, Any], None, None]:
        """Iterates over a consistent point-in-time snapshot of the index."""
        target_index = self.map.get(index_name)
        if target_index is None:
            raise KeyError(f"index '{index_name}' does not exist")

        # .items() in ConcurrentDictionary provides a thread-safe snapshot
        yield from target_index.items()

    def delete_from_index(self, index_name: str, key: Any) -> None:
        """Atomically removes a key from an index."""
        target_index = self.map.get(index_name)
        if target_index:
            target_index.remove_atomic(key)

    def delete_index(self, index_name: str) -> None:
        """Atomically removes an entire index."""
        self.map.remove_atomic(index_name)

    def list_indexes(self) -> list[str]:
        """Returns a thread-safe list of all index names."""
        return list(self.map.keys())