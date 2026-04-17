import collections
import threading

from src.thread_safe.index import Index
from src.thread_safe.tslist import TsList

# Constants used in the Go code
VALUES_STRING = "Values"
CONTAINERS_STRING = "Containers"

def flatten(data: dict):
    fmt = []
    cont = build_container_tree(start=data, path_delim=".")
    for n, c in cont.range_containers:
        for k, v in enumerate(c.value):
            if isinstance(v, (dict, tuple, list)):
                continue

            if isinstance(c.value, dict):
                val = c.value[v]
                if isinstance(val, tuple):
                    continue
                fmt.append(c.value[v])
    return fmt

class Container:
    """
    Python equivalent of the Go Container struct.
    Uses threading.Lock to manage concurrent access to atomic values.
    """
    def __init__(self, parent, delim, path: str, value):
        # The Go code uses atomic.Value for everything. In Python, we use
        # properties and locks to protect mutable state where necessary.

        self._parent = parent
        self._path = path
        self._value = value
        self._children = TsList()
        self._path_delim = delim
        self._container_index = None  # Will be set by NewContainer logic
        self.root = None  # Will be set by NewContainer logic

        # Lock for managing concurrent reads/writes to this object's own simple properties
        self._lock = threading.Lock()

    def __repr__(self) -> str:
        # Implements the GoStringer interface equivalent
        return self.path

    def print_container_values(self):
        for key, value in self.range_values:
            print(f"{key}, {value}")

    _container_key = "Containers"
    _value_key = "Values"

    @property
    def container_key(self):
        return self._container_key

    @property
    def value_key(self):
        return self._value_key

    @property
    def parent(self):
        """Returns the parent container."""
        with self._lock:
            return self._parent

    @property
    def flatten(self):
        out = {}
        for n, c in self.range_containers:
            for k, v in enumerate(c.value):
                if isinstance(v, (dict, tuple, list)):
                    continue

                if isinstance(c.value, dict):
                    val = c.value[v]
                    if isinstance(val, tuple):
                        continue
                    out[f"{n}.{v}"] = val
        return out

    def children(self) -> list:
        """Returns a list of all child containers."""
        # The underlying TsList is already thread-safe
        child_containers = []
        for v in self._children.all():
            if isinstance(v, Container):
                child_containers.append(v)
        return child_containers

    @property
    def value(self):
        """Returns the raw value stored in this container."""
        with self._lock:
            return self._value

    @property
    def path(self) -> str:
        """Returns the path string for this container."""
        with self._lock:
            return self._path

    @property
    def path_delim(self) -> str:
        """Returns the path delimiter string for this container."""
        with self._lock:
            return self._path_delim

    def new_object(self, parent_container, path: str, value):
        """Creates a new child container and appends it to the current container."""
        # This function acts as a factory method integrated with the parent's structure
        new_container = new_container_func(parent_container, path, value)
        self.append_children(new_container)
        return new_container

    def append_children(self, *children: 'Container'):
        """Appends container objects to the internal thread-safe children list."""
        # TsList handles the synchronization
        self._children.add(*children)

    # --- Index/Data Management Methods ---

    def read_from_value(self, path: str):
        """Reads a value from the global index by path."""
        # Go's signature returns (interface{}, error). Python raises KeyError for missing data.
        if self._container_index is None:
            raise ValueError("Container index not initialized.")

        return  self._container_index.load_from_index(self._container_key, path)

    def read_from_containers(self, path: str):
        """Reads a Container object from the global index by path."""
        if self._container_index is None:
            raise ValueError("Container index not initialized.")

        x = self._container_index.load_from_index(self._container_key, path)
        if isinstance(x, Container):
            return x
        else:
            raise TypeError("Invalid container type found in index.")

    @property
    def range_values(self):
        """Iterates over all indexed values, applying a function."""
        if self._container_index is None:
            raise ValueError("Container index not initialized.")

        for key, value in self._container_index.range_index(self._value_key):
            yield key, value

    @property
    def range_containers(self):
        """Iterates over all indexed containers, applying a function."""
        if self._container_index is None:
            raise ValueError("Container index not initialized.")

        for key, value in self._container_index.range_index(self._container_key):
            yield key, value

    @property
    def container_index(self):
        return self._container_index

    def read_primitive_value(self, path: str):
        """Reads a primitive value from the global index by path."""
        new_path = path[:path.rfind(self.path_delim)]
        key = path[path.rfind(self.path_delim):][1:]

        value = self.read_from_value(new_path).value
        if isinstance(value, collections.abc.MutableMapping):
            return value.get(key) or None

        return None

# The NewContainer function as a standalone factory function
def new_container_func(parent: Container, delim, path: str, value) -> Container:
    """
    Factory function to create a new Container instance.
    Handles root initialization and index management logic.
    """

    # Go logic: If parent is nil (None), set path to "root"
    if parent is None:
        path = "root"

    if path.startswith(delim):
        path = path[1:]
    # Initialize the new container instance
    cn = Container(parent=parent, path=path, value=value, delim=delim)

    # Go Logic for setting up the index chain and the root reference
    if parent is not None:
        # Use the parent's existing index and root
        cn._container_index = parent.container_index
        cn.root = parent.root
    else:
        # Create a brand new Index for the root container
        existing_index = Index()  # Assumes Index class is available
        existing_index.new(VALUES_STRING)
        existing_index.new(CONTAINERS_STRING)
        cn._container_index = existing_index
        cn.root = cn  # Root points to itself

    # Store the new value and container reference in the global index
    cn.container_index.store_in_index(VALUES_STRING, path, value)
    cn.container_index.store_in_index(CONTAINERS_STRING, path, cn)

    return cn

def build_container_tree(current_container=None, path=None, path_delim=None, start=None):
    """
    Recursively builds a tree of Container objects from a nested data structure.
    """

    if path_delim is None or path_delim is "":
        path_delim = ""

    if path is None:
        path = []

    if current_container is None:
        if start is None:
            start = [{}]
        # The Go code uses NewContainer when current is nil
        current_container = new_container_func(None, path_delim, "", start)
        path = [""]

    # Use get_value() method to access the underlying data
    value = current_container.value

    if isinstance(value, collections.abc.MutableMapping):  # Handles dicts
        for name, sub_value in value.items():
            if isinstance(sub_value, (collections.abc.MutableMapping, collections.abc.Sequence)) and not isinstance(sub_value, str):
                # We need to skip string types in Python as they are sequences, but treated differently here
                path.append(str(name))
                join_path = path_delim.join(path)

                # In Python, we can directly manage object creation and relationships:
                # The Go code calls `current.NewObject(...)` which is a factory/relationship manager.
                # We recursively call the function with the newly created child container.
                child_container = new_container_func(current_container, path_delim, join_path, sub_value)
                # If you need to store children within the parent:
                # current_container.children.append(child_container)

                build_container_tree(child_container, path, path_delim)
                path.pop()  # Equivalent to path[:len(path)-1]

    elif isinstance(value, collections.abc.Sequence) and not isinstance(value, str):  # Handles lists, tuples, but skip strings
        for i, sub_value in enumerate(value):
            if isinstance(sub_value, (collections.abc.MutableMapping, collections.abc.Sequence)) and not isinstance(sub_value, str):
                path.append(str(i))
                join_path = path_delim.join(path)

                child_container = new_container_func(current_container, path_delim, join_path, sub_value)
                # current_container.children.append(child_container)

                build_container_tree(child_container, path, path_delim)
                path.pop()  # Equivalent to path[:len(path)-1]

    return current_container