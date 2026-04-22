import collections
import threading

from thread_safe.index import Index
from thread_safe.tslist import TsList

VALUES_STRING = "Values"
CONTAINERS_STRING = "Containers"

def clean_tag(tag):
    """Removes {namespace} from the tag string."""
    return tag.split('}')[-1] if '}' in tag else tag

class Container:
    def __init__(self, parent, delim, path: str, value):
        self._parent = parent
        self._path = path
        self._value = value
        self._children = TsList()
        self._path_delim = delim
        self._container_index = None
        self.root = None
        self._lock = threading.Lock()

    def __repr__(self) -> str:
        return f"Container(path={self.path})"

    @property
    def parent(self):
        with self._lock: return self._parent

    @property
    def value(self):
        """Returns the XML Element."""
        with self._lock: return self._value

    @property
    def path(self) -> str:
        with self._lock: return self._path

    @property
    def container_index(self):
        return self._container_index

    def read_from_value(self, path: str):
        if self._container_index is None:
            raise ValueError("Container index not initialized.")
        return self._container_index.load_from_index(VALUES_STRING, path)

    @property
    def range_values(self):
        """Iterates over all indexed values, applying a function."""
        if self._container_index is None:
            raise ValueError("Container index not initialized.")

        for key, value in self._container_index.range_index(VALUES_STRING):
            yield key, value

    @property
    def range_containers(self):
        """Iterates over all indexed containers, applying a function."""
        if self._container_index is None:
            raise ValueError("Container index not initialized.")

        for key, value in self._container_index.range_index(CONTAINERS_STRING):
            yield key, value

    @property
    def path_delim(self) -> str:
        """Returns the path delimiter string for this container."""
        with self._lock:
            return self._path_delim

    def read_primitive_value(self, path: str):
        """Reads a primitive value from the global index by path."""
        # 1. Load the XML element from the index using the provided path
        element = self.read_from_value(path)
        if element is None:
            return None

        if len(element) == 0:
            return element.text.strip() if element.text else None

        if "@" in path:
            base_path, attr_name = path.split("@")
            target_el = self.read_from_value(base_path)
            return target_el.attrib.get(attr_name) if target_el is not None else None
        return None


def new_container_func(parent: Container, delim, path: str, value) -> Container:
    if parent is None:
        path = "root"
    if path.startswith(delim):
        path = path[1:]

    cn = Container(parent=parent, path=path, value=value, delim=delim)

    if parent is not None:
        cn._container_index = parent.container_index
        cn.root = parent.root
    else:
        existing_index = Index()
        existing_index.new(VALUES_STRING)
        existing_index.new(CONTAINERS_STRING)
        cn._container_index = existing_index
        cn.root = cn

    # In XML mode, we store the element attributes or text as the "value"
    # Or store the whole element reference
    cn.container_index.store_in_index(VALUES_STRING, path, value)
    cn.container_index.store_in_index(CONTAINERS_STRING, path, cn)

    if parent:
        parent._children.add(cn)

    return cn

def build_xml_container_tree(current_container=None, path=None, path_delim=".", root_element=None):
    """
    Recursively builds a tree of Container objects from an XML ElementTree.
    """
    if path is None:
        path = ["root"]

    # Initial setup: Parse XML string if provided as root_element
    if current_container is None:
        if root_element is None:
            raise ValueError("Need a root XML Element to start.")

        # Initialize root container with the root XML element
        current_container = new_container_func(None, path_delim, "root", root_element)

    # The 'value' here is an xml.etree.ElementTree.Element
    element = current_container.value

    # Track tag occurrences to handle siblings with same names (e.g., <item>, <item>)
    tag_counts = collections.Counter()

    for child in element:
        tag_name = clean_tag(child.tag)
        tag_counts[child.tag] += 1

        # Create a unique path segment (e.g., root.users.user_1)
        # Using index suffix for duplicate tags to ensure path uniqueness in the index
        tag_path_name = f"{tag_name}_{tag_counts[tag_name]}"

        path.append(tag_path_name)
        join_path = path_delim.join(path)

        # Create child container
        child_container = new_container_func(current_container, path_delim, join_path, child)

        # Recurse into the XML child
        build_xml_container_tree(child_container, path, path_delim)

        path.pop()
    return current_container