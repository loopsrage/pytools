import inspect
import json
from dataclasses import fields as dataclass_fields, is_dataclass
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

from concurrent_collections import ConcurrentDictionary


def _safe_signature(obj: Any) -> Optional[inspect.Signature]:
    """Resolves inspect.Signature safely across Python 3.11-3.14+ (including PEP 649 NameErrors)."""
    try:
        return inspect.signature(obj)
    except NameError:
        try:
            import annotationlib  # type: ignore[import-not-found]
            return inspect.signature(obj, annotation_format=annotationlib.Format.STRING)
        except Exception:
            return None
    except (ValueError, TypeError):
        return None


def reflect_entry_to_dict(key: Any, value: Any) -> Dict[str, Any]:
    """
    Reflects an arbitrary Index entry (callable function, class/dataclass, or scalar/collection)
    into a deterministic JSON-serializable schema dictionary.
    """
    key_str = str(key)
    if inspect.isclass(value):
        params_meta: List[Dict[str, Any]] = []
        sig = _safe_signature(value)
        if sig is not None:
            for p_name, p in sig.parameters.items():
                if p_name == "self":
                    continue
                params_meta.append(
                    {
                        "name": p_name,
                        "kind": str(p.kind.name),
                        "annotation": (
                            None
                            if p.annotation is inspect.Parameter.empty
                            else getattr(p.annotation, "__name__", str(p.annotation))
                        ),
                        "default": (
                            None
                            if p.default is inspect.Parameter.empty
                            else repr(p.default)
                        ),
                        "required": p.default is inspect.Parameter.empty,
                    }
                )

        dc_fields: List[Dict[str, Any]] = []
        if is_dataclass(value):
            for f in dataclass_fields(value):
                dc_fields.append(
                    {
                        "name": f.name,
                        "type": getattr(f.type, "__name__", str(f.type)),
                    }
                )

        public_methods = [
            m_name
            for m_name, m_obj in inspect.getmembers(value, predicate=inspect.isfunction)
            if not m_name.startswith("_")
        ]
        return {
            "key": key_str,
            "kind": "dataclass" if is_dataclass(value) else "class",
            "module": getattr(value, "__module__", None),
            "qualname": getattr(value, "__qualname__", getattr(value, "__name__", key_str)),
            "doc": (inspect.getdoc(value) or "").splitlines()[0] if inspect.getdoc(value) else "",
            "init_parameters": params_meta,
            "dataclass_fields": dc_fields,
            "methods": public_methods,
        }

    if callable(value):
        params_meta = []
        ret_ann = None
        sig = _safe_signature(value)
        if sig is not None:
            for p_name, p in sig.parameters.items():
                params_meta.append(
                    {
                        "name": p_name,
                        "kind": str(p.kind.name),
                        "annotation": (
                            None
                            if p.annotation is inspect.Parameter.empty
                            else getattr(p.annotation, "__name__", str(p.annotation))
                        ),
                        "default": (
                            None
                            if p.default is inspect.Parameter.empty
                            else repr(p.default)
                        ),
                        "required": p.default is inspect.Parameter.empty,
                    }
                )
            if sig.return_annotation is not inspect.Signature.empty:
                ret_ann = getattr(sig.return_annotation, "__name__", str(sig.return_annotation))

        return {
            "key": key_str,
            "kind": "callable",
            "module": getattr(value, "__module__", None),
            "qualname": getattr(value, "__qualname__", getattr(value, "__name__", key_str)),
            "doc": (inspect.getdoc(value) or "").splitlines()[0] if inspect.getdoc(value) else "",
            "parameters": params_meta,
            "return_annotation": ret_ann,
        }

    # Scalar, constant, or collection entry
    val_type = type(value).__name__
    if isinstance(value, (int, float, str, bool)) or value is None:
        ser_val: Any = value
    elif isinstance(value, (list, tuple, set)):
        seq = list(value)
        ser_val = [x if isinstance(x, (int, float, str, bool)) else repr(x) for x in seq[:64]]
    elif isinstance(value, dict):
        ser_val = {
            str(k): (v if isinstance(v, (int, float, str, bool)) else repr(v))
            for k, v in list(value.items())[:64]
        }
    else:
        ser_val = repr(value)

    return {
        "key": key_str,
        "kind": "constant",
        "type": val_type,
        "value": ser_val,
    }


class Index:
    """
    A thread-safe manager for multiple nested dictionaries using
    ConcurrentDictionary. Mimics Go's sync.Map for arbitrary objects.
    Supports reflecting registered tools, classes, and constants to JSON schemas.
    """

    def __init__(self):
        # Main map stores sub-index ConcurrentDictionary objects
        self.map = ConcurrentDictionary()

    def __getstate__(self):
        return {
            name: dict(self.range_index(name))
            for name in self.list_indexes()
        }

    def __setstate__(self, state):
        self.map = ConcurrentDictionary()
        for name, sub_dict in state.items():
            self.new(name)
            for k, v in sub_dict.items():
                self.store_in_index(name, k, v)

    def new(self, index_name: str) -> "Index":
        """Creates a new nested concurrent dictionary atomically."""
        self.map.put_if_absent(index_name, ConcurrentDictionary())
        return self

    def store_in_index(self, index_name: str, key: Any, value: Any):
        """Stores a key-value pair. Sub-index creation is handled atomically."""
        if index_name not in self.map:
            self.new(index_name)

        target_index = self.map[index_name]
        target_index.assign_atomic(key, value)

    def load_or_store_in_index(self, index_name: str, key: Any, value: Any) -> tuple[Any, bool]:
        """Equivalent to Go's LoadOrStore. Returns (value, loaded_flag)."""
        if index_name not in self.map:
            self.new(index_name)

        target_index = self.map[index_name]
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

    def reflect_partition_to_dict(self, index_name: str) -> Dict[str, Any]:
        """Reflects all tools, classes, and constants in a single partition to a dictionary."""
        entries: Dict[str, Any] = {}
        counts = {"callable": 0, "class": 0, "dataclass": 0, "constant": 0}
        for k, v in self.range_index(index_name):
            meta = reflect_entry_to_dict(k, v)
            entries[str(k)] = meta
            kind = meta.get("kind", "constant")
            counts[kind] = counts.get(kind, 0) + 1
        return {
            "partition": index_name,
            "entry_count": len(entries),
            "kind_counts": counts,
            "entries": entries,
        }

    def reflect_to_dict(self, index_name: Optional[str] = None) -> Dict[str, Any]:
        """Reflects the entire Index (or a single named partition) into a JSON-serializable dict."""
        if index_name is not None:
            return self.reflect_partition_to_dict(index_name)
        partitions: Dict[str, Any] = {}
        total_entries = 0
        for name in sorted(self.list_indexes()):
            part = self.reflect_partition_to_dict(name)
            partitions[name] = part
            total_entries += part["entry_count"]
        return {
            "partition_count": len(partitions),
            "total_entries": total_entries,
            "partitions": partitions,
        }

    def reflect_to_json(self, index_name: Optional[str] = None, indent: int = 2) -> str:
        """Serializes the reflected Index tool/constant manifest to a JSON string."""
        return json.dumps(self.reflect_to_dict(index_name=index_name), indent=indent, sort_keys=False)

    def save_json_reflection(
        self, output_path: str | Path, index_name: Optional[str] = None, indent: int = 2
    ) -> Path:
        """Writes the reflected Index JSON manifest to disk and returns the resolved Path."""
        out = Path(output_path).resolve()
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(self.reflect_to_json(index_name=index_name, indent=indent), encoding="utf-8")
        return out
