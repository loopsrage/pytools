import io
from pathlib import Path

from fsspecc.base_fsspecfs.base_fsspecfs import FSBase


class DocumentStore:
    def __init__(self, base_dir: str | Path, filesystem: str = "file"):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._fs = FSBase(filesystem=filesystem)

    def _path(self, key: str) -> str:
        return str(self.base_dir / key)

    def save_bytes(self, key: str, data: bytes) -> None:
        parent = Path(self._path(key)).parent
        self._fs.make_dirs(str(parent), exist_ok=True)
        self._fs.write(self._path(key), io.BytesIO(data), use_pipe=False)

    def read_bytes(self, key: str) -> bytes:
        buf = io.BytesIO()
        self._fs.read(self._path(key), buf)
        return buf.getvalue()

    def save_text(self, key: str, text: str) -> None:
        self.save_bytes(key, text.encode("utf-8"))

    def read_text(self, key: str) -> str:
        return self.read_bytes(key).decode("utf-8")

    def exists(self, key: str) -> bool:
        return self._fs.client.exists(self._path(key))

    def list_docs(self, prefix: str = "") -> list[str]:
        pattern = f"{prefix}/*" if prefix else "*"
        return [p for p in self._fs.list(pattern)]
