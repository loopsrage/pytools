import io
import json
from pathlib import Path

import pandas as pd

from fsspecc.base_fsspecfs.base_fsspecfs import FSBase


class LocalStorage:
    def __init__(self, base_dir: str | Path):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._fs = FSBase(filesystem="file")

    def _path(self, key: str) -> str:
        return str(self.base_dir / key)

    def _ensure_parent(self, key: str) -> None:
        parent = Path(self._path(key)).parent
        self._fs.make_dirs(str(parent), exist_ok=True)

    def save_bytes(self, key: str, data: bytes) -> None:
        self._ensure_parent(key)
        self._fs.write(self._path(key), io.BytesIO(data), use_pipe=False)

    def read_bytes(self, key: str) -> bytes:
        buf = io.BytesIO()
        self._fs.read(self._path(key), buf)
        return buf.getvalue()

    def save_df(self, key: str, df: pd.DataFrame) -> None:
        self.save_bytes(key, df.to_csv(index=False).encode("utf-8"))

    def load_df(self, key: str) -> pd.DataFrame:
        return pd.read_csv(io.BytesIO(self.read_bytes(key)))

    def save_json(self, key: str, obj: object) -> None:
        self.save_bytes(key, json.dumps(obj, default=str).encode("utf-8"))

    def load_json(self, key: str):
        return json.loads(self.read_bytes(key).decode("utf-8"))

    def exists(self, key: str) -> bool:
        return self._fs.client.exists(self._path(key))
