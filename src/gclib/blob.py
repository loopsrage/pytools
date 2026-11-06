import base64
import hashlib
from pathlib import Path
from typing import Optional, Callable, Tuple, Any

try:
    from google.cloud import storage
except ImportError:
    storage = Any

import fsspec
from pydantic import ConfigDict
from pydantic_settings import BaseSettings

from fsspecc.base_fsspecfs.base_fsspecfs import FSBase


class GcsBlobConfig(BaseSettings):
    model_config = ConfigDict(extra="allow")
    bucket_name: str | None = None
    project: str | None = None
    token: Any | None = None


class GcsBlob(FSBase):
    def __init__(self, storage_options: GcsBlobConfig):
        options = storage_options.model_dump()
        bucket = options.pop("bucket_name", None)
        super().__init__("gcs", options)
        self.bucket_name = bucket

    def _full_key(self, file_path: str) -> str:
        if self.bucket_name and not file_path.startswith(f"{self.bucket_name}/"):
            return f"{self.bucket_name}/{file_path}"
        return file_path

    def get_remote_md5(self, file_path: str) -> Optional[str]:
        try:
            info = self.client.info(self._full_key(file_path))
            md5 = info.get("etag", "").strip('"')
            return md5.lower() if md5 else None
        except Exception:
            return None

    def write_if_different(self, file_path: str, data: bytes) -> bool:
        local_md5 = hashlib.md5(data).hexdigest().lower()
        remote_md5 = self.get_remote_md5(file_path)

        if local_md5 == remote_md5:
            return False

        self.client.pipe_file(self._full_key(file_path), data)
        return True

    def load_or_store(self, file_path: str, get_bytes: Callable[[], bytes]) -> Tuple[bytes, bool]:
        local_data = get_bytes()
        local_md5 = hashlib.md5(local_data).hexdigest().lower()

        remote_md5 = self.get_remote_md5(file_path)

        if remote_md5 is not None:
            if remote_md5 == local_md5:
                return local_data, True
            self.write_if_different(file_path, local_data)
            return local_data, False

        self.write_if_different(file_path, local_data)
        return local_data, False

    def transfer(self, to: "FSBase", pattern, target_directory):
        if target_directory is None:
            raise AttributeError("target_directory cannot be None")

        dest_dir = Path(f"{target_directory}")
        dest_dir.mkdir(parents=True, exist_ok=True)

        for remote_path in self.list(pattern):
            _, path_without_protocol = fsspec.core.split_protocol(remote_path)
            clean_filename = Path(path_without_protocol).name
            to_file_path = dest_dir / clean_filename
            with self.client.open(str(remote_path), 'rb') as remote_file:
                to.load_or_store(f"{str(to_file_path)}", remote_file.read)

    def sync(self, to: FSBase, local_path, remote_path):
        self.transfer(to, f"{local_path}/*", f"{remote_path}/")
        to.transfer(self, f"{remote_path}/*", local_path)

    def put(self, local_path: str, remote_path: str, overwrite: bool = False, **kwargs):
        self.client.put(local_path, remote_path, overwrite=overwrite, **kwargs)

    def gcs_client(self):
        if storage is Any:
            raise ImportError("google-cloud-storage is not installed")
        return storage.Client(project=self._storage_options.get("project"))
