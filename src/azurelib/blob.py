import base64
import hashlib
import shutil
from pathlib import Path
from typing import Optional, Callable, Tuple

import fsspec
from pydantic_settings import BaseSettings

from fsspecc.base_fsspecfs.base_fsspecfs import FSBase


class AzureBlobConfig(BaseSettings):
    account_name: str
    account_key: str

def calculate_local_md5(data: bytes) -> str:
    """Calculates MD5 hash of local bytes."""
    return hashlib.md5(data).hexdigest()

def normalize_md5(md5_val: Optional[str]) -> Optional[str]:
    """Normalizes various MD5 formats (Base64, Hex, or Quoted ETag) to a clean Hex string."""
    if not md5_val:
        return None

    if isinstance(md5_val, (bytes, bytearray)):
        return md5_val.hex().lower()

    # Remove quotes (common in ETags)
    clean_val = md5_val.strip('"')
    if len(clean_val) == 32 and all(c in "0123456789abcdefABCDEF" for c in clean_val):
        return clean_val.lower()

    # If it's Base64 (like Azure's Content-MD5 header), convert to Hex
    try:
        decoded = base64.b64decode(clean_val)
        if len(decoded) == 16:
            return decoded.hex().lower()
    except Exception:
        pass

    return clean_val.lower()

class AzureBlob(FSBase):

    def __init__(self, storage_options: AzureBlobConfig):
        super().__init__("abfs", storage_options.model_dump())

    def get_remote_md5(self, file_path: str) -> Optional[str]:
        try:
            info = self._fs.info(file_path)
            return normalize_md5(info["content_settings"]["content_md5"])
        except:
            return None

    def write_if_different(self, file_path: str, data: bytes) -> bool:
        local_md5 = hashlib.md5(data).hexdigest().lower()
        remote_md5 = self.get_remote_md5(file_path)

        if local_md5 == remote_md5:
            return False

        self.client.pipe_file(file_path, data)
        return True

    def load_or_store(self, file_path: str, get_bytes: Callable[[], bytes]) -> Tuple[bytes, bool]:
        """
        1. If file exists, check MD5.
        2. If MD5 matches what get_bytes() produces, just return the data.
        3. If MD5 differs or file missing, write new data.
        Returns: (data, was_loaded_from_storage)
        """
        local_data = get_bytes()
        local_md5 = hashlib.md5(local_data).hexdigest().lower()

        remote_md5 = self.get_remote_md5(file_path)

        if remote_md5 is not None:
            if remote_md5 == local_md5:
                return local_data, True
            else:
                self.write_if_different(file_path, local_data)
                return local_data, False
        else:
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
        to.transfer(self.client, f"{remote_path}/*", local_path)

    def put(self, local_path: str, remote_path: str, overwrite: bool = False, **kwargs):
        self.client.put(local_path, remote_path, overwrite=overwrite, **kwargs)