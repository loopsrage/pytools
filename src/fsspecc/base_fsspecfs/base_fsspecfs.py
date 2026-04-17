import io
import shutil
from pathlib import Path
from typing import List, Optional

import fsspec
from pydantic import errors

from src.thread_safe.index import Index


def get_file_path(request_id: str, file_name: str, sub_dir: str | List[str] = None):
    core = f"/{request_id}"
    if sub_dir is None:
        return f"{core}/{file_name}"

    if isinstance(sub_dir, list) and all(isinstance(s, str) for s in sub_dir):
        sub_dir_path = "/".join(sub_dir)
        return f"{core}/{sub_dir_path}/{file_name}"

    return f"{core}/{sub_dir}/{file_name}"

class FSBase:
    _key = "directories"
    def __init__(self, filesystem: str = "memory", storage_options: Optional[dict] = None, path=None):
        """
        :param filesystem: The fsspec protocol (e.g., 'abfs' for Azure, 's3', 'gcs', 'file')
        :param storage_options: Credentials (e.g., {'account_name': '...', 'account_key': '...'})
        """
        self._filesystem = filesystem
        self._storage_options = storage_options or {}
        self._fs = fsspec.filesystem(self._filesystem, **self._storage_options)
        self._index = Index()
        self._index.new(self._key)
        if path is not None:
            self.index(path)

    @property
    def client(self):
        return self._fs

    @property
    def filesystem(self):
        return self._filesystem

    def write(self, file_path: str, file_buffer: io.BytesIO, use_pipe=False):
        file_buffer.seek(0)

        if use_pipe is None:
            use_pipe = False

        try:
            if use_pipe:
                try:
                    self.client.pipe_file(file_path, file_buffer.getvalue())
                except Exception as pipe_err:
                    errors.append(pipe_err)
            else:
                with self.client.open(file_path, "wb") as fs:
                    fs.write(file_buffer.getbuffer())
        finally:
            file_buffer.truncate(0)
            file_buffer.seek(0)

    def append(self, file_path: str, file_buffer: io.BytesIO):
        """
        Appends content to the end of an existing file.
        Note: Not all object stores (e.g., standard S3) support native append.
        """
        file_buffer.seek(0)

        try:
            # "ab" = Append Binary mode
            with self.client.open(file_path, "ab") as fs:
                fs.write(file_buffer.getbuffer())
        finally:
            # Maintain consistency with your write() method's cleanup logic
            file_buffer.truncate(0)
            file_buffer.seek(0)

    def make_dirs(self, path: str, exist_ok: bool = True):
        try:
            self.client.makedirs(path, exist_ok=exist_ok)
        except Exception as dir_err:
            # Some fsspec implementations might behave differently with permissions
            if not exist_ok:
                raise dir_err

    def read(self, file_path: str, file_buffer: io.BytesIO, use_pipe=False):
        """Reads the file content into the provided buffer."""
        file_buffer.seek(0)
        file_buffer.truncate(0)

        errors = []

        if use_pipe:
            try:
                data = self.client.cat_file(file_path)
                file_buffer.write(data)
                file_buffer.seek(0)
                return
            except Exception as pipe_err:
                errors.append(pipe_err)
        try:
            with self.client.open(file_path, "rb") as fs:
                file_buffer.write(fs.read())
                file_buffer.seek(0)
        except Exception as read_err:
            raise ExceptionGroup("errors", [*errors, read_err])

    def list(self, glob_pattern):
        for i in self.client.glob(glob_pattern):
            yield i

    def walk(self, path):
        if self._filesystem == "mongodb":
            return

        for root, dirs, files in self.client.walk(path):
            if len(files) > 0:
                yield files

    def index(self, path):
        for root, files in self.walk_files(path):
            self._index.store_in_index(self._key, root, files)

    def list_files(self):
        for root, files in self._index.range_index(self._key):
            for f in files:
                yield f"{root}/{f}"

    def walk_files(self, path):
        for root, dirs, files in self.client.walk(path):
            if len(files) > 0:
                yield root, files

    def walk_dirs(self, path):
        for root, dirs, files in self.client.walk(path):
            if len(dirs) > 0:
                yield dirs

    def transfer_to_drive(self, pattern=None, target_directory=None):
        if target_directory is None:
            raise AttributeError("target_directory cannot be None")

        dir_path = Path(target_directory)
        dir_path.mkdir(parents=True, exist_ok=True)

        for i in self.list(pattern):
            file_path = dir_path / i.filename
            with file_path.open(mode='wb') as f:
                f.write(i.read())

    def close(self):
        # Only needed if using protocols like SFTP/FTP/SSH/Mongodb
        if hasattr(self._fs, "close"):
            self._fs.close()

    def load_or_store(self, file_path: str, get_bytes):
        """
        Attempts to load a binary index. If it doesn't exist, stores default_data.
        Returns: (bytes, was_loaded_from_storage)
        """
        # Create a temporary buffer for the potential read
        temp_buffer = io.BytesIO()
        try:
            if self.client.exists(file_path):
                self.read(file_path, temp_buffer)
                return temp_buffer.getvalue(), True
        except (FileNotFoundError, Exception):
            # If read fails or file vanished, proceed to attempt a store
            pass

        data = get_bytes()
        temp_buffer.seek(0)
        temp_buffer.truncate(0)
        temp_buffer.write(data)

        try:
            self.write(file_path, temp_buffer)
            return data, False

        except ExceptionGroup:
            temp_buffer.seek(0)
            temp_buffer.truncate(0)

            self.read(file_path, temp_buffer)
            return temp_buffer.getvalue(), True

    def transfer(self, to: "FSBase", pattern, target_directory):
        if target_directory is None:
            raise AttributeError("target_directory cannot be None")

        dest_dir = Path(f"{target_directory}")
        dest_dir.mkdir(parents=True, exist_ok=True)

        for remote_path in self.list(pattern):
            clean_filename = Path(remote_path).name
            to_file_path = dest_dir / clean_filename
            with self.client.open(str(remote_path), 'rb') as remote_file:
                to.load_or_store(str(to_file_path), remote_file.read)

    def sync(self, to: "FSBase", local_path, remote_path):
        self.transfer(to, f"{local_path}/*", f"{remote_path}/")
        to.transfer(self.client, f"{remote_path}/*", local_path)

    def open(self, path: str, mode: str = "rb", **kwargs):
        return self.client.open(path, mode, **kwargs)

    def put(self, local_path: str, remote_path: str):
        """Uploads a local file to the remote filesystem, clearing block lists."""
        self.client.put(local_path, remote_path)