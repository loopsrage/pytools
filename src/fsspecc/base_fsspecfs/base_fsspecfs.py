import io
import uuid
from pathlib import Path
from typing import Any, List

import fsspec

from src.thread_safe.index import Index


def get_file_path(request_id: str, file_name: str, sub_dir: str | List[str] = None):
    core = f"/{request_id}"
    if sub_dir is None:
        return f"{core}/{file_name}"

    if isinstance(sub_dir, list) and all(isinstance(s, str) for s in sub_dir):
        sub_dir_path = "/".join(sub_dir)
        return f"{core}/{sub_dir_path}/{file_name}"

    return f"{core}/{sub_dir}/{file_name}"


class FSpecFS:
    _fs: Any = None
    _filesystem = None
    _index = None
    _key = "directories"

    def __init__(self, filesystem: str = None):
        self._filesystem = filesystem or "memory"
        if self._filesystem == "mongodb":
            return

        self._fs = fsspec.filesystem(self._filesystem)
        self._index = Index()
        self._index.new(self._key)


    @property
    def client(self):
        return self._fs

    @property
    def filesystem(self):
        return self._filesystem

    def write(self, file_path: str, file_buffer: io.BytesIO, use_pipe=None):
        file_buffer.seek(0)
        errors = []
        temp_path = f"{file_path}.{uuid.uuid4()}.tmp"

        if use_pipe is None:
            use_pipe = False

        try:
            if use_pipe:
                try:
                    self.client.pipe_file(temp_path, file_buffer.getvalue())
                except Exception as pipe_err:
                    errors.append(pipe_err)
            else:
                with self.client.open(temp_path, "wb") as fs:
                    fs.write(file_buffer.getbuffer())

            self.client.rename(temp_path, file_path)
        except Exception as write_err:
            if self.client.exists(temp_path):
                self.client.rm(temp_path)
            raise ExceptionGroup("Atomic write failed", [*errors, write_err])
        finally:
            file_buffer.truncate(0)
            file_buffer.seek(0)

    def read(self, file_path: str, file_buffer: io.BytesIO, use_pipe=None):
        file_buffer.seek(0)
        file_buffer.truncate(0)

        if use_pipe is None:
            use_pipe = False

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


