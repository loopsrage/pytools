import io
from typing import Any

from fastapi import Request

from src.fsspecc.base_fsspecfs.base_fsspecfs import FSpecFS, get_file_path


def get_storage(request: Request) -> "FSpecFS":
    return request.app.state.storage

class MemFS(FSpecFS):

    def __init__(self):
        super().__init__("memory")

    def store(self, request_id, key, value):
        bytes_data = value.encode('utf-8')
        buffer = io.BytesIO(bytes_data)
        self.write(get_file_path(request_id, key), buffer, True)

    def load(self, request_id,  key: str, value: io.BytesIO) -> Any:
        self.read(get_file_path(request_id, key), value, True)