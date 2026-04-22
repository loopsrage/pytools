import io

import pandas as pd

from fsspecc.atomic_writefs.atomic_writefs import AtomicWriteFs
from fsspecc.base_fsspecfs.base_fsspecfs import get_file_path


class CleanFs(AtomicWriteFs):

    def __init__(self, filesystem: str = None):
        super().__init__(filesystem=filesystem)

    @property
    def clean_filename(self):
        return "clean.csv"

    @property
    def raw_filename(self):
        return "raw.csv"

    def _write_df(self, file_path: str, df: pd.DataFrame, use_pipe=None):
        file_buffer = io.BytesIO()
        df.to_csv(file_buffer, index=False)
        self.write(file_path, file_buffer, use_pipe)

    def _read_df(self, file_path: str, use_pipe=None):
        buffer = io.BytesIO()
        self.read(file_path, buffer, use_pipe=use_pipe)
        return pd.read_csv(buffer)

    def get_clean_file(self, request_id: str, use_pipe=None):
        file_path = f"{get_file_path(request_id, self.clean_filename)}"
        return self._read_df(file_path, use_pipe)

    def get_raw_file(self, request_id: str, use_pipe=None):
        file_path = f"{get_file_path(request_id, self.raw_filename)}"
        return self._read_df(file_path, use_pipe)

    def save_clean_file(self, request_id, data, use_pipe=None):
        file_path = f"{get_file_path(request_id, self.clean_filename)}"
        self._write_df(file_path, data, use_pipe)

    def save_raw_file(self, request_id, data, use_pipe=None):
        file_path = f"{get_file_path(request_id, self.raw_filename)}"
        self._write_df(file_path, data, use_pipe)

    def list_raw_files(self, request_id: str):
        file_path = f"{get_file_path(request_id, "raw*")}"
        for i in self.client.glob(file_path):
            yield i

    def list_clean_files(self, request_id: str):
        file_path = f"{get_file_path(request_id, "clean*")}"
        for i in self.client.glob(file_path):
            yield i