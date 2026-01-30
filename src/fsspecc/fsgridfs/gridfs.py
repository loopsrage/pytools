import hashlib
import io
import re
from functools import partial
from pathlib import Path

import gridfs
from gridfs import errors

from src.fsspecc.base_fsspecfs.base_fsspecfs import FSpecFS
from pymongo import MongoClient


def _aggregate_last_version(db, fs, filename=None, collection=None):
    if collection is None:
        collection = "fs"

    if filename:
        match_filter = {"filename": {"$regex": f"{re.escape(filename)}"}}
    else:
        match_filter = {"filename": {"$exists": 1}}

    pipeline = [
        {'$match': match_filter}, {'$sort': {'filename': 1, 'uploadDate': -1}},
        {'$group': {'_id': '$filename', 'latest_id': {'$first': '$_id'}}}
    ]
    latest_files_cursor = db[f"{collection}.files"].aggregate(pipeline)
    for entry in latest_files_cursor:
        yield fs.get(entry['latest_id'])

class MongoFS(FSpecFS):
    _client = None
    _name = None
    def __init__(self, dsn: str, database_name: str):
        super().__init__(filesystem="mongodb")
        self._client = MongoClient(dsn)
        self._fs = gridfs.GridFS(self._client[database_name])
        self._name = database_name

    @property
    def name(self):
        return self._name

    @property
    def client(self):
        return self._client

    @property
    def files_db(self):
        return self.client[self.name]

    def files_fs(self, collection: str = None):
        return partial(gridfs.GridFS, self.files_db, collection=collection)

    def write(self, file_path: str, file_buffer: io.BytesIO, **kwargs):
        file_buffer.seek(0)
        return self.files_fs()().put(file_buffer, **kwargs)

    def list(self, collection, pattern=None, fs=None):
        for i in _aggregate_last_version(self.files_db, fs, pattern, collection):
            yield i

    def read(self, file_path: str, x = None, y = None):
        return io.BytesIO(self._fs.get_last_version(filename=file_path)).getvalue()

    def save_image(self, file_name, model_name, figure):
        img_buffer = io.BytesIO()
        figure.savefig(img_buffer, format='png')
        img_buffer.seek(0)
        return self.files_fs()().put(
            img_buffer,
            model_name=model_name,
            filename=file_name,
            content_type="image/png")

    def load_or_store(self, file_path: str, get_bytes):
        file_id = hashlib.sha256(file_path.encode()).hexdigest()

        try:
            if self._fs.exists(file_id):
                grid_out = self._fs.get(file_id)
                return grid_out.read(), True
        except errors.NoFile:
            pass

        data = get_bytes()

        try:
            self._fs.put(data, _id=file_id, filename=file_path)
            return data, False
        except errors.FileExists:
            grid_out = self._fs.get(file_id)
            return grid_out.read(), True

    def transfer_to_drive(self, pattern=None, target_directory=None, collection=None):
        if target_directory is None:
            raise AttributeError("target_directory cannot be None")

        dir_path = Path(target_directory)
        dir_path.mkdir(parents=True, exist_ok=True)

        for i in self.list(collection=collection, pattern=pattern):
            file_path = dir_path / i.filename
            with file_path.open(mode='wb') as f:
                f.write(i.read())

    def close(self):
        self.client.close()
        if hasattr(self._fs, "close"):
            self._fs.close()