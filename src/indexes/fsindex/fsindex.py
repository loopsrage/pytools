from src.fsspecc.base_fsspecfs.base_fsspecfs import FSBase
from src.thread_safe.index import Index


class FilesystemIndex:
    _index = None
    _namespace = None

    def __init__(self):
        self._index = Index()
        self._namespace = "filesystems"
        self._index.new(self._namespace)

    def filesystem(self, name, filesystem = None):
        return self._index.load_or_store_in_index(
            index_name=self._namespace,
            key=name,
            value=filesystem
        )

    def register_filesystems(self, filesystems: dict[str, FSBase]):
        for name, filesystem in filesystems.items():
            self.filesystem(name, filesystem)

