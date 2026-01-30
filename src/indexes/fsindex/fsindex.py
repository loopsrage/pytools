from src.fsspecc.base_fsspecfs.base_fsspecfs import FSpecFS
from src.thread_safe.index import Index


class FilesystemIndex:
    _index = None
    _namespace = None

    def __init__(self):
        self._index = Index()
        self._namespace = "filesystems"
        self._index.new(self._namespace)

    def register(self, name, filesystem):
        return self._index.load_or_store_in_index(
            index_name=self._namespace,
            key=name,
            value=filesystem
        )

    def register_filesystems(self, filesystems: dict[str, FSpecFS]):
        for name, filesystem in filesystems.items():
            self.register(name, filesystem)

    def filesystem(self, name: str, filesystem = None):
        return self.register(name, filesystem)
