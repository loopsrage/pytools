from fsspecc.base_fsspecfs.base_fsspecfs import FSBase
from thread_safe.index import Index


class FilesystemIndex:
    _index = None
    _fs_namespace = "filesystems"

    def __init__(self):
        super().__init__()
        if self._index is None:
            self._index = Index()
        self._namespace = self._fs_namespace
        self._index.new(self._fs_namespace)

    def filesystem(self, name, filesystem = None):
        return self._index.load_or_store_in_index(
            index_name=self._fs_namespace,
            key=name,
            value=filesystem
        )

    def register_filesystems(self, filesystems: dict[str, FSBase]):
        for name, filesystem in filesystems.items():
            self.filesystem(name, filesystem)

