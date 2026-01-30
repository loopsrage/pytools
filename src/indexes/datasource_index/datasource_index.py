from abc import abstractmethod, ABC

from src.fsspecc.base_fsspecfs.base_fsspecfs import FSpecFS
from src.thread_safe.index import Index


class Datasource(ABC):
    @abstractmethod
    def query_datasource(self):
        pass

class FsDatasource(FSpecFS, Datasource):
    def __init__(self, filesystem: str):
        super().__init__(filesystem=filesystem)

    def query_datasource(self):
        self.index("/Users/jarek/IdeaProjects/non_threaded/settings")
        return self.list_files()

class DatasourceIndex:
    _index = None
    _namespace = None

    def __init__(self):
        self._index = Index()
        self._namespace = "datasource"
        self._index.new(self._namespace)

    def register(self, name, datasource: Datasource):
        return self._index.load_or_store_in_index(
            index_name=self._namespace,
            key=name,
            value=datasource
        )

    def register_datasource(self, datasource: dict[str, Datasource]):
        for name, datasource in datasource.items():
            self.register(name, datasource)

    def datasource(self, name: str, datasource: Datasource = None):
        return self.register(name, datasource)

    def list_datasource(self):
        for key, value in self._index.range_index(self._namespace):
            yield key, value