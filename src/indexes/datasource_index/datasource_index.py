from abc import abstractmethod, ABC

from fsspecc.base_fsspecfs.base_fsspecfs import FSBase
from thread_safe.index import Index


class Datasource(ABC):
    @abstractmethod
    def query_datasource(self):
        pass

class FsDatasource(FSBase, Datasource):
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

    def datasource(self, name, datasource: Datasource):
        return self._index.load_or_store_in_index(
            index_name=self._namespace,
            key=name,
            value=datasource
        )

    def register_datasources(self, datasource: dict[str, Datasource]):
        for name, datasource in datasource.items():
            self.datasource(name, datasource)


    def list_datasource(self):
        for key, value in self._index.range_index(self._namespace):
            yield key, value