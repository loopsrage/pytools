from httpclient.api import Api
from thread_safe.index import Index

class ApiIndex:
    _index = None
    _api_namespace = "http_apis"

    def __init__(self):
        super().__init__()
        if self._index is None:
            self._index = Index()
        self._namespace = self._api_namespace
        self._index.new(self._api_namespace)

    def api(self, name, api: Api = None):
        return self._index.load_or_store_in_index(
            index_name=self._api_namespace,
            key=name,
            value=api
        )

    def register_apis(self, api: dict[str, Api]):
        for name, http_api in api.items():
            self.api(name, http_api)

    def list_api(self):
        for key, value in self._index.range_index(self._api_namespace):
            yield key, value