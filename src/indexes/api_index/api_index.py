from src.httpclient.api import Api
from src.thread_safe.index import Index


class ApiIndex:
    _index = None
    _namespace = None

    def __init__(self):
        self._index = Index()
        self._namespace = "http_apis"
        self._index.new(self._namespace)

    def register(self, name, api: Api):
        return self._index.load_or_store_in_index(
            index_name=self._namespace,
            key=name,
            value=api
        )

    def register_api(self, api: dict[str, Api]):
        for name, http_api in api.items():
            self.register(name, http_api)

    def api(self, name: str, api: Api = None):
        return self.register(name, api)

    def list_api(self):
        for key, value in self._index.range_index(self._namespace):
            yield key, value