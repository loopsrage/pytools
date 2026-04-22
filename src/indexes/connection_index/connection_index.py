from src.thread_safe.index import Index


class ConnectionIndex:
    _index = None
    _namespace = None

    def __init__(self):
        self._index = Index()
        self._namespace = "connections"
        self._index.new(self._namespace)

    def connection(self, name, connection=None):
        return self._index.load_or_store_in_index(
            index_name=self._namespace,
            key=name,
            value=connection
        )

    def register_connections(self, connections):
        for name, connection in connections.items():
            self.connection(name, connection)

