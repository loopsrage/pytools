from thread_safe.index import Index


class ConnectionIndex:
    _index = None
    _conn_namespace = "connections"

    def __init__(self):
        super().__init__()
        if self._index is None:
            self._index = Index()
        self._namespace = self._conn_namespace
        self._index.new(self._conn_namespace)

    def connection(self, name, connection=None):
        return self._index.load_or_store_in_index(
            index_name=self._conn_namespace,
            key=name,
            value=connection
        )

    def register_connections(self, connections):
        for name, connection in connections.items():
            self.connection(name, connection)

