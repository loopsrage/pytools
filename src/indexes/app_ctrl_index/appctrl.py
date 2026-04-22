from thread_safe.index import Index


class ApplicationIndex:
    _index = None
    _namespace = None

    def __init__(self):
        self._index = Index()
        self._namespace = "application_index"
        self._index.new(self._namespace)

    def application(self, name, application=None):
        return self._index.load_or_store_in_index(
            index_name=self._namespace,
            key=name,
            value=application
        )

    def register_applications(self, connections):
        for name, connection in connections.items():
            self.application(name, connection)
