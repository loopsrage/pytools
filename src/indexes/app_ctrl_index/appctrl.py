from thread_safe.index import Index


class ApplicationIndex:
    _index = None
    _app_namespace = "application_index"

    def __init__(self):
        super().__init__()
        if self._index is None:
            self._index = Index()
        self._namespace = self._app_namespace
        self._index.new(self._app_namespace)

    def application(self, name, application=None):
        return self._index.load_or_store_in_index(
            index_name=self._app_namespace,
            key=name,
            value=application
        )

    def register_applications(self, applications):
        for name, application in applications.items():
            self.application(name, application)
