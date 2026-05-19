import asyncio

from thread_safe.index import Index


class WorkerServiceIndex:
    _index = None
    _namespace = None

    def __init__(self):
        self._index = Index()
        self._namespace = "workers"
        self._index.new(self._namespace)

    def worker(self, name, workers=None):
        return self._index.load_or_store_in_index(
            index_name=self._namespace,
            key=name,
            value=workers
        )

    def register_workers(self, workers):
        for name, worker in workers.items():
            self.worker(name, worker)

    @property
    def range_workers(self):
        for name, worker in self._index.range_index(self._namespace):
            yield name, worker

    async def close_workers(self):
        futures = []
        for _, worker in self.range_workers:
            futures.append(worker.close())
        await asyncio.gather(*futures)

    def start_workers(self, stop_event, tg_proxy):
        for _, worker in self.range_workers:
            worker.init(stop_event, tg_proxy)