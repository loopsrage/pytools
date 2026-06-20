import asyncio
import threading
import traceback
import uuid
from tqdm import tqdm

from meta_models.working_service import WorkerActionService, WorkerServiceConfig
from queue_controller.helpers import new_controller
from queue_controller.queueController import QueueController
from queue_controller.queueData import QueueData
from thread_safe.index import Index


class ActionConfig:
    service: WorkerActionService
    config: WorkerServiceConfig
    queue: QueueController

    def __init__(self, queue: QueueController):
        self.queue = queue

class IndexQueue:
    index: Index = None
    _counter_lock = None

    def __init__(self, action: dict[str, ActionConfig]):
        self.index = Index()
        self.max_index_size = 256
        self._counter_lock = threading.Condition(lock=threading.Lock())
        self._in_flight_tasks = 0
        self._loop = asyncio.get_running_loop()
        self._all_tasks_done = asyncio.Event()

        self._in_flight_tasks: dict[str, int] = {}
        self._max_sizes: dict[str, int] = {}


        self._pbar = tqdm(desc="Processing Tasks", unit="task", total=0)

        for name, config in action.items():
            self.index.new(name)
            self._in_flight_tasks[name] = 0
            self._max_sizes[name] = self.max_index_size
            config.config = WorkerServiceConfig(
                identity=name,
                worker_count=2,
                worker_interval=2,
                start_now=True,
            )
            config.service = self.producer(name, config.config)
            self.store_stage_action(name, config)

    def producer(self, action_key: str, config):
        async def action():
            queued = []
            try:
                items = list(self.index.range_index(action_key))
                for k, x in items:
                    self.index.delete_from_index(action_key, k)
                    queued.append(self.load_stage_action(action_key).queue.enqueue(QueueData(action_key=action_key, key=k, obj=x, index_queue=self)))
            except Exception as e:
                traceback.print_exception(e)
                pass
            finally:
                if queued:
                    await asyncio.gather(*queued)

        return WorkerActionService(
            config=config,
            action=action,
        )

    def store_stage_action(self, name: str, config: ActionConfig):
        self.index.store_in_index("STAGE_ACTIONS", name, config)

    def load_stage_action(self, name: str) -> ActionConfig:
        return self.index.load_from_index("STAGE_ACTIONS", name)

    def enqueue(self, action_key: str, key=None, value=None):
        with self._counter_lock:

            if action_key not in self._in_flight_tasks:
                self._in_flight_tasks[action_key] = 0
                self._max_sizes[action_key] = 100

            while self._in_flight_tasks[action_key] >= self._max_sizes[action_key]:
                self._counter_lock.wait()

            self._in_flight_tasks[action_key] += 1
            total_current_tasks = sum(self._in_flight_tasks.values())
            self._pbar.total = max(self._pbar.total, total_current_tasks + self._pbar.n)
            self._pbar.refresh()

            if self._in_flight_tasks == 1:
                self._all_tasks_done.clear()

        if key is None:
            key = uuid.uuid4().hex

        self.index.store_in_index(action_key, key, value)

    def task_complete(self, action_key: str):
        with self._counter_lock:
            if self._in_flight_tasks.get(action_key, 0) > 0:
                self._in_flight_tasks[action_key] -= 1
                self._pbar.update(1)
                self._counter_lock.notify_all()

                if sum(self._in_flight_tasks.values()) == 0:
                    self._loop.call_soon_threadsafe(self._all_tasks_done.set)

    async def wait_for_completions(self):
        with self._counter_lock:
            if self._in_flight_tasks == 0:
                return
        await self._all_tasks_done.wait()

        with self._counter_lock:
            self._pbar.close()

def new_index_queue(worker_count: int, *actions):
    _closed_workers_tracker = 0
    _tracker_lock = threading.Lock()
    _actions = {}

    for x in actions:
        def a(target_action):
            def b(queue_data: QueueData):
                index = queue_data.attribute("index_queue")
                act_key = queue_data.attribute("action_key") or target_action.__name__
                try:
                    target_action(index, queue_data)
                except Exception as e:
                    traceback.print_exception(e)
                finally:
                    index.task_complete(act_key)
            return b
        _actions[x.__name__] = ActionConfig(queue=new_controller(action=a(x), worker_count=worker_count))

    return _actions
