import asyncio
import traceback

from meta_models.working_service import WorkerActionService, WorkerServiceConfig
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

    def __init__(self, action: dict[str, ActionConfig]):
        self.index = Index()
        for name, config in action.items():
            self.index.new(name)
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
                    queued.append(self.load_stage_action(action_key).queue.enqueue(QueueData(key=action_key, obj=x, index_queue=self)))
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

    def enqueue(self, action_key: str, key, value):
        self.index.store_in_index(action_key, key, value)

