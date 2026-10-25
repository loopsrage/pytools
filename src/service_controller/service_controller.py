from asyncio import TaskGroup
from logging import Logger

from periodic_producer.periodic_producer import PeriodicProducer
from queue_controller.helpers import start_pipeline, stop_pipeline
from queue_controller.queueController import QueueController


class ServiceController:
    controllers: list[PeriodicProducer]
    queues: list[QueueController]

    stop_event = None
    tg = None

    logger: Logger = None

    def __init__(self):
        self.controllers = []
        self.queues = []

    def init(self, stop_event, tg: TaskGroup, logger = None):
        if not hasattr(self, "controllers") or self.controllers is None:
            self.controllers = []
        if not hasattr(self, "queues") or self.queues is None:
            self.queues = []
        self.stop_event = stop_event
        self.logger = logger
        start_pipeline(tg, self.queues)
        tg.create_task(self.waiter())

    async def close(self):
        self.close_controllers()
        if getattr(self, "queues", None):
            await stop_pipeline(self.queues)

    async def waiter(self):
        try:
            await self.stop_event.wait()
            await self.close()
        except Exception:
            raise

    def close_controllers(self):
        if self.controllers is not None:
            for c in self.controllers:
                if hasattr(c, "close"):
                    c.close()
