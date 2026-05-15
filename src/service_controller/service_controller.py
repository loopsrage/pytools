from asyncio import TaskGroup

from periodic_producer.periodic_producer import PeriodicProducer
from queue_controller.helpers import start_pipeline, stop_pipeline
from queue_controller.queueController import QueueController


class ServiceController:
    controllers: list[PeriodicProducer] = []
    queues: list[QueueController] = []

    stop_event = None
    tg = None

    def init(self, stop_event, tg: TaskGroup):
        self.stop_event = stop_event
        start_pipeline(tg, self.queues)
        tg.create_task(self.waiter())

    async def close(self):
        for c in self.controllers:
            c.close()

        await stop_pipeline(self.queues)

    async def waiter(self):
        try:
            await self.stop_event.wait()
            await self.close()
        except Exception:
            raise