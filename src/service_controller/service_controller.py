from asyncio import TaskGroup

from queue_controller.helpers import start_pipeline, stop_pipeline


class ServiceController:
    controllers = []
    queues = []

    either = False
    stop_event = None
    tg = None

    async def init(self, tg: TaskGroup):
        if self.either:
            start_pipeline(tg, self.queues)

    async def close(self):
        if self.either:
            await stop_pipeline(self.queues)
