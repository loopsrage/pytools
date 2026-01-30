import asyncio
from typing import Callable, Union, Any

from src.queue_controller.queueController import QueueController
from src.queue_controller.queueData import QueueData
from src.thread_safe.controller.controller import Controller


class PeriodicProducer:
    _controller: Controller
    _queue: QueueController
    _action: Callable[[], Union[list[Any]|None]]

    def __init__(self,action: Callable[[], Any],  queue: QueueController, interval: int, start_now: bool):
        self._controller = Controller(interval, start_now)
        self._queue = queue
        self._action = action
        self._task = asyncio.create_task(self.run_loop())

    async def run_loop(self):
        try:
            while self._controller.wait():
                await asyncio.sleep(0.01)
                if asyncio.iscoroutinefunction(self._action):
                    result = await self._action()
                else:
                    result = await asyncio.to_thread(self._action)

                if result is None:
                    continue

                if result is not None:
                    for r in result:
                        data = QueueData()["result"] = r
                        await self._queue.enqueue(data)

                self._controller.clear()
        finally:
            self._task.cancel()
            self._controller.close()

def get_producer_result(queue_data: QueueData) -> Any:
    return queue_data.attribute("result")