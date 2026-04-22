import asyncio
import traceback
from typing import Callable, Union, Any

from queue_controller.queueController import QueueController
from queue_controller.queueData import QueueData
from thread_safe.controller.controller import AsyncController


def handle_error(e: Exception) -> bool:
    traceback.print_exception(e)
    return False

class PeriodicProducer:
    _controller: AsyncController
    _queue: QueueController
    _action: Callable[[], Union[list[Any]|None]]
    _handle_error = None
    def __init__(self,action: Callable[[], Any],  queue: QueueController = None, interval: int = 1, start_now: bool = False, handle_error=None):
        if handle_error is None:
            self._handle_error = handle_error
        self._controller = AsyncController(interval, start_now)
        self._queue = queue
        self._action = action
        self._task = asyncio.create_task(self.run_loop())

    def close(self):
        self._controller.close()
        self._task.cancel()

    async def run_loop(self):
        try:
            while self._controller.running:
                await self._controller.wait()

                if not self._controller.running:
                    break

                if asyncio.iscoroutinefunction(self._action):
                    result = await self._action()
                else:
                    result = await asyncio.to_thread(self._action)

                if result is None:
                    continue

                if isinstance(result, Exception):
                    raise result

                tasks = []
                if result is not None:
                    for r in result:
                        tasks.append(self._queue.enqueue(QueueData(result=r)))
                await asyncio.gather(*tasks)
                if len(tasks) > 0:
                    self._controller.trigger()
        except Exception as e:
            traceback.print_exception(e)
            if not self._handle_error(e):
                raise e
        finally:
            self._controller.close()
            self._task.cancel()

def get_producer_result(queue_data: QueueData) -> Any:
    return queue_data.attribute("result")