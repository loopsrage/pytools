import asyncio
import inspect
import traceback
from typing import Callable, Union, Any

from queue_controller.queueController import QueueController
from queue_controller.queueData import QueueData
from thread_safe.controller.controller import AsyncController


def handle_error_f(e: Exception) -> bool:
    traceback.print_exception(e)
    return False

class PeriodicProducer:
    _controller: AsyncController
    _queue: QueueController
    _action: Callable[[], Union[list[Any]|None]]
    _handle_error = None
    def __init__(self,action: Callable[[], Any],  queue: QueueController = None, interval: int = 1, start_now: bool = False, handle_error=None):
        if handle_error is None:
            self._handle_error = handle_error_f
        self._controller = AsyncController(interval, start_now)
        self._queue = queue
        self._action = action

        try:
            loop = asyncio.get_running_loop()
            self._task = loop.create_task(self.run_loop())
        except RuntimeError:
            self._task = asyncio.ensure_future(self.run_loop())
            self._task = asyncio.create_task(self.run_loop())

    def close(self):
        self._controller.close()
        if not self._task.done():
            self._task.cancel()

    async def run_loop(self):
        try:
            while self._controller.running:
                await self._controller.wait()

                if not self._controller.running:
                    break

                if inspect.iscoroutinefunction(self._action):
                    result = await self._action()
                else:
                    result = await asyncio.to_thread(self._action)

                if result is None:
                    continue

                if isinstance(result, Exception):
                    raise result

                if isinstance(result, (list, tuple, set)):
                    tasks = [self._queue.enqueue(QueueData(result=r)) for r in result]
                else:
                    tasks = [self._queue.enqueue(QueueData(result=result))]

                if tasks:
                    asyncio.create_task(self._gather_and_trigger(tasks))

        except asyncio.CancelledError:
            raise
        except Exception as e:
            traceback.print_exception(e)
            if not self._handle_error(e):
                raise e
        finally:
            self._controller.close()
            self._task.cancel()

    async def _gather_and_trigger(self, tasks):
        try:
            await asyncio.gather(*tasks)
            self._controller.trigger()
        except Exception as e:
            self._handle_error(e)

def get_producer_result(queue_data: QueueData) -> Any:
    return queue_data.attribute("result")