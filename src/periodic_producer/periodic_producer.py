import asyncio
import inspect
import traceback
from typing import Callable, Union, Any, Optional

from queue_controller.queueController import QueueController
from queue_controller.queueData import QueueData
from thread_safe.controller.controller import AsyncController


def handle_error_f(e: Exception) -> bool:
    traceback.print_exception(e)
    return False


class PeriodicProducer:
    _controller: AsyncController
    _queue: Optional[QueueController]
    _action: Callable[[], Union[list[Any] | None]]
    _handle_error: Callable[[Exception], bool]
    _task: Optional[asyncio.Task]

    def __init__(
        self,
        action: Callable[[], Any],
        queue: Optional[QueueController] = None,
        interval: int = 1,
        start_now: bool = False,
        handle_error=None,
    ):
        self._handle_error = handle_error if handle_error is not None else handle_error_f
        self._controller = AsyncController(interval, start_now)
        self._controller.start()
        self._queue = queue
        self._action = action
        self._task: Optional[asyncio.Task] = None
        self.start()

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self.run_loop())

    def close(self):
        self._controller.close()
        if self._task is not None:
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
                    self._handle_error(result)
                    continue

                if not isinstance(result, list):
                    result = [result]

                tasks = []
                for r in result:
                    if self._queue is not None:
                        tasks.append(self._queue.enqueue(QueueData(result=r)))
                await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            traceback.print_exception(e)
            if not self._handle_error(e):
                raise e
        finally:
            self._controller.close()

    @property
    def running(self) -> bool:
        return self._controller.running


def get_producer_result(queue_data: QueueData) -> Any:
    return queue_data.attribute("result")
