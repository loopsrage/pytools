import asyncio
import logging
import time
import traceback
from concurrent import futures
from typing import Optional, Callable, Union, Coroutine, Any

from queue_controller.queueData import QueueData
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

def debug_action(item: QueueData) -> None:
    print(item)

def handle_error(e: Exception) -> bool:
    traceback.print_exception(e)
    logger.error("An error occurred during queue execution", exc_info=True)
    return True

class QueueController:
    _identity: str
    _queue: asyncio.Queue = None
    _broadcast: dict[str, 'QueueController']

    _action: Callable[[QueueData], Exception | None] | Coroutine[Any, Any, Exception | None]
    _next_queue_controller: Optional['QueueController'] = None
    _error_handler: Callable[[Exception], bool]
    worker_count: int

    def __init__(self,
                 action: Callable[[QueueData], Exception | None] | Coroutine[Any, Any, Exception | None],
                 executor: futures.ThreadPoolExecutor = None,
                 max_queue_size: int = None,
                 identity: str = None,
                 error_handler: Callable[[Exception], bool] = None, worker_count=5) -> None:

        self._error_handler = error_handler
        if self._error_handler is None:
            self._error_handler = handle_error
        self.worker_count = worker_count
        if max_queue_size is None:
            max_queue_size = 1024

        self._max_queue_size = max_queue_size
        self._identity = identity
        self._action = action
        self._broadcast = {}

        self._executor = executor
        if self._executor is None:
            self._executor = futures.ThreadPoolExecutor()

    @property
    def identity(self):
        if self._identity is None:
            return ""
        return self._identity

    @property
    def queue(self) -> asyncio.Queue:
        if self._queue is None:
            self._queue = asyncio.Queue(maxsize=self._max_queue_size)
        return self._queue

    @property
    def next_queue_controller(self) -> Union['QueueController', None]:
       return self._next_queue_controller

    def set_next(self, next_queue_controller: 'QueueController') -> None:
        self._next_queue_controller = next_queue_controller

    def set_broadcast(self, broadcast_to: dict[str, 'QueueController']) -> None:
        self._broadcast = broadcast_to

    async def enqueue(self, queue_data: QueueData) -> None:
        await self.queue.put(queue_data)

    async def close(self) -> None:
        for _ in range(self.worker_count):
            await self.queue.put(None)
        await self.queue.join()

    async def broadcast(self, item) -> None:
        for identity, target in self._broadcast.items():
            await target.enqueue(item.copy_derivative(identity))

    async def queue_action(self) -> None:
        while True:
            item: QueueData = await self.queue.get()
            if item is None:
                self.queue.task_done()
                return

            item.append_trace(self.identity)

            try:
                start_time = time.perf_counter()
                if asyncio.iscoroutinefunction(self._action):
                    result = await self._action(item)
                else:
                    result = await asyncio.to_thread(self._action, item)

                await self.broadcast(item)

                duration = time.perf_counter() - start_time
                item.append_duration(duration)

                if isinstance(result, Exception):
                    raise result

                next_node = self.next_queue_controller
                if next_node:
                    await next_node.enqueue(item)
            except Exception as e:
                e.add_note(f"{item.trace()}")
                e.add_note(f"{item.kwargs()}")
                if not self._error_handler(e):
                    raise e
            finally:
                self.queue.task_done()

