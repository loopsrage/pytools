import asyncio
import traceback
import uuid
from asyncio import TaskGroup
from concurrent.futures import ThreadPoolExecutor, Future
from typing import Callable, Iterable, Coroutine, Any

from queue_controller.queueController import QueueController
from queue_controller.queueData import QueueData


def simple_error_handler(e: Exception) -> bool:
    traceback.print_exception(e)
    return True

def link_pipeline(nodes: Iterable[QueueController]) -> list[QueueController]:
    node_list = list(nodes)

    if not node_list:
        return []

    for i in range(len(node_list)-1):
        current_node = node_list[i]
        next_node = node_list[i+1]
        current_node.set_next(next_node)

    return node_list

def start_pipeline(tg: TaskGroup, nodes: list[QueueController]) -> list[asyncio.Task]:
    return [tg.create_task(node.queue_action()) for node in nodes]

def gather_results(futures: list[Future]):
    return [f.result() for f in futures]

async def stop_pipeline(nodes: list[QueueController]) -> None:
    for node in nodes:
        await node.close()

def default_queue_action(queue_data: QueueData) -> None:
    total = 0
    for i in range(1, 900):
        total += i * 2
        if total % 1000 == 0:
            print(total, list(zip(queue_data.trace(), queue_data.trace_duration())))

def new_controller(identity: str = None, executor: ThreadPoolExecutor = None, action: Callable[[QueueData], Exception | None] | Coroutine[Any, Any, Exception | None] = None, **kwargs) -> QueueController:
    if action is None:
        action = default_queue_action

    if identity is None:
        identity = f"{uuid.uuid4().hex}-{action.__name__}"

    def _controller() -> QueueController:
        return QueueController(identity=identity, action=action, executor=executor, **kwargs)

    return _controller()

