import asyncio
import unittest

from periodic_producer.periodic_producer import PeriodicProducer, get_producer_result
from queue_controller.queueController import QueueController
from queue_controller.queueData import QueueData


def context_for_mock_action(*args, **kwargs):

    def mock_action():
        print("hey", flush=True)
        return kwargs

    return mock_action


def mock_controller(queue_data: QueueData):
    print(queue_data)

class Test(unittest.IsolatedAsyncioTestCase):

    async def test_periodic_producer(self):
        qc = QueueController(action=get_producer_result)
        result = PeriodicProducer(action=context_for_mock_action("arg1"), queue=qc, interval=1, start_now=True)

        x = 0
        while True:
            await asyncio.sleep(3)

            x = x + 1
            if x >= 10:
                break

        print(result)
