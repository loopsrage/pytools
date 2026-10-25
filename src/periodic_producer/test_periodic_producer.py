import asyncio
import unittest

from periodic_producer.periodic_producer import PeriodicProducer, get_producer_result
from queue_controller.helpers import start_pipeline, stop_pipeline
from queue_controller.queueController import QueueController
from queue_controller.queueData import QueueData


class TestPeriodicProducer(unittest.IsolatedAsyncioTestCase):

    async def test_periodic_producer_sync_and_async(self):
        received = []
        done_event = asyncio.Event()

        async def consumer(qd: QueueData):
            val = get_producer_result(qd)
            received.append(val)
            if len(received) >= 4:
                done_event.set()

        qc = QueueController(identity="consumer", action=consumer, worker_count=1)
        batches = [["a", "b"], ["c", "d"], None]
        idx = 0

        def producer_action():
            nonlocal idx
            if idx < len(batches):
                out = batches[idx]
                idx += 1
                return out
            return None

        async with asyncio.TaskGroup() as tg:
            start_pipeline(tg, [qc])
            pp = PeriodicProducer(
                action=producer_action,
                queue=qc,
                interval=0.02,
                start_now=True,
            )
            try:
                await asyncio.wait_for(done_event.wait(), timeout=2.0)
            finally:
                pp.close()
                await stop_pipeline([qc])

        assert sorted(received[:4]) == ["a", "b", "c", "d"]


if __name__ == "__main__":
    unittest.main()
