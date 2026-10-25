import asyncio
import unittest

from index_queue.index_queue import IndexQueue, new_index_queue
from queue_controller.helpers import start_pipeline, stop_pipeline
from queue_controller.queueData import QueueData


class TestIndexQueue(unittest.IsolatedAsyncioTestCase):

    async def test_index_queue_processing_and_completion(self):
        processed = []

        def stage_one(iq: IndexQueue, qd: QueueData):
            processed.append((qd.attribute("key"), qd.attribute("obj")))

        actions = new_index_queue(2, stage_one)
        iq = IndexQueue(actions)
        cfg = iq.load_stage_action("stage_one")

        async with asyncio.TaskGroup() as tg:
            start_pipeline(tg, [cfg.queue])
            try:
                iq.enqueue("stage_one", key="k1", value="v1")
                iq.enqueue("stage_one", key="k2", value="v2")
                # Trigger the producer action directly to flush index items into queue
                await cfg.service.action()
                await asyncio.wait_for(iq.wait_for_completions(), timeout=2.0)
            finally:
                cfg.service.close_controllers()
                await stop_pipeline([cfg.queue])

        assert sorted(processed) == [("k1", "v1"), ("k2", "v2")]
        assert sum(iq._in_flight_tasks.values()) == 0


if __name__ == "__main__":
    unittest.main()
