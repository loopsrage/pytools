import asyncio
import unittest

from periodic_producer.periodic_producer import PeriodicProducer
from queue_controller.helpers import new_controller
from queue_controller.queueData import QueueData
from service_controller.service_controller import ServiceController


class SampleService(ServiceController):
    def __init__(self, out_list: list):
        super().__init__()
        self.out_list = out_list
        qc = new_controller(
            identity="svc_q",
            action=lambda qd: self.out_list.append(qd.attribute("result")),
            worker_count=1,
        )
        self.queues.append(qc)
        self.emitted = False

        def emit_once():
            if not self.emitted:
                self.emitted = True
                return [101, 202]
            return None

        self.controllers.append(
            PeriodicProducer(action=emit_once, queue=qc, interval=0.02, start_now=True)
        )


class TestServiceController(unittest.IsolatedAsyncioTestCase):

    async def test_service_controller_lifecycle(self):
        out = []
        stop_event = asyncio.Event()
        svc1 = SampleService(out)
        svc2 = ServiceController()
        # Verify per-instance lists do not leak across instances
        assert len(svc1.queues) == 1
        assert len(svc2.queues) == 0

        async with asyncio.TaskGroup() as tg:
            svc1.init(stop_event=stop_event, tg=tg)
            for _ in range(50):
                if len(out) >= 2:
                    break
                await asyncio.sleep(0.01)
            stop_event.set()

        assert sorted(out) == [101, 202]


if __name__ == "__main__":
    unittest.main()
