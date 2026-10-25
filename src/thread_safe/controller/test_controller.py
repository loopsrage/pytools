import asyncio
import unittest
from concurrent.futures import ThreadPoolExecutor

from thread_safe.controller.controller import AsyncController, Controller


def action(ct: Controller, max_ticks: int) -> int:
    ticks = 0
    while ticks < max_ticks:
        ct.wait()
        ct.clear()
        ticks += 1
    ct.close()
    return ticks


class TestController(unittest.IsolatedAsyncioTestCase):

    def test_sync_controller_bounded_ticks(self):
        with ThreadPoolExecutor(max_workers=2) as executor:
            ct0 = Controller(interval=0.02, start_now=True)
            ct1 = Controller(interval=0.02, start_now=False)
            f0 = executor.submit(action, ct0, 3)
            f1 = executor.submit(action, ct1, 2)
            assert f0.result(timeout=2.0) == 3
            assert f1.result(timeout=2.0) == 2

    def test_sync_controller_close_unblocks_wait(self):
        ct = Controller(interval=10.0, start_now=False)
        ct.close()
        assert ct.wait() is False

    async def test_async_controller_ticks_and_trigger(self):
        act = AsyncController(interval=0.02, start_now=True)
        # First wait returns immediately due to start_now=True
        await asyncio.wait_for(act.wait(), timeout=0.01)
        # Second wait completes after interval (~0.02s)
        await asyncio.wait_for(act.wait(), timeout=0.2)

        # Manual trigger wakes wait early on a long interval
        act_long = AsyncController(interval=5.0, start_now=False)
        asyncio.get_running_loop().call_later(0.02, act_long.trigger)
        await asyncio.wait_for(act_long.wait(), timeout=0.2)
        act.close()
        act_long.close()


if __name__ == '__main__':
    unittest.main()
