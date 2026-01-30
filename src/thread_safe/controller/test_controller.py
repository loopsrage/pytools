import unittest
from concurrent.futures import ThreadPoolExecutor

import pytest

from lib.thread_safe.controller.controller import Controller


def action(ct: Controller, index: int):
    while True:
        print(f"starting {index}")
        ct.wait()
        ct.clear()
        print(f"finished {index}")

class TestController(unittest.IsolatedAsyncioTestCase):

    @pytest.mark.asyncio
    async def test_controller(self):
        routines = []
        with ThreadPoolExecutor() as executor:
            ct = Controller(interval=1, start_now=False)
            routines.append(executor.submit(action, ct, index=0))

            ct1 = Controller(interval=2, start_now=False)
            routines.append(executor.submit(action, ct1, index=1))

            ct2 = Controller(interval=6, start_now=False)
            routines.append(executor.submit(action, ct2, index=2))

            ct3 = Controller(interval=12, start_now=False)
            routines.append(executor.submit(action, ct3, index=3))

        for r in routines:
            print(r.result())




if __name__ == '__main__':
    unittest.main()