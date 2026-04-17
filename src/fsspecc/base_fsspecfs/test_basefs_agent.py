import unittest

from src.fsspecc.base_fsspecfs.base_tool_agent import fs_agent
from src.langchain_agent_ltm_stm.agent import arun_commands
from src.langlib.pgstore import PGS


class Get:
    pass


class Test(unittest.IsolatedAsyncioTestCase):

    async def test_build_governed_node(self):
        store = PGS()
        try:
            pool = await Get()
            agent = await fs_agent(pool, store)
            for i in range(0, 1):
                commands = [
                    "store the key 'save_key', and the value is 'value_knife'",
                    "store the key 'save_key1', and the value is 'value_knife 321'",
                    "store the key 'asdf', and the value is 'vavava gabasetrh'",
                    "load the key asdf",
                    "load the key save_key1",
                    "load the key save_key",
                ]
                async for r in arun_commands(agent, "user", "fs", commands):
                    print(r)
        except Exception as e:
            raise e


