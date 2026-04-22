import unittest

from fsspecc.cleanfs.cleanfs_tool_agent import cleanfs_agent
from langchain_agent_ltm_stm.agent import arun_commands
from langlib.pgstore import PGS


async def get_pool():
    pass


class Test(unittest.IsolatedAsyncioTestCase):

    async def test_build_governed_node(self):
        store = PGS()
        try:
            pool = await get_pool()
            agent = await cleanfs_agent(pool, store)
            for i in range(0, 1):
                commands = [
                    """save the following raw csv file: 
0,1,2,3,4,5,6,7,8
0,1,2,3,4,5,6,7,8
0,1,2,3,4,5,6,7,8
0,1,2,3,4,5,6,7,8
0,1,2,3,4,5,6,7,8
0,1,2,3,4,5,6,7,8
0,1,2,3,4,5,6,7,8
0,1,2,3,4,5,6,7,8
""",
                    """save the following clean csv file: 
4,5,6,7,8,0,1,2,3
4,5,6,7,8,0,1,2,3
4,5,6,7,8,0,1,2,3
4,5,6,7,8,0,1,2,3
4,5,6,7,8,0,1,2,3
4,5,6,7,8,0,1,2,3
4,5,6,7,8,0,1,2,3
4,5,6,7,8,0,1,2,3
""",
                ]
                async for r in arun_commands(agent, "user", "cleanfs", commands):
                    print(r)
        except Exception as e:
            raise e
