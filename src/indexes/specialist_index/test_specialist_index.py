import os
import unittest

from dotenv import load_dotenv
from starlette.routing import Router

from src.fsspecc.base_fsspecfs.base_fsspecfs import FSBase
from src.fsspecc.base_fsspecfs.base_tool_agent import fs_agent
from src.fsspecc.cleanfs.cleanfs_tool_agent import cleanfs_agent
from src.fsspecc.imagefs.imagesfs_tool_agent import imagesfs_agent
from src.fsspecc.memfs.memfs_tool_agent import memfs_agent
from src.indexes.connection_index.connection_index import ConnectionIndex
from src.indexes.specialist_index.specialist_index import SpecialistIndex
from src.langlib.pgstore import PGS
from src.markov.mongo_mdp import MongoMDP
from src.settings.helper import restore, setting

load_dotenv()
conn_index = ConnectionIndex()
restore(os.getenv("ENV_FILE"))

class Test(unittest.IsolatedAsyncioTestCase):
    _mk = None
    async def test_agent_index(self):
        pool, _ = conn_index.register("pool", get_async_connection_pool(dsn=setting("LocalDatabase", "DSN")))

        registry = SpecialistIndex("testing")
        store=PGS()
        age = {
            "filesystem": await fs_agent(pool, store),
            "memory": await memfs_agent(pool, store),
            "clean_fs": await cleanfs_agent(pool, store),
            "images": await imagesfs_agent(pool, store)
        }
        registry.register_agents(age)
        uiv = registry.agent_user_invoker("filesystem", "user")
        result = await uiv[0]("save file_path 'testing', file_data: 'fishsticks'")
        print(result.result)
        print(list(age["filesystem"].filesystem.list_files()))


    async def test_agent_indexes(self):
        pool, _ = conn_index.register("pool", get_async_connection_pool(dsn=setting("LocalDatabase", "DSN")))

        registry = SpecialistIndex("testing")
        store=PGS()
        age = {
            "memory": await memfs_agent(pool, store),
            "clean_fs": await cleanfs_agent(pool, store),
            "filesystem": await fs_agent(pool, store),
            "images": await imagesfs_agent(pool, store)
        }
        registry.register_agents(age)

        ri = registry.agent_reverse_index("filesystem")
        print(ri)
        print(registry.agent("filesystem"))
        print(registry.agent_name_from_reverse_index(ri))
        assert ri == 271111081996697077377771599539766263747
        assert registry.agent_name_from_reverse_index(ri) == "filesystem"



    async def test_mk_m(self):
        registry = SpecialistIndex("testing")
        pool, _ = conn_index.register("pool", get_async_connection_pool(dsn=setting("LocalDatabase", "DSN")))
        store=PGS()
        pool = None
        store = None
        age = {
            "filesystem": await fs_agent(pool, store),
            "memory": await memfs_agent(pool, store),
            "clean_fs": await cleanfs_agent(pool, store),
            "images": await imagesfs_agent(pool, store)
        }
        registry.register_agents(age)
        router = Router(registry)
        await router.test("user_id", "create a file for me named 'file1.txt' with the letter 'a' as content, then list the files")
        print(router.build_friendly())

def fs_inj(filesystem: FSBase):
    async def norm_mag(nxt, current):
        print(nxt, current)
        return 1
    return norm_mag

class TestSp(unittest.IsolatedAsyncioTestCase):
    _mk = None
    def setUp(self):
        client = AsyncMongoClient("mongodb://localhost:27017")
        self._mk = MongoMDP("testing", client, .1)

    async def test_mk_other(self):
        registry = SpecialistIndex("testing")
        pool = None
        fs = FSBase(filesystem="memory")
        store = None
        age = {
            "filesystem": await fs_agent(pool, store),
            "memory": await memfs_agent(pool, store),
            "clean_fs": await cleanfs_agent(pool, store),
            "images": await imagesfs_agent(pool, store)
        }
        registry.register_agents(age)

        user_id = ""
        user_query = "create a file"
        last = None
        current = None
        while True:
            try:
                if current is None:
                    current = registry.agent_list[0]

                invoker, _ = registry.agent_user_invoker(
                    agent_name=registry.agent(current),
                    user_id=user_id)
                result = await invoker(user_query)
                await self._mk.call(fs_inj(fs), current, last, current)
                last = current
            except ExceptionGroup as e:
                print(str(e))
                return 0.0
