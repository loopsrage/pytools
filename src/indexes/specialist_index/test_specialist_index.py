import unittest
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.outputs import ChatGeneration, ChatResult

from fsspecc.base_fsspecfs.base_tool_agent import fs_agent
from fsspecc.cleanfs.cleanfs_tool_agent import cleanfs_agent
from fsspecc.imagefs.imagesfs_tool_agent import imagesfs_agent
from fsspecc.memfs.memfs_tool_agent import memfs_agent
from indexes.specialist_index.specialist_index import SpecialistIndex


class EchoChatModel(BaseChatModel):
    @property
    def _llm_type(self) -> str:
        return "echo"

    def bind_tools(self, tools, **kwargs):
        return self

    def _generate(self, messages: list[BaseMessage], stop=None, run_manager=None, **kwargs) -> ChatResult:
        return ChatResult(generations=[ChatGeneration(message=AIMessage(content="ack"))])


class TestSpecialistIndex(unittest.IsolatedAsyncioTestCase):

    async def test_agent_indexes_and_invokers(self):
        model = EchoChatModel()
        registry = SpecialistIndex("testing")
        age = {
            "memory": await memfs_agent(model=model, emb=None),
            "clean_fs": await cleanfs_agent(model=model, emb=None),
            "filesystem": await fs_agent(model=model, emb=None),
            "images": await imagesfs_agent(model=model, emb=None),
        }
        registry.register_agents(age)

        ri = registry.agent_reverse_index("filesystem")
        assert ri == 271111081996697077377771599539766263747
        assert registry.agent_name_from_reverse_index(ri) == "filesystem"
        assert set(registry.agent_list) == {"memory", "clean_fs", "filesystem", "images"}

        invoke, invoke_commands = registry.agent_user_invoker("filesystem", "user_1")
        sync_res = invoke("hello")
        assert sync_res.result["messages"][-1].content == "ack"
        sync_batch = list(invoke_commands(["cmd1", "cmd2"]))
        assert len(sync_batch) == 2

        ainvoke, ainvoke_commands = registry.async_agent_user_invoker("filesystem", "user_1")
        res = await ainvoke("hello")
        assert res.result["messages"][-1].content == "ack"
        abatch = [r async for r in ainvoke_commands(["cmd1", "cmd2"])]
        assert len(abatch) == 2


if __name__ == "__main__":
    unittest.main()
