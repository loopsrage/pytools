import io
import unittest
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.outputs import ChatGeneration, ChatResult

from fsspecc.memfs.memfs_tool_agent import memfs_agent
from langchain_agent_ltm_stm.agent import arun_commands


class ScriptedChatModel(BaseChatModel):
    script: list[AIMessage] = []
    idx: int = 0

    @property
    def _llm_type(self) -> str:
        return "scripted"

    def bind_tools(self, tools, **kwargs):
        return self

    def _generate(self, messages: list[BaseMessage], stop=None, run_manager=None, **kwargs) -> ChatResult:
        msg = self.script[min(self.idx, len(self.script) - 1)]
        self.idx += 1
        return ChatResult(generations=[ChatGeneration(message=msg)])


class TestMemFsAgent(unittest.IsolatedAsyncioTestCase):

    async def test_build_governed_node(self):
        model = ScriptedChatModel(
            script=[
                AIMessage(
                    content="",
                    tool_calls=[
                        {"name": "store", "args": {"key": "save_key", "value": "value_knife"}, "id": "c1", "type": "tool_call"}
                    ],
                ),
                AIMessage(content="Stored save_key"),
                AIMessage(
                    content="",
                    tool_calls=[
                        {"name": "load", "args": {"key": "save_key"}, "id": "c2", "type": "tool_call"}
                    ],
                ),
                AIMessage(content="Loaded value_knife"),
            ]
        )
        agent = await memfs_agent(model=model, emb=None, pool=None, store=None)
        commands = [
            "store the key 'save_key', and the value is 'value_knife'",
            "load the key save_key",
        ]
        results = [r async for r in arun_commands(agent, "user", "memfs", commands, request_id="req_mem_1")]
        assert len(results) == 2

        buf = io.BytesIO()
        agent.filesystem.load("req_mem_1", "save_key", buf)
        assert buf.getvalue() == b"value_knife"


if __name__ == "__main__":
    unittest.main()
