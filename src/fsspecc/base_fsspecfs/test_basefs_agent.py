import io
import unittest
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.outputs import ChatGeneration, ChatResult

from fsspecc.base_fsspecfs.base_tool_agent import fs_agent
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


class TestBaseFsAgent(unittest.IsolatedAsyncioTestCase):

    async def test_build_governed_node(self):
        model = ScriptedChatModel(
            script=[
                AIMessage(
                    content="",
                    tool_calls=[
                        {
                            "name": "save_file",
                            "args": {"file_path": "save_key.txt", "file_data": "value_knife"},
                            "id": "call_1",
                            "type": "tool_call",
                        }
                    ],
                ),
                AIMessage(content="Saved save_key.txt"),
                AIMessage(
                    content="",
                    tool_calls=[
                        {
                            "name": "get_file",
                            "args": {"file_path": "save_key.txt"},
                            "id": "call_2",
                            "type": "tool_call",
                        }
                    ],
                ),
                AIMessage(content="Loaded value_knife"),
            ]
        )
        agent = await fs_agent(model=model, emb=None, pool=None, store=None)
        commands = [
            "store the key 'save_key.txt', and the value is 'value_knife'",
            "load the key save_key.txt",
        ]
        results = []
        async for r in arun_commands(agent, "user", "fs", commands, request_id="req_fs_1"):
            results.append(r)

        assert len(results) == 2
        buf = io.BytesIO()
        agent.filesystem.read("/req_fs_1/save_key.txt", buf, use_pipe=True)
        assert buf.getvalue() == b"value_knife"


if __name__ == "__main__":
    unittest.main()
