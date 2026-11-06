import unittest
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.outputs import ChatGeneration, ChatResult

from fsspecc.cleanfs.cleanfs_tool_agent import cleanfs_agent
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


class TestCleanFsAgent(unittest.IsolatedAsyncioTestCase):

    async def test_build_governed_node(self):
        raw_csv = "c0,c1\n0,1\n2,3\n"
        clean_csv = "c0,c1\n4,5\n6,7\n"
        model = ScriptedChatModel(
            script=[
                AIMessage(
                    content="",
                    tool_calls=[
                        {"name": "save_raw_file", "args": {"csv_data": raw_csv}, "id": "c1", "type": "tool_call"}
                    ],
                ),
                AIMessage(content="Saved raw csv"),
                AIMessage(
                    content="",
                    tool_calls=[
                        {"name": "save_clean_file", "args": {"csv_data": clean_csv}, "id": "c2", "type": "tool_call"}
                    ],
                ),
                AIMessage(content="Saved clean csv"),
            ]
        )
        agent = await cleanfs_agent(model=model, emb=None, pool=None, store=None)
        commands = [f"save raw:\n{raw_csv}", f"save clean:\n{clean_csv}"]
        results = [r async for r in arun_commands(agent, "user", "cleanfs", commands, request_id="req_clean_1")]
        assert len(results) == 2

        df_raw = agent.filesystem.get_raw_file("req_clean_1", use_pipe=True)
        df_clean = agent.filesystem.get_clean_file("req_clean_1", use_pipe=True)
        assert len(df_raw) == 2
        assert len(df_clean) == 2


if __name__ == "__main__":
    unittest.main()
