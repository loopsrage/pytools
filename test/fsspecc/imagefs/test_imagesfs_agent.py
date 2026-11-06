import io
import unittest
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from PIL import Image
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.outputs import ChatGeneration, ChatResult

from fsspecc.imagefs.imagesfs_tool_agent import imagesfs_agent
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


class TestImagesFsAgent(unittest.IsolatedAsyncioTestCase):

    async def test_imagesfs_agent(self):
        fig, ax = plt.subplots()
        ax.plot([1, 2, 3], [1, 4, 9])

        model = ScriptedChatModel(
            script=[
                AIMessage(
                    content="",
                    tool_calls=[
                        {"name": "save_png_file", "args": {"file_name": "silly.png"}, "id": "c1", "type": "tool_call"}
                    ],
                ),
                AIMessage(content="Saved silly.png"),
                AIMessage(
                    content="",
                    tool_calls=[
                        {"name": "list_images", "args": {}, "id": "c2", "type": "tool_call"}
                    ],
                ),
                AIMessage(content="Listed images"),
            ]
        )
        agent = await imagesfs_agent(model=model, emb=None, pool=None, store=None)
        commands = ["Save current figure as silly.png", "list all current pngs"]
        results = [r async for r in arun_commands(agent, "user", "imagesfs", commands, request_id="req_img_1")]
        plt.close(fig)

        assert len(results) == 2
        png_bytes = agent.filesystem.get_png_bytes("req_img_1", "silly.png")
        with Image.open(io.BytesIO(png_bytes)) as img:
            assert img.format == "PNG"
            assert img.size[0] > 0 and img.size[1] > 0


if __name__ == "__main__":
    unittest.main()
