import asyncio
import unittest
from typing import Annotated, TypedDict
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langgraph.graph import add_messages

from agent_queue_controller.agent_queue_controller import (
    get_exchange_specialists,
    set_exchange_specialists,
    specialist_index_queue_data,
    user_agent_queue_controller,
)
from fsspecc.base_fsspecfs.base_tool_agent import fs_agent
from fsspecc.memfs.memfs_tool_agent import memfs_agent
from indexes.specialist_index.specialist_index import SpecialistIndex
from langchain_agent_ltm_stm.agent import base_agent
from queue_controller.helpers import new_controller, start_pipeline, stop_pipeline
from queue_controller.queueData import QueueData


class DeterministicAgentModel(BaseChatModel):
    @property
    def _llm_type(self) -> str:
        return "deterministic"

    def bind_tools(self, tools, **kwargs):
        return self

    def _generate(self, messages: list[BaseMessage], stop=None, run_manager=None, **kwargs) -> ChatResult:
        return ChatResult(generations=[ChatGeneration(message=AIMessage(content="formatted_ok"))])


class AgentRequestIdKeyValue(TypedDict):
    request_id: str
    messages: Annotated[list, add_messages]


class TestAgentQueueController(unittest.IsolatedAsyncioTestCase):

    async def test_agent_queue_controller_pipeline(self):
        llm = DeterministicAgentModel()
        node_specialist_index_1 = SpecialistIndex("group_1")
        node_specialist_index_2 = SpecialistIndex("group_2")

        agent_group_1 = {
            "filesystem": await fs_agent(model=llm, emb=None),
            "memory": await memfs_agent(model=llm, emb=None),
        }
        request_formatter = await base_agent(
            schema=AgentRequestIdKeyValue,
            model=llm,
            emb=None,
            system_prompt=["Format prompt"],
        )
        node_specialist_index_1.register_agents(agent_group_1)
        node_specialist_index_2.register_agents({"request_formatter": request_formatter})

        exchanged = []
        done = asyncio.Event()

        async def exchange_specialist(user_id, node_index: SpecialistIndex, queue_index: SpecialistIndex, queue_data: QueueData):
            ainvoke, _ = queue_index.async_agent_user_invoker("request_formatter", user_id)
            res = await ainvoke(queue_data["important_data"])
            queue_data["formatted_response"] = res.result["messages"][-1].content
            exchanged.append(queue_data["formatted_response"])

        async def after_action(qd: QueueData):
            done.set()

        after_node = new_controller("after", action=after_action, worker_count=1)
        qc = user_agent_queue_controller("user", node_specialist_index_1, "filesystem", exchange_specialist)
        qc.set_next(after_node)
        pl = [qc, after_node]

        qd = specialist_index_queue_data(node_specialist_index_2, True)
        assert get_exchange_specialists(qd) is True
        set_exchange_specialists(qd, False)
        assert get_exchange_specialists(qd) is False
        set_exchange_specialists(qd, True)
        qd["important_data"] = "Save 1 barrel of oil"

        async with asyncio.TaskGroup() as tg:
            start_pipeline(tg=tg, nodes=pl)
            try:
                await qc.enqueue(qd)
                await asyncio.wait_for(done.wait(), timeout=2.0)
            finally:
                await stop_pipeline(nodes=pl)

        assert exchanged == ["formatted_ok"]
        assert qd["formatted_response"] == "formatted_ok"


if __name__ == "__main__":
    unittest.main()
