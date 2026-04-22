from typing import NotRequired, TypedDict, Any, Annotated

from langgraph.graph import add_messages

from src.fsspecc.memfs.memfs import MemFS
from src.fsspecc.memfs.memfs_toolkit import MemFSToolkit
from src.langchain_agent_ltm_stm.agent import base_agent


class AgentRequestIdKeyValue(TypedDict):
    request_id: str
    key: str
    value: NotRequired[Any]
    messages: Annotated[list, add_messages]


async def memfs_agent(model, emb, pool=None, store=None):
    fs = MemFS()
    tools = MemFSToolkit(fs=fs).get_tools()
    tool_names = ','.join([t.name for t in tools])
    return await base_agent(
        model=model,
        emb=emb,
        schema=AgentRequestIdKeyValue,
        pool=pool,
        store=store,
        tools=tools,
        system_prompt=[
            "You are a model that can do function calling with the following functions",
            "You are a memory tool calling agent. Ensure memory store and load tools are undeniably run within your capability. If impossible report why",
            "The following is a list of tools available",
            f"AVAILABLE_TOOLS: {tool_names}"
        ],
        fs=fs,
    )
