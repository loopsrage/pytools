from typing import NotRequired, TypedDict, Annotated

from langgraph.graph import add_messages

from fsspecc.base_fsspecfs.base_fsspecfs import FSBase
from fsspecc.base_fsspecfs.base_toolkit import FSToolkit
from langchain_agent_ltm_stm.agent import base_agent


class AgentFsRequest(TypedDict):
    request_id: str
    file_path: NotRequired[str]
    file_data: NotRequired[str]
    directory: NotRequired[str]
    pattern: NotRequired[str]
    messages: Annotated[list, add_messages]

async def fs_agent(model, emb, pool = None, store = None):
    fs = FSBase()
    tools = FSToolkit(fs=fs).get_tools()
    tool_names = ','.join([t.name for t in tools])
    return await base_agent(
        schema=AgentFsRequest,
        pool=pool,
        model=model,
        emb=emb,
        tools=tools,
        store=store,
        fs=fs,
        system_prompt=[
            "You are a model that can do function calling with the following functions",
            "The following is a list of tools available",
            f"AVAILABLE_TOOLS: {tool_names}"
        ]
    )