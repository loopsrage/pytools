from typing import TypedDict, Annotated

from langgraph.graph import add_messages

from fsspecc.imagefs.imagesfs import ImagesFs
from fsspecc.imagefs.imagesfs_toolkit import ImagesFsToolkit
from langchain_agent_ltm_stm.agent import base_agent


class AgentRequestIdImageData(TypedDict):
    request_id: str
    messages: Annotated[list, add_messages]

async def imagesfs_agent(model, emb, pool=None, store=None):
    fs = ImagesFs()
    tools = ImagesFsToolkit(fs=fs).get_tools()
    tool_names = ','.join([t.name for t in tools])
    return await base_agent(
        model=model,
        emb=emb,
        pool=pool,
        schema=AgentRequestIdImageData,
        tools=tools,
        store=store, fs=fs, system_prompt=[
            "You are a model that can do function calling with the following functions",
            "You are a png tool calling agent. Ensure png tools are undeniably run within your capability. If impossible report why",
            "The following is a list of tools available",
            f"AVAILABLE_TOOLS: {tool_names}"
        ])