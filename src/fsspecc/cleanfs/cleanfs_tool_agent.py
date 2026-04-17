from typing import Annotated, TypedDict, NotRequired

from langgraph.graph import add_messages

from src.fsspecc.cleanfs.cleanfs import CleanFs
from src.fsspecc.cleanfs.cleanfs_toolkit import CleanFSToolkit
from src.langchain_agent_ltm_stm.agent import base_agent


class AgentRequestIdCsvData(TypedDict):
    request_id: str
    csv_data: NotRequired[str]
    messages: Annotated[list, add_messages]

async def cleanfs_agent(model, emb, pool=None, store=None):
    fs = CleanFs()
    tools = CleanFSToolkit(fs=fs).get_tools()
    tool_names = ','.join([t.name for t in tools])
    return await base_agent(
        schema=AgentRequestIdCsvData,
        pool=pool,
        model=model,
        emb=emb,
        tools=tools,
        store=store, fs=fs, system_prompt=[
            "You are a model that can do function calling with the following functions",
            "You are a raw and clean csv tool calling agent. Ensure raw and clean csv tools are undeniably run within your capability. If impossible report why",
            "The following is a list of tools available",
            f"AVAILABLE_TOOLS: {tool_names}"
        ])