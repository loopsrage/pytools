import uuid
from typing import List, Any

from pydantic import BaseModel
from langchain.agents import create_agent
from langchain.agents.middleware import SummarizationMiddleware
from langchain_core.messages import SystemMessage
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from fsspecc.base_fsspecfs.base_fsspecfs import FSpecFS


def _system_message(inject: str):
    content = f"""
### CONTEXT
{inject}
"""
    return SystemMessage(content=content, additional_kwargs={"role": "developer"})

def _RAG_message(inject: str):
    content = f"""
### EXTERNAL CONTEXT
{inject}
"""
    return SystemMessage(content=content, additional_kwargs={"role": "developer"})

def search_past_memories(store, user_id, namespace, query, threshold: float):
    if store is None:
        return []

    past_memories = store.search(user_id, namespace, query)
    return _system_message(format_past_memories([item for item in past_memories if item.score > threshold]))

async def asearch_past_memories(store, user_id, namespace, query, threshold: float):
    if store is None:
        return []

    past_memories = await store.asearch(user_id, namespace, query)
    return _system_message(format_past_memories([item for item in past_memories if item.score > threshold]))

def format_past_memories(past_memories):
    return "\n".join([
        f"[{item.value['text']}:{item.score:.2f}]"
        for item in past_memories
    ])


class AsyncPostgresSaver:
    pass


class DefaultAgent:
    _agent = None
    _store = None
    _state = None
    _identity = None
    _fs: FSpecFS = None

    checkpointer: AsyncPostgresSaver = None

    def __init__(self, agent, state, store = None, fs = None, identity = None):
        self._agent = agent
        self._store = store
        self._state = state
        self._fs = fs

        self._identity = identity
        if self._identity is None:
            self._identity = uuid.uuid4().hex

    @property
    def tools(self):
        for key, value in self._agent.nodes.get("tools").bound.tools_by_name.items():
            yield key, value

    @property
    def identity(self):
        return self._identity

    @property
    def store(self):
        return self._store

    @property
    def filesystem(self):
        return self._fs

    async def ainvoke(self, user_id: Any, request_id: str, namespace: str, query, config, threshold: float):
        if self.store is not None:
            past_memories = await search_past_memories(self.store, user_id, namespace, query, threshold)
            formatted_memories = format_past_memories(past_memories)
            msg = _system_message(formatted_memories)
            history = self._state.get("messages", [])
            response = await self._agent.ainvoke( {
                "messages": [msg] + history + [query],
                "request_id": request_id,
                "user_id": user_id
            }, config=config)
            agent_message = response["messages"][-1]
            await self.store.aput(user_id, request_id, namespace,  {"text": agent_message.content})
        else:
            response = await self._agent.ainvoke( {
                "messages": [query],
                "request_id": request_id,
                "user_id": user_id
            }, config=config)
            agent_message = response["messages"][-1]
        return {
            "messages": [agent_message],
            "request_id": request_id
        }


    def invoke(self, user_id: Any, request_id: str, namespace: str, query, config, threshold: float):
        if self.store is not None:
            past_memories = search_past_memories(self.store, user_id, namespace, query, threshold)
            formatted_memories = format_past_memories(past_memories)
            msg = _system_message(formatted_memories)
            history = self._state.get("messages", [])
            response = self._agent.invoke( {
                "messages": [msg] + history + [query],
                "request_id": request_id,
                "user_id": user_id
            }, config=config)
            agent_message = response["messages"][-1]
            self.store.put(user_id, request_id, namespace,  {"text": agent_message.content})
        else:
            response = self._agent.invoke( {
                "messages": [query],
                "request_id": request_id,
                "user_id": user_id
            }, config=config)
            agent_message = response["messages"][-1]
        return {
            "messages": [agent_message],
            "request_id": request_id
        }


class Config(BaseModel):
    dimensions: int
    embedding: Any
    fields: List[str]
    index_kwargs: dict
    summary_kwargs: dict
    create_agent_kwargs: dict
    pii_types: List[str]
    pii_strategy: str


class SummarizationMiddleware:
    pass


async def create_agent_pg_ltm_stm(model, state_schema, config: Config, store = None, pool = None, tools = None, fs = None):
    if store is not None:
        await store.ainit(dimensions=config.dimensions,
                          embedding=config.embedding,
                          fields=config.fields,
                          pool=pool,
                          index_kwargs=config.index_kwargs)
        store = store.store or None


    summary_args = config.summary_kwargs
    if summary_args is None:
        summary_args = {}

    mw = [
          SummarizationMiddleware(
              model=model,
              **summary_args
          )]

    checkpointer = None
    if pool is not None:
        checkpointer = AsyncPostgresSaver(conn=pool)
        await checkpointer.setup()

    agent = create_agent(
        model=model,
        tools=tools,
        checkpointer=checkpointer,
        store=store,
        state_schema=state_schema,
        **config.create_agent_kwargs,
        middleware=mw,
    )

    return DefaultAgent(agent, store=store, state=state_schema(), fs=fs)
