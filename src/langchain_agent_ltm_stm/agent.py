import uuid
from typing import Any, AsyncGenerator, Dict, Union, List, Generator

from pydantic import BaseModel

from src.langlib.langlib import create_agent_pg_ltm_stm, Config


def pii_types():
    return ['email', 'credit_card', 'ip', 'mac_address', 'url']

class CommandResult(BaseModel):
    config: dict
    request_id: str
    result: Dict[str, Union[List[Any], str]]

def run_command(agent, user_id, namespace, query, threshold: float = None, config=None, request_id=None):
    if threshold is None:
        threshold = .9

    if request_id is None:
        request_id = uuid.uuid4().hex

    if config is None:
        config = {"configurable": {"thread_id": request_id}}

    output = agent.invoke(
        user_id,
        request_id,
        namespace=namespace,
        query=query,
        config=config,
        threshold=threshold)
    return CommandResult(
        config=config,
        request_id=request_id,
        result=output
    )

async def arun_command(agent, user_id, namespace, query, threshold: float = None, config=None, request_id=None):
    if threshold is None:
        threshold = .9

    if request_id is None:
        request_id = uuid.uuid4().hex

    if config is None:
        config = {"configurable": {"thread_id": request_id}}

    output = await agent.invoke(
        user_id,
        request_id,
        namespace=namespace,
        query=query,
        config=config,
        threshold=threshold)
    return CommandResult(
        config=config,
        request_id=request_id,
        result=output
    )


def run_commands(agent, user_id, namespace, commands, threshold: float = None, config=None, request_id=None) -> \
Generator[CommandResult, Any, None]:
    if request_id is None:
        request_id = uuid.uuid4().hex

    if config is None:
        config = {"configurable": {"thread_id": request_id}}

    for cmd in commands:
        try:
            yield run_command(agent, user_id, namespace, cmd, threshold, config, request_id)
        except ValueError as e:
            raise e

async def arun_commands(agent, user_id, namespace, commands, threshold: float = None, config=None, request_id=None) -> AsyncGenerator[
    CommandResult, Any]:
    if request_id is None:
        request_id = uuid.uuid4().hex

    if config is None:
        config = {"configurable": {"thread_id": request_id}}

    for cmd in commands:
        try:
            yield await arun_command(agent, user_id, namespace, cmd, threshold, config, request_id)
        except ValueError as e:
            raise e

async def base_agent(model, emb, pool = None, schema = None, store = None, tools = None, fs = None, system_prompt: List[str] = None):
    return await create_agent_pg_ltm_stm(
        model=model,
        state_schema=schema,
        store=store,
        fs=fs,
        pool=pool,
        tools=tools,
        config=Config(
            dimensions=1024,
            embedding=emb,
            fields=["text"],
            index_kwargs={
                "ann_index_config": {"m": 16, "ef_construction": 100},
                "text_fields": ["text"] # Fallback for fields property
            },
            summary_kwargs={
                "trigger": [("tokens", 2000), ("messages", 20)],
                "retention": ("messages", 5),
                "summary_prompt": "Summarize the conversation. ",
                "max_summary_tokens": 128,
                "token_counter": count_tokens_approximately,
            },
            create_agent_kwargs={
                "system_prompt": f"{'\n'.join(system_prompt)}",
            },
            pii_types=pii_types(),
            pii_strategy="hash"
        )
    )