from typing import List

from langchain.agents.middleware import PIIMiddleware, HumanInTheLoopMiddleware

def pii_middleware(types: List[str], strategy):
    return [PIIMiddleware(t, strategy=strategy, apply_to_input=True, apply_to_output=True) for t in types]

def hil_middleware(tool_calls):
    ia = {}
    for f in tool_calls:
        ia[f.name] = False
    return HumanInTheLoopMiddleware(interrupt_on={**ia})


