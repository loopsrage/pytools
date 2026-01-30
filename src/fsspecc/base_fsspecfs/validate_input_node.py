from typing import Annotated

from langgraph.prebuilt import InjectedState

def validate_request_id(tool_name: str, state: Annotated[dict, InjectedState]):
    request_id = state.get("request_id")
    if not request_id:
        raise KeyError(
            f"Execution halted: Tool '{tool_name}' requires 'request_id' in state. "
        )
    return request_id
