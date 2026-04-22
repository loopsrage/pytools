import io
from typing import List, Annotated, Any

from langchain_core.tools import BaseToolkit, tool
from langgraph.prebuilt import InjectedState, ToolRuntime
from pydantic import ConfigDict

from fsspecc.base_fsspecfs.validate_input_node import validate_request_id
from fsspecc.memfs.memfs import MemFS


def _validate_key(tool_name: str, state: Annotated[dict, InjectedState]):
    key = state.get("key")
    if not key:
        raise KeyError(
            f"Execution halted: Tool '{tool_name}' requires 'key' in state. "
        )
    return key


def _validate_value(tool_name: str, state: Annotated[dict, InjectedState]):
    value = state.get("value")
    if not value:
        raise KeyError(
            f"Execution halted: Tool '{tool_name}' requires 'value' in state. "
        )
    return value

class MemFSToolkit(BaseToolkit):

    fs: MemFS

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def get_tools(self) -> List:
        @tool(response_format="content_and_artifact")
        def load(key: str, runtime: ToolRuntime):
            """Retrieve and read value for a specific key for a specific request ID."""
            fn = self.fs.load
            tool_name = fn.__name__
            state = runtime.state
            request_id = validate_request_id(tool_name, state)
            value = io.BytesIO()
            try:
                fn(request_id, key, value)
                return value.getvalue(), None
            except Exception:
                raise

        @tool(response_format="content_and_artifact")
        def store(key: str, value: Any, runtime: ToolRuntime):
            """Store a value for a specific key."""
            fn = self.fs.store
            state = runtime.state
            tool_name = fn.__name__
            request_id = validate_request_id(tool_name, state)
            try:
                return fn(request_id, key, value), True
            except Exception:
                raise

        return [load, store]

