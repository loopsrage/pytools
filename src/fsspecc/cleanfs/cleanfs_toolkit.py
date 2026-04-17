
import io
from typing import List, Annotated, Any

import pandas as pd
from langchain_core.tools import BaseToolkit, tool
from langgraph.prebuilt import InjectedState, ToolRuntime
from pydantic import ConfigDict

from src.fsspecc.base_fsspecfs.validate_input_node import validate_request_id
from src.fsspecc.cleanfs.cleanfs import CleanFs


def _validate_csv_data(tool_name: str, csv_data, state: Annotated[dict, InjectedState]):
    request_id = validate_request_id(tool_name, state)
    if not csv_data:
        raise AttributeError(
            f"Execution halted: Tool '{tool_name}' requires 'csv_data' in state. "
            f"Request ID: {request_id}"
        )
    return request_id, csv_data

def _retrieve(get_file, tool_name, state) -> tuple[Any, dict[str, str | int]] | None:
    try:
        request_id = validate_request_id(tool_name, state)
        df = get_file(request_id)

        csv_string = df.to_csv(index=False, encoding='utf-8')
        artifact = {
            "data": str(csv_string),
            "row_count": len(df),
            "type": "binary"
        }
        return csv_string, artifact
    except AttributeError:
        pass
    except Exception:
        raise

class CleanFSToolkit(BaseToolkit):

    fs: CleanFs

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def get_tools(self) -> List:

        # We wrap the methods in functions to use the @tool decorator
        @tool(response_format="content_and_artifact")
        def get_clean_file(runtime: ToolRuntime) -> tuple[Any, dict[str, str | int]] | None:
            """Retrieve and read the cleaned CSV file for a specific request ID."""
            fn = self.fs.get_clean_file
            try:
                return _retrieve(fn, fn.__name__, runtime.state)
            except Exception:
                raise

        @tool(response_format="content_and_artifact")
        def get_raw_file(runtime: ToolRuntime) -> tuple[Any, dict[str, str | int]] | None:
            """Retrieve and read the raw CSV file for a specific request ID."""
            fn = self.fs.get_raw_file
            try:
                return _retrieve(fn, fn.__name__, runtime.state)
            except Exception:
                raise

        @tool(response_format="content_and_artifact")
        def save_clean_file(csv_data, runtime: ToolRuntime):
            """Save cleaned data to the filesystem. Input must be a CSV-formatted string."""
            fn = self.fs.save_clean_file
            try:
                request_id, csv_data = _validate_csv_data(fn.__name__, csv_data, runtime.state)
                df = pd.read_csv(io.StringIO(csv_data))
                fn(request_id, df, use_pipe=True)
                resp = f"Successfully saved clean file for {request_id}"
                return resp, True
            except Exception:
                raise

        @tool(response_format="content_and_artifact")
        def save_raw_file(csv_data, runtime: ToolRuntime):
            """Saves files specifically requiring CSV formatting data."""
            fn = self.fs.save_raw_file
            try:
                request_id, csv_data = _validate_csv_data(fn.__name__, csv_data, runtime.state)
                df = pd.read_csv(io.StringIO(csv_data))
                fn(request_id, df, use_pipe=True)
                resp = f"Successfully saved raw file for {request_id}"
                return resp, True
            except Exception:
                raise

        @tool(response_format="content_and_artifact")
        def list_raw_files(runtime: ToolRuntime) -> None | tuple[str, list[Any]] | tuple[Any, bool]:
            """List all raw files available for a given request ID."""
            fn = self.fs.list_raw_files
            try:
                opres = list(fn(validate_request_id(fn.__name__, runtime.state)))
                return str(opres), opres
            except Exception:
                raise

        @tool(response_format="content_and_artifact")
        def list_clean_files(runtime: ToolRuntime) -> tuple[str, list[Any]] | tuple[Exception, bool]:
            """List all clean files available for a given request ID."""
            fn = self.fs.list_clean_files
            try:
                opres = list(fn(validate_request_id(fn.__name__, runtime.state)))
                return str(opres), opres
            except Exception:
                raise

        return [
            get_clean_file, get_raw_file, save_clean_file,
            save_raw_file, list_raw_files, list_clean_files
        ]