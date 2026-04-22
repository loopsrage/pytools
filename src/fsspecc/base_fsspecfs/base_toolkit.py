import io
from typing import Any

from langchain_core.tools import BaseTool, tool, BaseToolkit
from langgraph.prebuilt import ToolRuntime
from pydantic import ConfigDict

from fsspecc.base_fsspecfs.base_fsspecfs import FSBase, get_file_path
from fsspecc.base_fsspecfs.validate_input_node import validate_request_id


class FSToolkit(BaseToolkit):

    fs: FSBase

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def get_tools(self) -> list[BaseTool]:

        @tool(response_format="content_and_artifact")
        def get_file(file_path: str, runtime: ToolRuntime) -> tuple[bytes, bool]:
            """Read file_path provided by user"""
            buffer = io.BytesIO()
            try:
                request_id = validate_request_id("get_file", runtime.state)
                path = get_file_path(request_id, file_path)
                self.fs.read(path, buffer, True)
                return buffer.getvalue(), True
            except Exception:
                raise

        @tool(response_format="content_and_artifact")
        def save_file(file_path: str, file_data: str, runtime: ToolRuntime) -> tuple[None, bool]:
            """Save file_path with content provided from file_data"""
            try:
                request_id = validate_request_id("save_file", runtime.state)
                path = get_file_path(request_id, file_path)
                return self.fs.write(path, io.BytesIO(f"{file_data}".encode('utf-8')), True), True
            except Exception:
                raise

        @tool(response_format="content_and_artifact")
        def list_files(pattern: str, runtime: ToolRuntime) -> tuple[list[Any], None]:
            """Retrieve a list of files that match a pattern."""
            try:
                request_id = validate_request_id("list_files", runtime.state)
                path = get_file_path(request_id, pattern)
                return list(self.fs.list(path)), None
            except Exception:
                raise

        @tool(response_format="content_and_artifact")
        def walk_dir(directory: str, runtime: ToolRuntime) -> tuple[list[Any], None]:
            """Iterates over a directory request ID."""
            try:
                request_id = validate_request_id("walk_dir", runtime.state)
                path = get_file_path(request_id, directory)
                return list(self.fs.walk(path)), None
            except Exception:
                raise

        return [get_file, save_file, list_files, walk_dir]