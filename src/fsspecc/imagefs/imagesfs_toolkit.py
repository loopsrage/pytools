from typing import List

from langchain_core.tools import BaseToolkit, tool
from langgraph.prebuilt import ToolRuntime
from pydantic import ConfigDict, BaseModel
import matplotlib.pyplot as plt

from src.fsspecc.base_fsspecfs.validate_input_node import validate_request_id
from src.fsspecc.imagefs.imagesfs import ImagesFs

class SavePNGInput(BaseModel):
    file_name: str

class ImagesFsToolkit(BaseToolkit):

    fs: ImagesFs

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def get_tools(self) -> List:

        @tool(response_format="content_and_artifact")
        def list_images(runtime: ToolRuntime):
            """Retrieve a list of png files."""
            fn = self.fs.list_images
            request_id =  validate_request_id(fn.__name__, runtime.state)
            try:
                return list(fn(request_id)), True
            except Exception:
                raise

        @tool(args_schema=SavePNGInput, response_format="content_and_artifact")
        def save_png_file(file_name: str, runtime: ToolRuntime):
            """
            Saves a Matplotlib/Seaborn figure object to the filesystem.
            Input should be the variable name of the figure object (e.g., from plt.gcf()).
            """

            # Grab the active figure directly from the environment
            figure = plt.gcf()

            fn = self.fs.save_png_file
            request_id =  validate_request_id(fn.__name__, runtime.state)
            try:
                return fn(request_id, file_name, figure, True), True
            except Exception:
                raise

        return [list_images, save_png_file]