import io
import unittest

import pandas as pd
from PIL import Image

from fsspecc.imagefs.imagesfs import ImagesFs
from fsspecc.imagefs.imagesfs_tool_agent import imagesfs_agent
from langchain_agent_ltm_stm.agent import arun_commands


class Test(unittest.IsolatedAsyncioTestCase):

    async def test_imagesfs_agent(self):
        fs = ImagesFs()
        df = pd.DataFrame({"Values": [1, 2, 3]}, index=["Point A", "Point B", "Point C"])
        try:
            agent = await imagesfs_agent()
            for i in range(0, 1):
                csv_data = df.to_csv()
                commands = [
                    f"Using the following CSV data:\n{csv_data}\n"
                    "1. Create a pandas line plot with markers, a grid, and 'Growth Trend' title.\n"
                    "2. Save the resulting figure as 'silly.png' using the save_png_file tool.",
                    "list all current pngs"
                ]
                async for r in arun_commands(agent, "user", "imagesfs", commands):
                    print(r)
                    png_bytes = fs.get_png_bytes(r["request_id"], "silly.png")
                    img = Image.open(io.BytesIO(png_bytes))
                    img.show()
                    img.close()

        except Exception as e:
            raise e
