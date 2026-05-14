import os
import unittest

from dotenv import load_dotenv
from postgreslib.engine import named_session
from settings.helper import setting, restore

from tweak_similarity.lib.models.file import select_work, UploadedFiles

load_dotenv()
restore(os.getenv("ENV_FILE"))

class MyTestCase(unittest.TestCase):
    def test_something(self):
        with named_session("TWE_PG") as session:
            results = select_work(
                session=session,
                model=UploadedFiles,
                stage="initial",
                limit=setting("TWE.Initial", "limit"),
                retries=setting("TWE.Initial", "retries"),
            )
        print(f"found {len(results)} results")


if __name__ == '__main__':
    unittest.main()
