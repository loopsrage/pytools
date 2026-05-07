import os
import unittest

from dotenv import load_dotenv

from openailib.openailib import AzureSearchClientConfig, AzureAIClient, AzureOpenAIConfig
from settings.helper import restore, unmarshal_app_settings

load_dotenv()
restore(os.getenv("ENV_FILE"))

class MyTestCase(unittest.TestCase):
    def test_something(self):
        config = unmarshal_app_settings("Azure", AzureSearchClientConfig)
        client = AzureAIClient(AzureOpenAIConfig(), config)


if __name__ == '__main__':
    unittest.main()
