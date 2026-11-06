import unittest

from openailib.openailib import (
    AzureAIClient,
    AzureOpenAIConfig,
    AzureSearchClientConfig,
    InjectConfig,
    Messages,
    clean_context,
)


class TestOpenAILib(unittest.TestCase):

    def test_clean_context_and_messages(self):
        snippets = [
            {"snippet": "<table><tr><th>Heading/ Subheading</th><th>Desc</th></tr><tr><td>8471.30</td><td>Laptop</td></tr></table>"},
            {"snippet": "<table><tr><td>8471.30</td><td>Laptop</td></tr><tr><td>8471.41</td><td></td></tr></table>"},
        ]
        cleaned = clean_context(snippets)
        assert cleaned == [
            "| 8471.30 | Laptop |",
            "| 8471.41 | - |",
        ]

        msgs = Messages()
        pos0 = msgs.add("u1", "hello")
        pos1 = msgs.add("u1", "world")
        assert pos0 == 0 and pos1 == 1

    def test_client_configs_and_initialization(self):
        oai_cfg = AzureOpenAIConfig(
            azure_deployment="gpt-4o",
            embedding_deployment="text-embedding-3-large",
            index_name="idx",
            api_key="test_key",
            azure_endpoint="https://example.openai.azure.com/",
            api_version="2024-10-21",
        )
        search_cfg = AzureSearchClientConfig(
            endpoint="https://example.search.windows.net",
            index_name="idx",
            key="search_key",
        )
        client = AzureAIClient(oai_cfg, search_cfg)
        assert client.client is not None
        assert client.search_client is not None
        assert client.chat is not None
        assert client.files is not None

        inj = InjectConfig(query="laptops", vector_query="portable computer")
        assert inj.top == 100 and inj.query == "laptops"


if __name__ == "__main__":
    unittest.main()
