import unittest
from unittest import mock

from azurelib.ai import AzureAi, AzureAiConfig


class TestAzureAiConfig(unittest.TestCase):

    def test_fields(self):
        cfg = AzureAiConfig(
            azure_endpoint="https://e.openai.azure.com",
            api_key="key",
            api_version="2024-01-01",
            azure_deployment="gpt-4",
        )
        assert cfg.azure_endpoint == "https://e.openai.azure.com"
        assert cfg.api_key == "key"
        assert cfg.api_version == "2024-01-01"
        assert cfg.azure_deployment == "gpt-4"


class TestAzureAiInit(unittest.TestCase):

    def test_init_raises_without_sdk(self):
        cfg = AzureAiConfig(azure_endpoint="https://e.openai.azure.com", api_key="k")
        with self.assertRaises(ImportError):
            AzureAi(cfg)


class TestAzureAi(unittest.TestCase):

    def _svc(self):
        cfg = AzureAiConfig(azure_endpoint="https://e.openai.azure.com", api_key="k")
        svc = AzureAi.__new__(AzureAi)
        svc._client = mock.MagicMock()
        return svc

    def test_chat(self):
        svc = self._svc()
        choice = mock.MagicMock()
        choice.message.content = "hello world"
        svc._client.chat.completions.create.return_value.choices = [choice]
        result = svc.chat([{"role": "user", "content": "hi"}], model="gpt-4", temperature=0.5, max_tokens=100)
        assert result == "hello world"
        svc._client.chat.completions.create.assert_called_once()

    def test_chat_uses_deployment(self):
        cfg = AzureAiConfig(azure_endpoint="https://e.openai.azure.com", api_key="k", azure_deployment="my-deploy")
        svc = AzureAi.__new__(AzureAi)
        svc._client = mock.MagicMock()
        svc.config = cfg
        choice = mock.MagicMock()
        choice.message.content = "ok"
        svc._client.chat.completions.create.return_value.choices = [choice]
        svc.chat([{"role": "user", "content": "hi"}])
        call_kwargs = svc._client.chat.completions.create.call_args.kwargs
        assert call_kwargs["model"] == "my-deploy"

    def test_chat_error(self):
        svc = self._svc()
        svc._client.chat.completions.create.side_effect = Exception("fail")
        assert svc.chat([{"role": "user", "content": "hi"}]) is None

    def test_embed(self):
        svc = self._svc()
        data = mock.MagicMock()
        data.embedding = [0.1, 0.2, 0.3]
        svc._client.embeddings.create.return_value.data = [data]
        result = svc.embed("hello world", model="embed-1")
        assert result == [0.1, 0.2, 0.3]

    def test_embed_error(self):
        svc = self._svc()
        svc._client.embeddings.create.side_effect = Exception("fail")
        assert svc.embed("hello") is None


if __name__ == "__main__":
    unittest.main()
