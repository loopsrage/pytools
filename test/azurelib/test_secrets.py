import unittest
from unittest import mock

from azurelib.secrets import AzureSecrets, AzureSecretsConfig


class TestAzureSecretsConfig(unittest.TestCase):

    def test_fields(self):
        cfg = AzureSecretsConfig(vault_url="https://myvault.vault.azure.net")
        assert cfg.vault_url == "https://myvault.vault.azure.net"
        assert cfg.credential is None


class TestAzureSecretsInit(unittest.TestCase):

    def test_init_raises_without_sdk(self):
        cfg = AzureSecretsConfig(vault_url="https://myvault.vault.azure.net")
        with self.assertRaises(ImportError):
            AzureSecrets(cfg)


class TestAzureSecrets(unittest.TestCase):

    def _svc(self):
        cfg = AzureSecretsConfig(vault_url="https://myvault.vault.azure.net")
        svc = AzureSecrets.__new__(AzureSecrets)
        svc._client = mock.MagicMock()
        return svc

    def test_get_secret(self):
        svc = self._svc()
        svc._client.get_secret.return_value.value = "top-secret"
        assert svc.get_secret("my-secret") == "top-secret"
        svc._client.get_secret.assert_called_once_with("my-secret")

    def test_get_secret_not_found(self):
        svc = self._svc()
        svc._client.get_secret.side_effect = Exception("not found")
        assert svc.get_secret("missing") is None

    def test_put_secret(self):
        svc = self._svc()
        resp = mock.MagicMock()
        resp.name = "my-secret"
        resp.properties.version = "v1"
        svc._client.set_secret.return_value = resp
        result = svc.put_secret("my-secret", "value")
        assert result == {"name": "my-secret", "version": "v1"}
        svc._client.set_secret.assert_called_once_with("my-secret", "value")

    def test_list_secrets(self):
        svc = self._svc()
        item = mock.MagicMock()
        svc._client.list_properties_of_secrets.return_value = iter([item])
        result = list(svc.list_secrets(max_results=10))
        assert result == [item]
        svc._client.list_properties_of_secrets.assert_called_once_with(max_results=10)


if __name__ == "__main__":
    unittest.main()
