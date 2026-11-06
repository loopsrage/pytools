import unittest
from unittest import mock

from gclib.secrets import GcpSecrets, GcpSecretsConfig


class TestGcpSecretsConfig(unittest.TestCase):

    def test_fields(self):
        cfg = GcpSecretsConfig(project_id="my-project")
        assert cfg.project_id == "my-project"


class TestGcpSecretsInit(unittest.TestCase):

    def test_init_raises_without_sdk(self):
        cfg = GcpSecretsConfig(project_id="my-project")
        with self.assertRaises(ImportError):
            GcpSecrets(cfg)


class TestGcpSecrets(unittest.TestCase):

    def _svc(self):
        cfg = GcpSecretsConfig(project_id="my-project")
        svc = GcpSecrets.__new__(GcpSecrets)
        svc._client = mock.MagicMock()
        svc.config = cfg
        return svc

    def test_get_secret(self):
        svc = self._svc()
        svc._client.secret_path.return_value = "projects/my-project/secrets/my-secret"
        payload = mock.MagicMock()
        payload.data = b"top-secret"
        version = mock.MagicMock()
        version.payload = payload
        svc._client.access_secret_version.return_value = version
        assert svc.get_secret("my-secret") == "top-secret"
        svc._client.access_secret_version.assert_called_once_with(
            request={"name": "projects/my-project/secrets/my-secret/versions/latest"}
        )

    def test_get_secret_not_found(self):
        svc = self._svc()
        svc._client.access_secret_version.side_effect = Exception("not found")
        assert svc.get_secret("missing") is None

    def test_put_secret(self):
        svc = self._svc()
        created = mock.MagicMock()
        created.name = "projects/my-project/secrets/my-secret"
        svc._client.create_secret.return_value = created
        result = svc.put_secret("my-secret", "value")
        assert result == {"name": "projects/my-project/secrets/my-secret"}
        svc._client.create_secret.assert_called_once()
        svc._client.add_secret_version.assert_called_once()

    def test_list_secrets(self):
        svc = self._svc()
        item = mock.MagicMock()
        svc._client.list_secrets.return_value = [item]
        result = svc.list_secrets(max_results=10)
        assert result == [item]


if __name__ == "__main__":
    unittest.main()
