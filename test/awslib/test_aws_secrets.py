import json
import unittest
from unittest import mock

from awslib.secrets import AwsSecrets, AwsSecretsConfig


class TestAwsSecretsConfig(unittest.TestCase):

    def test_fields(self):
        cfg = AwsSecretsConfig(
            region_name="us-east-1",
            aws_access_key_id="key",
            aws_secret_access_key="secret",
        )
        assert cfg.region_name == "us-east-1"
        assert cfg.aws_access_key_id == "key"
        assert cfg.aws_secret_access_key == "secret"


class TestAwsSecretsInit(unittest.TestCase):

    def test_init_raises_without_sdk(self):
        cfg = AwsSecretsConfig(region_name="us-east-1")
        with self.assertRaises(ImportError):
            AwsSecrets(cfg)


class TestAwsSecrets(unittest.TestCase):

    def _svc(self):
        cfg = AwsSecretsConfig(region_name="us-east-1")
        svc = AwsSecrets.__new__(AwsSecrets)
        svc._client = mock.MagicMock()
        return svc

    def test_get_secret(self):
        svc = self._svc()
        svc._client.get_secret_value.return_value = {"SecretString": "top-secret"}
        assert svc.get_secret("my-secret") == "top-secret"
        svc._client.get_secret_value.assert_called_once_with(SecretId="my-secret")

    def test_get_secret_not_found(self):
        svc = self._svc()
        svc._client.get_secret_value.side_effect = Exception("boom")
        assert svc.get_secret("missing") is None

    def test_put_secret(self):
        svc = self._svc()
        svc._client.put_secret_value.return_value = {"ARN": "arn:aws:secretsmanager:us-east-1:123:secret:my-secret"}
        result = svc.put_secret("my-secret", "value")
        assert result == {"ARN": "arn:aws:secretsmanager:us-east-1:123:secret:my-secret"}
        svc._client.put_secret_value.assert_called_once_with(SecretId="my-secret", SecretString="value")

    def test_list_secrets(self):
        svc = self._svc()
        item = mock.MagicMock()
        svc._client.list_secrets.return_value = {"SecretList": [item]}
        result = svc.list_secrets(max_results=10)
        assert result == [item]
        svc._client.list_secrets.assert_called_once_with(MaxResults=10)


if __name__ == "__main__":
    unittest.main()
