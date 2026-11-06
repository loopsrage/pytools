import json
import unittest
from unittest import mock

import awslib.ai as aws_ai_mod
from awslib.ai import AwsAi, AwsAiConfig


class TestAwsAiConfig(unittest.TestCase):

    def test_fields(self):
        cfg = AwsAiConfig(region_name="us-east-1", aws_access_key_id="key", aws_secret_access_key="secret")
        assert cfg.region_name == "us-east-1"
        assert cfg.aws_access_key_id == "key"
        assert cfg.aws_secret_access_key == "secret"


class TestAwsAiInit(unittest.TestCase):

    def test_init_raises_without_sdk(self):
        cfg = AwsAiConfig(region_name="us-east-1")
        with self.assertRaises(ImportError):
            AwsAi(cfg)


class TestAwsAi(unittest.TestCase):

    def _svc(self):
        cfg = AwsAiConfig(region_name="us-east-1")
        aws_ai_mod.boto3 = mock.MagicMock()
        svc = AwsAi.__new__(AwsAi)
        svc._runtime = mock.MagicMock()
        return svc

    def test_invoke_model(self):
        svc = self._svc()
        body = mock.MagicMock()
        body.read.return_value = json.dumps({"completion": "hello"}).encode()
        svc._runtime.invoke_model.return_value = {"body": body}
        result = svc.invoke_model("anthropic.claude-v2", "prompt", max_tokens=100, temperature=0.5)
        assert result == json.dumps({"completion": "hello"})
        svc._runtime.invoke_model.assert_called_once()

    def test_invoke_model_error(self):
        svc = self._svc()
        svc._runtime.invoke_model.side_effect = Exception("boom")
        assert svc.invoke_model("anthropic.claude-v2", "prompt") is None

    def test_list_models(self):
        svc = self._svc()
        item = {"modelId": "m1"}
        svc._runtime.list_foundation_models.return_value = {"modelSummaries": [item]}
        result = svc.list_models()
        assert result == [item]

    def test_list_models_error(self):
        svc = self._svc()
        svc._runtime.list_foundation_models.side_effect = Exception("boom")
        assert svc.list_models() == []


if __name__ == "__main__":
    unittest.main()
