import unittest
from unittest import mock

from gclib.ai import GcpAi, GcpAiConfig


class TestGcpAiConfig(unittest.TestCase):

    def test_fields(self):
        cfg = GcpAiConfig(project_id="my-project", location="us-central1")
        assert cfg.project_id == "my-project"
        assert cfg.location == "us-central1"


class TestGcpAiInit(unittest.TestCase):

    def test_init_raises_without_sdk(self):
        cfg = GcpAiConfig(project_id="my-project", location="us-central1")
        with self.assertRaises(ImportError):
            GcpAi(cfg)


class TestGcpAi(unittest.TestCase):

    def _svc(self):
        cfg = GcpAiConfig(project_id="my-project", location="us-central1")
        svc = GcpAi.__new__(GcpAi)
        return svc

    def test_predict(self):
        svc = self._svc()
        ep = mock.MagicMock()
        ep.predict.return_value.predictions = [{"score": 0.9}]
        with mock.patch("gclib.ai.aiplatform") as mapl:
            mapl.Endpoint.return_value = ep
            result = svc.predict("projects/my-project/locations/us-central1/endpoints/123", [{"a": 1}])
            assert result == [{"score": 0.9}]
            ep.predict.assert_called_once_with(instances=[{"a": 1}])

    def test_predict_error(self):
        svc = self._svc()
        ep = mock.MagicMock()
        ep.predict.side_effect = Exception("fail")
        with mock.patch("gclib.ai.aiplatform") as mapl:
            mapl.Endpoint.return_value = ep
            assert svc.predict("ep", [{"a": 1}]) is None

    def test_list_endpoints(self):
        svc = self._svc()
        ep1 = mock.MagicMock()
        ep1.resource_name = "ep1"
        ep1.display_name = "First"
        with mock.patch("gclib.ai.aiplatform") as mapl:
            mapl.Endpoint.list.return_value = [ep1]
            result = svc.list_endpoints()
            assert result == [{"name": "ep1", "display_name": "First"}]

    def test_list_endpoints_error(self):
        svc = self._svc()
        with mock.patch("gclib.ai.aiplatform") as mapl:
            mapl.Endpoint.list.side_effect = Exception("fail")
            assert svc.list_endpoints() == []


if __name__ == "__main__":
    unittest.main()
