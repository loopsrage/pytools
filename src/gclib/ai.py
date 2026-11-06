from typing import Optional, Any, List, Dict

try:
    from google.cloud import aiplatform
except ImportError:
    aiplatform = Any


class GcpAiConfig:
    def __init__(self, project_id: str | None = None, location: str | None = None):
        self.project_id = project_id
        self.location = location


class GcpAi:
    def __init__(self, config: GcpAiConfig):
        if aiplatform is Any:
            raise ImportError("google-cloud-aiplatform is not installed")
        self.config = config
        aiplatform.init(project=config.project_id, location=config.location)

    def predict(self, endpoint_id: str, instances: List[Dict]) -> Optional[List[Dict]]:
        try:
            endpoint = aiplatform.Endpoint(endpoint_id)
            resp = endpoint.predict(instances=instances)
            return resp.predictions
        except Exception:
            return None

    def list_endpoints(self) -> List[Dict]:
        try:
            endpoints = aiplatform.Endpoint.list()
            return [{"name": e.resource_name, "display_name": e.display_name} for e in endpoints]
        except Exception:
            return []
