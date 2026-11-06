from typing import Optional, Any

try:
    from google.cloud import secretmanager
except ImportError:
    secretmanager = Any


class GcpSecretsConfig:
    def __init__(self, project_id: str | None = None):
        self.project_id = project_id


class GcpSecrets:
    def __init__(self, config: GcpSecretsConfig):
        if secretmanager is Any:
            raise ImportError("google-cloud-secret-manager is not installed")
        self.config = config
        self._client = secretmanager.SecretManagerServiceClient()

    def _secret_path(self, secret_id: str) -> str:
        return self._client.secret_path(self.config.project_id, secret_id)

    def get_secret(self, secret_id: str) -> Optional[str]:
        try:
            resp = self._client.access_secret_version(request={"name": f"{self._secret_path(secret_id)}/versions/latest"})
            return resp.payload.data.decode("UTF-8")
        except Exception:
            return None

    def put_secret(self, secret_id: str, secret_string: str) -> dict:
        parent = f"projects/{self.config.project_id}"
        resp = self._client.create_secret(
            request={
                "parent": parent,
                "secret_id": secret_id,
                "secret": {"replication": {"automatic": {}}},
            }
        )
        self._client.add_secret_version(
            request={
                "parent": resp.name,
                "payload": {"data": secret_string.encode("UTF-8")},
            }
        )
        return {"name": resp.name}

    def list_secrets(self, max_results: int = 100) -> list:
        parent = f"projects/{self.config.project_id}"
        resp = self._client.list_secrets(request={"parent": parent, "page_size": max_results})
        return list(resp)
