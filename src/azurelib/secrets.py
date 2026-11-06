from typing import Optional, Any

try:
    from azure.keyvault.secrets import SecretClient
except ImportError:
    SecretClient = Any

try:
    from azure.identity import DefaultAzureCredential
except ImportError:
    DefaultAzureCredential = Any


class AzureSecretsConfig:
    def __init__(self, vault_url: str | None = None, credential: Any = None):
        self.vault_url = vault_url
        self.credential = credential


class AzureSecrets:
    def __init__(self, config: AzureSecretsConfig):
        if SecretClient is Any:
            raise ImportError("azure-keyvault-secrets is not installed")
        self.config = config
        cred = config.credential or DefaultAzureCredential()
        self._client = SecretClient(vault_url=config.vault_url, credential=cred)

    def get_secret(self, secret_id: str) -> Optional[str]:
        try:
            return self._client.get_secret(secret_id).value
        except Exception:
            return None

    def put_secret(self, secret_id: str, secret_string: str) -> dict:
        resp = self._client.set_secret(secret_id, secret_string)
        return {"name": resp.name, "version": resp.properties.version}

    def list_secrets(self, max_results: int = 100) -> list:
        return list(self._client.list_properties_of_secrets(max_results=max_results))
