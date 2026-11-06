from typing import Optional, Any

try:
    import boto3
except ImportError:
    boto3 = Any


class AwsSecretsConfig:
    def __init__(self, region_name: str | None = None, aws_access_key_id: str | None = None, aws_secret_access_key: str | None = None, endpoint_url: str | None = None):
        self.region_name = region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.endpoint_url = endpoint_url


class AwsSecrets:
    def __init__(self, config: AwsSecretsConfig):
        if boto3 is Any:
            raise ImportError("boto3 is not installed")
        self.config = config
        self._client = boto3.client(
            "secretsmanager",
            region_name=config.region_name,
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
            endpoint_url=config.endpoint_url,
        )

    def get_secret(self, secret_id: str) -> Optional[str]:
        try:
            resp = self._client.get_secret_value(SecretId=secret_id)
            return resp.get("SecretString")
        except Exception:
            return None

    def put_secret(self, secret_id: str, secret_string: str) -> dict:
        return self._client.put_secret_value(SecretId=secret_id, SecretString=secret_string)

    def list_secrets(self, max_results: int = 100) -> list:
        resp = self._client.list_secrets(MaxResults=max_results)
        return resp.get("SecretList", [])
