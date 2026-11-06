from typing import Optional, Any, List, Dict

try:
    import boto3
except ImportError:
    boto3 = Any


class AwsAiConfig:
    def __init__(self, region_name: str | None = None, aws_access_key_id: str | None = None, aws_secret_access_key: str | None = None, endpoint_url: str | None = None):
        self.region_name = region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.endpoint_url = endpoint_url


class AwsAi:
    def __init__(self, config: AwsAiConfig):
        if boto3 is Any:
            raise ImportError("boto3 is not installed")
        self.config = config
        self._runtime = boto3.client(
            "bedrock-runtime",
            region_name=config.region_name,
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
            endpoint_url=config.endpoint_url,
        )

    def invoke_model(self, model_id: str, prompt: str, max_tokens: int = 512, temperature: float = 0.7, **kwargs) -> Optional[str]:
        if boto3 is Any:
            raise ImportError("boto3 is not installed")
        body = {
            "prompt": prompt,
            "max_tokens_to_sample": max_tokens,
            "temperature": temperature,
        }
        body.update(kwargs)
        import json
        try:
            resp = self._runtime.invoke_model(
                modelId=model_id,
                body=json.dumps(body),
                accept="application/json",
                contentType="application/json",
            )
            return resp.get("body").read().decode()
        except Exception:
            return None

    def list_models(self) -> List[Dict]:
        try:
            resp = self._runtime.list_foundation_models()
            return resp.get("modelSummaries", [])
        except Exception:
            return []
