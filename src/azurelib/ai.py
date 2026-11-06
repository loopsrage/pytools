from typing import Optional, Any, List, Dict

try:
    from openai import AsyncAzureOpenAI
except ImportError:
    AsyncAzureOpenAI = Any


class AzureAiConfig:
    def __init__(self, azure_endpoint: str | None = None, api_key: str | None = None,
                 api_version: str | None = None, azure_deployment: str | None = None):
        self.azure_endpoint = azure_endpoint
        self.api_key = api_key
        self.api_version = api_version
        self.azure_deployment = azure_deployment


class AzureAi:
    def __init__(self, config: AzureAiConfig):
        if AsyncAzureOpenAI is Any:
            raise ImportError("openai is not installed")
        self.config = config
        self._client = AsyncAzureOpenAI(
            azure_endpoint=config.azure_endpoint,
            api_key=config.api_key,
            api_version=config.api_version,
        )

    def chat(self, messages: List[Dict], model: str | None = None, temperature: float = 0.7, max_tokens: int = 512) -> Optional[str]:
        try:
            import json as _json
            resp = self._client.chat.completions.create(
                model=model or self.config.azure_deployment,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
            )
            return resp.choices[0].message.content
        except Exception:
            return None

    def embed(self, input_text: str, model: str | None = None) -> Optional[List[float]]:
        try:
            resp = self._client.embeddings.create(
                model=model or self.config.azure_deployment,
                input=input_text,
            )
            return resp.data[0].embedding
        except Exception:
            return None
