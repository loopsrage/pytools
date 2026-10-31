"""Secret management for agent credentials.

Wraps :class:`src.secretslib.secretslib.Secrets` to provide secure injection
of API keys, tokens, and other credentials from environment variables or
files (e.g. Kubernetes/Docker secrets).

Secrets are cached after first resolution and can be cleared per-name or
entirely.
"""

from __future__ import annotations

from typing import Optional

_REPO_SRC = str(__import__("pathlib").Path(__file__).resolve().parents[4] / "src")
import sys

if _REPO_SRC not in sys.path:
    sys.path.insert(0, _REPO_SRC)

from secretslib.secretslib import Secrets  # noqa: E402


class AgentSecrets:
    """Secret resolver backed by env vars and files.

    Resolution priority:
      1. In-memory cache (after first load)
      2. Environment variable ``NAME``
      3. File pointed to by ``NAME_FILE`` environment variable
    """

    def __init__(self):
        self._secrets = Secrets()

    def get(self, name: str, default: Optional[str] = None) -> Optional[str]:
        """Resolve a secret by name."""
        return self._secrets.get(name, default=default)

    def required(self, name: str) -> str:
        """Resolve a required secret, raising if missing."""
        return self._secrets.required(name)

    def clear(self, name: Optional[str] = None) -> None:
        """Clear a specific secret from cache, or all secrets."""
        self._secrets.clear(name)

    def load_env_file(self, path: str) -> int:
        """Load KEY=VALUE lines from a .env file (does not overwrite existing env)."""
        return self._secrets.load_env_file(path)


__all__ = ["AgentSecrets", "Secrets"]
