import os
from pathlib import Path
from typing import Optional


class Secrets:
    """Load secrets from environment variables or files, with optional versioning.

    Priority: explicit value > env var > file path.
    """

    def __init__(self):
        self._cache: dict[str, str] = {}

    def get(self, name: str, default: Optional[str] = None) -> Optional[str]:
        """Resolve a secret by name.

        Looks up NAME, NAME_FILE, then NAME_ENV environment variables.
        """
        if name in self._cache:
            return self._cache[name]

        # Direct env var
        value = os.getenv(name)
        if value is not None:
            self._cache[name] = value
            return value

        # File-based secret (common in Kubernetes / Docker secrets)
        file_var = f"{name}_FILE"
        file_path = os.getenv(file_var)
        if file_path:
            try:
                value = Path(file_path).read_text().strip()
                self._cache[name] = value
                return value
            except (FileNotFoundError, OSError):
                pass

        return default

    def required(self, name: str) -> str:
        value = self.get(name)
        if value is None:
            raise EnvironmentError(f"Required secret '{name}' not found")
        return value

    def clear(self, name: Optional[str] = None) -> None:
        if name:
            self._cache.pop(name, None)
        else:
            self._cache.clear()

    def load_env_file(self, path: str | Path) -> int:
        """Load KEY=VALUE lines from a .env file into os.environ (does not overwrite)."""
        loaded = 0
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = value
                    loaded += 1
        return loaded


secrets = Secrets()


def get(name: str, default: Optional[str] = None) -> Optional[str]:
    return secrets.get(name, default)


def required(name: str) -> str:
    return secrets.required(name)


def clear(name: Optional[str] = None) -> None:
    secrets.clear(name)


def load_env_file(path: str | Path) -> int:
    return secrets.load_env_file(path)
