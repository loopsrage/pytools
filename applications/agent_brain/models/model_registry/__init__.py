import json
import os
from pathlib import Path
from typing import Optional

_MODELS_DIR = None


def set_models_dir(path: str | Path) -> None:
    global _MODELS_DIR
    _MODELS_DIR = Path(path)


def models_dir() -> Path:
    if _MODELS_DIR is None:
        return Path(__file__).resolve().parent / "store"
    return _MODELS_DIR


class ModelInfo:
    def __init__(self, model_id: str, config: dict, model_path: Path, config_path: Path):
        self.model_id = model_id
        self.config = config
        self.model_path = model_path
        self.config_path = config_path
        self.name = config.get("name", model_id)
        self.framework = config.get("framework", "pytorch")
        self.format = config.get("format", "pt")
        self.architecture = config.get("architecture", "unknown")
        self.input_dim = config.get("input_dim")
        self.output_dim = config.get("output_dim")
        self.description = config.get("description", "")

    def to_dict(self) -> dict:
        return {
            "model_id": self.model_id,
            "name": self.name,
            "framework": self.framework,
            "format": self.format,
            "architecture": self.architecture,
            "input_dim": self.input_dim,
            "output_dim": self.output_dim,
            "description": self.description,
            "model_path": str(self.model_path),
            "config_path": str(self.config_path),
        }


class ModelRegistry:
    """Scans the models folder and registers available models."""

    def __init__(self, models_dir_path: str | Path | None = None):
        self._dir = Path(models_dir_path) if models_dir_path else models_dir()
        self._models: dict[str, ModelInfo] = {}
        self._scan()

    def _scan(self) -> None:
        if not self._dir.exists():
            return
        for entry in sorted(self._dir.iterdir()):
            if not entry.is_dir():
                continue
            config_path = entry / "config.json"
            if not config_path.exists():
                continue
            try:
                config = json.loads(config_path.read_text())
            except Exception:
                continue
            # Find weight file
            weight_file = None
            for fname in ("model.safetensors", "model.pt", "pytorch_model.bin"):
                cand = entry / fname
                if cand.exists():
                    weight_file = cand
                    break
            if weight_file is None:
                continue
            model_id = entry.name
            info = ModelInfo(model_id, config, weight_file, config_path)
            self._models[model_id] = info

    def list(self) -> list[ModelInfo]:
        return sorted(self._models.values(), key=lambda m: m.model_id)

    def get(self, model_id: str) -> Optional[ModelInfo]:
        return self._models.get(model_id)

    def has(self, model_id: str) -> bool:
        return model_id in self._models

    def count(self) -> int:
        return len(self._models)


# Ensure bare `model_registry` and qualified `applications.agent_brain.model_registry`
# refer to the same module object regardless of which import path is used. This keeps
# the `_MODELS_DIR` global (set via set_models_dir) consistent across both.
import sys as _sys  # noqa: E402

_qual = "applications.agent_brain.model_registry"
_bare = "model_registry"
if _qual in _sys.modules and _bare not in _sys.modules:
    _sys.modules[_bare] = _sys.modules[_qual]
elif _bare in _sys.modules and _qual not in _sys.modules:
    _sys.modules[_qual] = _sys.modules[_bare]
