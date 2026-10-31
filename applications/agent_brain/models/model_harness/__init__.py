"""Thread-safe, multi-agent model harness.

Multiple agents can concurrently load, swap, and run different models.
Each agent gets its own isolated active-model slot keyed by ``agent_id``.
Model loading is deduplicated via :class:`thread_safe.onceler.Onceler` so the
same (agent_id, model_id) pair is only loaded once, even under contention.

Backed by repo thread-safety primitives:
  * :class:`thread_safe.index.Index`  - concurrent per-agent model storage
  * :class:`thread_safe.onceler.Onceler` - once-only load per agent/model pair
"""

from __future__ import annotations

import json
import sys
import threading
from pathlib import Path
from typing import Any, Optional

import torch
import torch.nn as nn
from safetensors.torch import load_file as safe_load_file

_HERE = str(Path(__file__).resolve().parent)
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from applications.agent_brain.models.model_registry import (
    ModelInfo,
    ModelRegistry,
    models_dir as _models_dir,
)
from thread_safe.index import Index
from thread_safe.onceler import Onceler


# Architecture registry: name -> nn.Module subclass.
# Guarded by a lock so multiple agents can register architectures concurrently.
_ARCHITECTURES: dict[str, type[nn.Module]] = {}
_ARCH_LOCK = threading.RLock()


def register_architecture(name: str, cls: type[nn.Module]) -> None:
    """Register a model architecture class so state_dicts can be rebuilt."""
    with _ARCH_LOCK:
        _ARCHITECTURES[name] = cls


def get_registered_architectures() -> dict[str, type[nn.Module]]:
    """Return a copy of the registered architecture map (thread-safe snapshot)."""
    with _ARCH_LOCK:
        return dict(_ARCHITECTURES)


class ActiveModel:
    """A loaded, hot-swappable model with its metadata."""

    def __init__(self, model_id: str, info: ModelInfo, module: nn.Module):
        self.model_id = model_id
        self.info = info
        self.module = module
        self.module.eval()

    def forward(self, input_tensor: torch.Tensor) -> torch.Tensor:
        with torch.no_grad():
            return self.module(input_tensor)

    def tensor_info(self) -> list[dict]:
        out = []
        for name, tensor in (
            self.module.named_parameters()
            if hasattr(self.module, "named_parameters")
            else []
        ):
            out.append(
                {
                    "name": name,
                    "shape": list(tensor.shape),
                    "dtype": str(tensor.dtype),
                }
            )
        return out

    def to_dict(self) -> dict:
        return self.info.to_dict()


class ThreadSafeModelHarness:
    """Hot-swappable model harness safe for concurrent multi-agent use.

    Each agent is identified by an ``agent_id`` and gets its own isolated
    active-model slot. Loading the same model for the same agent is deduplicated
    via :class:`Onceler`, so concurrent callers block on the first load and then
    share the resulting model.

    Parameters
    ----------
    models_dir:
        Directory containing per-model subfolders (each with ``config.json`` +
        ``model.pt`` or ``model.safetensors``). Defaults to
        ``applications/agent_brain/models``.
    """

    def __init__(self, models_dir: str | Path | None = None):
        self._registry = ModelRegistry(models_dir)
        # Per-agent active model slots: agent_id -> ActiveModel
        self._slots = Index()
        self._slots.new("agent_models")
        # Once-only load deduplication keyed by "agent_id:model_id"
        self._onceler = Onceler()
        self._load_lock = threading.RLock()

    # ------------------------------------------------------------------
    # Registry (read-only after init, safe for concurrent reads)
    # ------------------------------------------------------------------
    def list_models(self) -> list[dict]:
        return [m.to_dict() for m in self._registry.list()]

    def get_model(self, model_id: str) -> Optional[dict]:
        info = self._registry.get(model_id)
        return info.to_dict() if info else None

    def model_count(self) -> int:
        return self._registry.count()

    def has_model(self, model_id: str) -> bool:
        return self._registry.has(model_id)

    # ------------------------------------------------------------------
    # Per-agent active model
    # ------------------------------------------------------------------
    def active_model(self, agent_id: str) -> Optional[dict]:
        """Return the active model metadata for ``agent_id``, or None."""
        try:
            active = self._slots.load_from_index("agent_models", agent_id)
        except KeyError:
            return None
        if active is None:
            return None
        return active.to_dict()

    def active_model_id(self, agent_id: str) -> Optional[str]:
        active = self.active_model(agent_id)
        return active["model_id"] if active else None

    def is_loaded(self, agent_id: str) -> bool:
        return self.active_model(agent_id) is not None

    def load_model(self, agent_id: str, model_id: str) -> dict:
        """Load ``model_id`` and make it the active model for ``agent_id``.

        The load is deduplicated via :class:`Onceler`: concurrent calls for the
        same (agent_id, model_id) block on the first load and share the result.
        Returns the loaded model metadata dict.
        """
        info = self._registry.get(model_id)
        if info is None:
            raise ValueError(f"Model '{model_id}' not found in registry")

        full_key = f"{agent_id}:{model_id}"

        def _do_load() -> ActiveModel:
            module = self._load_module(info)
            return ActiveModel(model_id, info, module)

        # Run the load exactly once per (agent_id, model_id) pair.
        result, _loaded = self._onceler.store_once(
            index_name="model_loads",
            key=full_key,
            do=_do_load,
        )

        active: ActiveModel = result

        # Store/replace the agent's active slot.
        self._slots.store_in_index("agent_models", agent_id, active)
        return active.to_dict()

    def unload_model(self, agent_id: str) -> Optional[dict]:
        """Clear ``agent_id``'s active model slot and return its prior metadata."""
        try:
            previous = self._slots.load_from_index("agent_models", agent_id)
        except KeyError:
            previous = None
        if previous is None:
            return None
        self._slots.delete_from_index("agent_models", agent_id)
        return previous.to_dict()

    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------
    def run_action(
        self, agent_id: str, action: str, input_data: Any = None
    ) -> dict:
        """Run an action with ``agent_id``'s active model.

        Actions:
          - info: return active model metadata + tensor info
          - tensors: list tensor names/shapes
          - forward/predict: run forward pass (input_data = list of floats)
        """
        try:
            active = self._slots.load_from_index("agent_models", agent_id)
        except KeyError:
            active = None

        if active is None:
            return {"error": "no model loaded", "agent_id": agent_id, "active": None}

        if action == "info":
            return {
                "agent_id": agent_id,
                "active": active.to_dict(),
                "tensors": active.tensor_info(),
            }

        if action == "tensors":
            return {
                "agent_id": agent_id,
                "active": active.model_id,
                "tensors": active.tensor_info(),
            }

        if action in ("forward", "predict"):
            if input_data is None:
                return {
                    "error": "input_data required for forward/predict",
                    "agent_id": agent_id,
                }
            try:
                vec = torch.tensor(input_data, dtype=torch.float32).reshape(1, -1)
                out = active.forward(vec)
                return {
                    "agent_id": agent_id,
                    "active": active.model_id,
                    "input_shape": list(vec.shape),
                    "output": out.reshape(-1).tolist(),
                }
            except Exception as exc:
                return {
                    "error": f"forward failed: {exc}",
                    "agent_id": agent_id,
                }

        return {"error": f"unknown action: {action}", "agent_id": agent_id}

    # ------------------------------------------------------------------
    # Slot management
    # ------------------------------------------------------------------
    def list_agents(self) -> list[str]:
        """Return all agent IDs that currently have a loaded model."""
        try:
            return [k for k, _ in self._slots.range_index("agent_models")]
        except KeyError:
            return []

    def agent_count(self) -> int:
        return len(self.list_agents())

    def clear_agent(self, agent_id: str) -> bool:
        """Remove ``agent_id``'s slot. Returns True if a slot existed."""
        try:
            self._slots.delete_from_index("agent_models", agent_id)
            return True
        except KeyError:
            return False

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------
    def _load_module(self, info: ModelInfo) -> nn.Module:
        path = info.model_path

        if info.format == "safetensors":
            state_dict = safe_load_file(str(path))
            return self._build_from_state_dict(info, state_dict)

        # .pt or .bin - try full module first, fall back to state_dict
        try:
            obj = torch.load(str(path), weights_only=False)
            if isinstance(obj, nn.Module):
                return obj
            if isinstance(obj, dict):
                return self._build_from_state_dict(info, obj)
            return obj  # type: ignore[return-value]
        except Exception:
            try:
                state_dict = safe_load_file(str(path))
                return self._build_from_state_dict(info, state_dict)
            except Exception as exc2:
                raise RuntimeError(
                    f"could not load model {info.model_id}: {exc2}"
                )

    def _build_from_state_dict(self, info: ModelInfo, state_dict: dict) -> nn.Module:
        arch_name = info.architecture
        with _ARCH_LOCK:
            arch_cls = _ARCHITECTURES.get(arch_name)

        if arch_cls is not None:
            model = arch_cls()
            model.load_state_dict(state_dict)
            return model

        # Try to infer architecture from config dims
        input_dim = info.input_dim
        output_dim = info.output_dim
        if input_dim is not None and output_dim is not None:
            model = _LinearClassifier(input_dim, output_dim)
            sd = model.state_dict()
            matched = {
                k: v
                for k, v in state_dict.items()
                if k in sd and sd[k].shape == v.shape
            }
            sd.update(matched)
            model.load_state_dict(sd)
            return model

        raise RuntimeError(
            f"model {info.model_id} is a state_dict but architecture '{arch_name}' "
            "is not registered and dims are unknown"
        )


class _LinearClassifier(nn.Module):
    """Generic fallback linear model for state_dicts with known dims."""

    def __init__(self, input_dim: int, output_dim: int):
        super().__init__()
        self.fc = nn.Linear(input_dim, output_dim)

    def forward(self, x):
        return self.fc(x)


__all__ = [
    "ThreadSafeModelHarness",
    "ActiveModel",
    "register_architecture",
    "get_registered_architectures",
]
