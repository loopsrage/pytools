"""Model architecture definitions registered with the harness.

Registers architectures so that state_dict (.pt / .safetensors) models can be
rebuilt into live nn.Module instances for inference.
"""

import sys
from pathlib import Path

_HERE = str(Path(__file__).resolve().parent)
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

import torch
import torch.nn as nn

from applications.agent_brain.models.model_harness import register_architecture


class TinyClassifier(nn.Module):
    """Small classifier: input -> Linear -> output."""

    def __init__(self, input_dim: int = 10, output_dim: int = 3):
        super().__init__()
        self.fc = nn.Linear(input_dim, output_dim)

    def forward(self, x):
        return self.fc(x)


class TinyEmbedder(nn.Module):
    """Small embedder: input -> Linear -> output."""

    def __init__(self, input_dim: int = 8, output_dim: int = 4):
        super().__init__()
        self.proj = nn.Linear(input_dim, output_dim)

    def forward(self, x):
        return self.proj(x)


# Default architectures
register_architecture("TinyClassifier", TinyClassifier)
register_architecture("TinyEmbedder", TinyEmbedder)

__all__ = ["TinyClassifier", "TinyEmbedder"]
