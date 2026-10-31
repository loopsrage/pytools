"""Feature flag management for agent features.

Wraps :class:`src.feature_flags.feature_flags.FeatureFlags` to provide
percentage rollouts, user/attribute targeting, and consistent hash bucketing
for agent features.

Each feature flag is global (not agent-scoped), but the ``is_enabled`` check
takes an optional ``user_id``/``attributes`` for per-call evaluation.
"""

from __future__ import annotations

from typing import Optional

_REPO_SRC = str(__import__("pathlib").Path(__file__).resolve().parents[4] / "src")
import sys

if _REPO_SRC not in sys.path:
    sys.path.insert(0, _REPO_SRC)

from feature_flags.feature_flags import FeatureFlag, FeatureFlags  # noqa: E402


class AgentFeatureFlags:
    """Feature flags scoped to the agent brain.

    Provides percentage rollouts, explicit user targeting, and attribute-based
    targeting for enabling/disabling agent features.
    """

    def __init__(self, namespace: str = "agent_brain_features"):
        self._flags = FeatureFlags(namespace=namespace)

    def create(self, flag: FeatureFlag) -> None:
        """Create a new feature flag."""
        self._flags.create(flag)

    def update(self, flag: FeatureFlag) -> None:
        """Update an existing feature flag."""
        self._flags.update(flag)

    def delete(self, name: str) -> bool:
        """Delete a feature flag. Returns False if it didn't exist."""
        return self._flags.delete(name)

    def get(self, name: str) -> Optional[FeatureFlag]:
        """Get a feature flag by name."""
        return self._flags.get(name)

    def list(self) -> list[FeatureFlag]:
        """List all feature flags."""
        return self._flags.list()

    def is_enabled(
        self,
        name: str,
        user_id: Optional[str] = None,
        attributes: Optional[dict] = None,
    ) -> bool:
        """Check if a feature is enabled.

        Evaluation order:
          1. If flag is disabled globally -> False
          2. If user_id is in target_users -> True
          3. If attributes match all target_attributes -> True
          4. If rollout_percentage > 0 and user_id buckets in -> True
          5. Otherwise -> False
        """
        return self._flags.is_enabled(
            name=name, user_id=user_id, attributes=attributes
        )

    def count(self) -> int:
        """Return the number of feature flags."""
        return len(self._flags.list())


__all__ = ["AgentFeatureFlags", "FeatureFlag", "FeatureFlags"]
