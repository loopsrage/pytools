import hashlib
from typing import Optional

from thread_safe.index import Index


class FeatureFlag:
    def __init__(
        self,
        name: str,
        enabled: bool = True,
        rollout_percentage: float = 0.0,
        target_users: Optional[list[str]] = None,
        target_attributes: Optional[dict] = None,
        description: Optional[str] = None,
    ):
        if not 0.0 <= rollout_percentage <= 100.0:
            raise ValueError("rollout_percentage must be between 0 and 100")
        self.name = name
        self.enabled = enabled
        self.rollout_percentage = rollout_percentage
        self.target_users = target_users or []
        self.target_attributes = target_attributes or {}
        self.description = description or ""

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "enabled": self.enabled,
            "rollout_percentage": self.rollout_percentage,
            "target_users": self.target_users,
            "target_attributes": self.target_attributes,
            "description": self.description,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "FeatureFlag":
        return cls(
            name=data["name"],
            enabled=data.get("enabled", True),
            rollout_percentage=data.get("rollout_percentage", 0.0),
            target_users=data.get("target_users", []),
            target_attributes=data.get("target_attributes", {}),
            description=data.get("description", ""),
        )


def _bucket(user_id: str, feature_name: str) -> float:
    """Consistent 0-100 bucket for a user/feature pair."""
    digest = hashlib.sha256(f"{feature_name}:{user_id}".encode("utf-8")).hexdigest()
    return int(digest[:8], 16) / 0xFFFFFFFF * 100.0


class FeatureFlags:
    def __init__(self, namespace: str = "feature_flags"):
        self._index = Index()
        self._namespace = namespace

    def _get(self, name: str) -> Optional[FeatureFlag]:
        try:
            data = self._index.load_from_index(self._namespace, name)
            return FeatureFlag.from_dict(data) if data else None
        except KeyError:
            return None

    def _put(self, flag: FeatureFlag) -> None:
        self._index.store_in_index(self._namespace, flag.name, flag.to_dict())

    def create(self, flag: FeatureFlag) -> None:
        if self._get(flag.name) is not None:
            raise ValueError(f"Feature flag '{flag.name}' already exists")
        self._put(flag)

    def update(self, flag: FeatureFlag) -> None:
        self._put(flag)

    def delete(self, name: str) -> bool:
        if self._get(name) is None:
            return False
        self._index.store_in_index(self._namespace, name, None)
        return True

    def get(self, name: str) -> Optional[FeatureFlag]:
        return self._get(name)

    def list(self) -> list[FeatureFlag]:
        flags = []
        try:
            for _, data in self._index.range_index(self._namespace):
                if data is not None:
                    flags.append(FeatureFlag.from_dict(data))
        except KeyError:
            pass
        return flags

    def is_enabled(self, name: str, user_id: Optional[str] = None, attributes: Optional[dict] = None) -> bool:
        flag = self._get(name)
        if flag is None or not flag.enabled:
            return False

        # Explicit user targeting
        if user_id and user_id in flag.target_users:
            return True

        # Attribute targeting (all must match)
        if attributes and flag.target_attributes:
            for key, value in flag.target_attributes.items():
                if attributes.get(key) != value:
                    return False
            return True

        # Percentage rollout (consistent bucketing)
        if user_id and flag.rollout_percentage > 0:
            return _bucket(user_id, name) < flag.rollout_percentage

        # No rollout and no targeting -> disabled unless explicitly enabled with 100%
        return flag.rollout_percentage >= 100.0
