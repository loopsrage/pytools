import hashlib
import json
from datetime import datetime, timezone
from typing import Optional

from thread_safe.index import Index


class AuditEntry:
    def __init__(
        self,
        actor: str,
        action: str,
        resource: str,
        details: Optional[dict] = None,
        entry_id: Optional[str] = None,
        previous_hash: Optional[str] = None,
        timestamp: Optional[str] = None,
    ):
        self.entry_id = entry_id or _new_id()
        self.timestamp = timestamp or datetime.now(timezone.utc).isoformat()
        self.actor = actor
        self.action = action
        self.resource = resource
        self.details = details or {}
        self.previous_hash = previous_hash
        self.current_hash = self._compute_hash()

    def _compute_hash(self) -> str:
        payload = {
            "entry_id": self.entry_id,
            "timestamp": self.timestamp,
            "actor": self.actor,
            "action": self.action,
            "resource": self.resource,
            "details": self.details,
            "previous_hash": self.previous_hash,
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True).encode("utf-8")
        ).hexdigest()

    def to_dict(self) -> dict:
        return {
            "entry_id": self.entry_id,
            "timestamp": self.timestamp,
            "actor": self.actor,
            "action": self.action,
            "resource": self.resource,
            "details": self.details,
            "previous_hash": self.previous_hash,
            "current_hash": self.current_hash,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "AuditEntry":
        entry = cls(
            actor=data["actor"],
            action=data["action"],
            resource=data["resource"],
            details=data.get("details", {}),
            entry_id=data["entry_id"],
            previous_hash=data.get("previous_hash"),
            timestamp=data.get("timestamp"),
        )
        # current_hash is intentionally recomputed by the constructor so
        # verify() can detect content tampering.
        return entry


def _new_id() -> str:
    import uuid

    return uuid.uuid4().hex


class AuditLog:
    """Immutable, hash-chained audit log.

    Each entry's hash covers the previous entry's hash, so any
    modification to an earlier entry invalidates the whole chain.
    """

    def __init__(self, name: str = "audit"):
        self._index = Index()
        self._name = name
        self._lock_key = f"{name}:lock"
        self._head_key = f"{name}:head"
        self._ensure_head()

    def _ensure_head(self) -> None:
        try:
            if self._index.load_from_index(self._name, self._head_key) is None:
                self._store_head({"previous_hash": None, "count": 0})
        except KeyError:
            self._store_head({"previous_hash": None, "count": 0})

    def _head(self) -> dict:
        try:
            return self._index.load_from_index(self._name, self._head_key) or {
                "previous_hash": None,
                "count": 0,
            }
        except KeyError:
            return {"previous_hash": None, "count": 0}

    def _store_head(self, head: dict) -> None:
        self._index.store_in_index(self._name, self._head_key, head)

    def append(
        self,
        actor: str,
        action: str,
        resource: str,
        details: Optional[dict] = None,
    ) -> AuditEntry:
        head = self._head()
        entry = AuditEntry(
            actor=actor,
            action=action,
            resource=resource,
            details=details,
            previous_hash=head.get("previous_hash"),
        )
        self._index.store_in_index(self._name, entry.entry_id, entry.to_dict())
        self._store_head(
            {
                "previous_hash": entry.current_hash,
                "count": head.get("count", 0) + 1,
            }
        )
        return entry

    def get(self, entry_id: str) -> Optional[dict]:
        return self._index.load_from_index(self._name, entry_id)

    def entries(self) -> list[dict]:
        return [
            v
            for _, v in self._index.range_index(self._name)
            if v.get("entry_id") is not None
        ]

    def verify(self) -> bool:
        """Verify hash-chain integrity across all entries."""
        prev_hash = None
        for entry in self.entries():
            if entry.get("previous_hash") != prev_hash:
                return False
            reconstructed = AuditEntry.from_dict(entry)
            if reconstructed.current_hash != entry.get("current_hash"):
                return False
            prev_hash = entry.get("current_hash")
        return True

    def count(self) -> int:
        return self._head().get("count", 0)
