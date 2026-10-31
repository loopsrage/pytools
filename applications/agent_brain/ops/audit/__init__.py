"""Agent-scoped audit logging.

Wraps :class:`src.auditlib.auditlib.AuditLog` to provide tamper-evident,
hash-chained audit trails for all agent actions within the brain.

Each audit entry is scoped to an agent_id and records actor, action, resource,
and optional details. The hash chain ensures any modification to an earlier
entry invalidates the entire chain.
"""

from __future__ import annotations

from typing import Optional

_REPO_SRC = str(__import__("pathlib").Path(__file__).resolve().parents[4] / "src")
import sys

if _REPO_SRC not in sys.path:
    sys.path.insert(0, _REPO_SRC)

from auditlib.auditlib import AuditLog, AuditEntry  # noqa: E402


class AgentAuditLog:
    """Audit log that scopes entries by agent_id.

    Each agent gets its own isolated audit log instance, ensuring that
    agent A cannot tamper with agent B's audit trail.
    """

    def __init__(self):
        self._logs: dict[str, AuditLog] = {}

    def _log_for(self, agent_id: str) -> AuditLog:
        if agent_id not in self._logs:
            self._logs[agent_id] = AuditLog(name=f"audit:{agent_id}")
        return self._logs[agent_id]

    def append(
        self,
        agent_id: str,
        actor: str,
        action: str,
        resource: str,
        details: Optional[dict] = None,
    ) -> AuditEntry:
        """Append an audit entry for the given agent."""
        return self._log_for(agent_id).append(
            actor=actor, action=action, resource=resource, details=details
        )

    def get(self, agent_id: str, entry_id: str) -> Optional[dict]:
        """Retrieve a specific audit entry by ID."""
        return self._log_for(agent_id).get(entry_id)

    def entries(self, agent_id: str) -> list[dict]:
        """Return all audit entries for an agent."""
        return self._log_for(agent_id).entries()

    def verify(self, agent_id: str) -> bool:
        """Verify hash-chain integrity for an agent's audit trail."""
        return self._log_for(agent_id).verify()

    def count(self, agent_id: str) -> int:
        """Return the number of audit entries for an agent."""
        return self._log_for(agent_id).count()

    def agent_ids(self) -> list[str]:
        """Return all agent IDs that have audit entries."""
        return list(self._logs.keys())


__all__ = ["AgentAuditLog", "AuditEntry", "AuditLog"]
