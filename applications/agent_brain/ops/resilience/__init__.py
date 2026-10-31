"""Resilience tooling for agent operations.

Wraps :class:`src.resilience.resilience` to provide circuit breakers, rate
limiters, and a combined ``@resilient`` decorator that can be applied to any
agent action.

Each agent gets isolated circuit breakers and rate limiters, so one agent's
failures don't cascade to others.
"""

from __future__ import annotations

from typing import Callable, Optional

_REPO_SRC = str(__import__("pathlib").Path(__file__).resolve().parents[4] / "src")
import sys

if _REPO_SRC not in sys.path:
    sys.path.insert(0, _REPO_SRC)

from resilience.resilience import (  # noqa: E402
    CircuitBreaker,
    CircuitBreakerOpenError,
    CircuitState,
    RateLimiter,
    RateLimitExceeded,
    resilient,
)


class AgentResilience:
    """Manages per-agent circuit breakers and rate limiters.

    Agents are isolated: agent A tripping its circuit breaker does not affect
    agent B.
    """

    def __init__(self):
        self._breakers: dict[str, CircuitBreaker] = {}
        self._limiters: dict[str, RateLimiter] = {}
        self._lock = __import__("threading").RLock()

    def breaker_for(
        self,
        agent_id: str,
        failure_threshold: int = 5,
        cooldown_seconds: float = 30.0,
        expected_exception: type = Exception,
    ) -> CircuitBreaker:
        """Return (or create) the circuit breaker for an agent."""
        with self._lock:
            key = agent_id
            if key not in self._breakers:
                self._breakers[key] = CircuitBreaker(
                    failure_threshold=failure_threshold,
                    cooldown_seconds=cooldown_seconds,
                    expected_exception=expected_exception,
                )
            return self._breakers[key]

    def limiter_for(
        self,
        agent_id: str,
        rate: float,
        burst: Optional[int] = None,
    ) -> RateLimiter:
        """Return (or create) the rate limiter for an agent."""
        with self._lock:
            key = agent_id
            if key not in self._limiters:
                self._limiters[key] = RateLimiter(rate=rate, burst=burst)
            return self._limiters[key]

    def resilient(
        self,
        agent_id: str,
        failure_threshold: int = 5,
        cooldown_seconds: float = 30.0,
        retries: int = 0,
        backoff: float = 0.5,
        expected_exception: type = Exception,
        rate: Optional[float] = None,
        burst: Optional[int] = None,
    ):
        """Decorator combining circuit breaker, rate limiting, and retries
        for a specific agent."""
        breaker = self.breaker_for(
            agent_id,
            failure_threshold=failure_threshold,
            cooldown_seconds=cooldown_seconds,
            expected_exception=expected_exception,
        )
        limiter = None
        if rate is not None:
            limiter = self.limiter_for(agent_id, rate=rate, burst=burst)

        return resilient(
            breaker=breaker,
            limiter=limiter,
            retries=retries,
            backoff=backoff,
            expected_exception=expected_exception,
        )

    def breaker_state(self, agent_id: str) -> Optional[str]:
        """Return the current circuit breaker state for an agent."""
        with self._lock:
            b = self._breakers.get(agent_id)
            return b.state.value if b else None

    def agent_ids(self) -> list[str]:
        """Return all agent IDs that have resilience configured."""
        with self._lock:
            return sorted(set(list(self._breakers.keys()) + list(self._limiters.keys())))


__all__ = [
    "AgentResilience",
    "CircuitBreaker",
    "CircuitBreakerOpenError",
    "CircuitState",
    "RateLimiter",
    "RateLimitExceeded",
    "resilient",
]
