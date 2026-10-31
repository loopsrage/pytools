"""Operations tooling for the agent brain.

Provides production-grade concerns that agents need at scale:

- ``ops/audit`` - Tamper-evident, hash-chained audit trails per agent
- ``ops/resilience`` - Per-agent circuit breakers, rate limiters, retries
- ``ops/feature_flags`` - Percentage rollouts, user/attribute targeting
- ``ops/secrets`` - Secure credential injection (env + files)
- ``ops/webhook`` - Reliable webhook delivery with retries + dead-letter

Each module wraps the corresponding tool in ``src/`` and scopes it to the
agent brain so multiple agents can operate independently.
"""

__all__ = ["audit", "resilience", "feature_flags", "secrets", "webhook"]
