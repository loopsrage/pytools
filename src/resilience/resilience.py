import time
import hashlib
from enum import Enum
from functools import wraps
from threading import Lock, RLock
from typing import Callable, Optional


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Circuit breaker that trips after N consecutive failures.

    States:
      CLOSED  - requests flow normally
      OPEN    - requests fail fast for cooldown seconds
      HALF_OPEN - trial request allowed to test recovery
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        cooldown_seconds: float = 30.0,
        expected_exception: type = Exception,
    ):
        self.failure_threshold = failure_threshold
        self.cooldown_seconds = cooldown_seconds
        self.expected_exception = expected_exception
        self._failures = 0
        self._state = CircuitState.CLOSED
        self._opened_at: Optional[float] = None
        self._lock = Lock()

    @property
    def state(self) -> CircuitState:
        with self._lock:
            return self._state

    def _record_success(self) -> None:
        with self._lock:
            self._failures = 0
            self._state = CircuitState.CLOSED
            self._opened_at = None

    def _record_failure(self) -> None:
        with self._lock:
            self._failures += 1
            if self._failures >= self.failure_threshold:
                self._state = CircuitState.OPEN
                self._opened_at = time.time()

    def _allow_request(self) -> bool:
        with self._lock:
            if self._state == CircuitState.CLOSED:
                return True
            if self._state == CircuitState.OPEN:
                if self._opened_at is not None and (
                    time.time() - self._opened_at
                ) >= self.cooldown_seconds:
                    self._state = CircuitState.HALF_OPEN
                    return True
                return False
            # HALF_OPEN: allow one trial
            return True

    def call(self, func: Callable, *args, **kwargs):
        if not self._allow_request():
            raise CircuitBreakerOpenError(
                f"Circuit breaker is {self._state.value}"
            )
        try:
            result = func(*args, **kwargs)
            self._record_success()
            return result
        except self.expected_exception as exc:
            self._record_failure()
            raise exc


class CircuitBreakerOpenError(Exception):
    pass


class RateLimiter:
    """Token-bucket rate limiter (tokens per second)."""

    def __init__(self, rate: float, burst: Optional[int] = None):
        self.rate = rate
        self.burst = burst if burst is not None else int(rate)
        self._tokens = float(self.burst)
        self._last = time.time()
        self._lock = Lock()

    def acquire(self, tokens: float = 1.0) -> bool:
        with self._lock:
            now = time.time()
            elapsed = now - self._last
            self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
            self._last = now
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    def wait_slot(self, tokens: float = 1.0, max_wait: float = 5.0) -> bool:
        deadline = time.time() + max_wait
        while time.time() < deadline:
            if self.acquire(tokens):
                return True
            time.sleep(0.01)
        return False


def resilient(
    breaker: Optional[CircuitBreaker] = None,
    limiter: Optional[RateLimiter] = None,
    retries: int = 0,
    backoff: float = 0.5,
    expected_exception: type = Exception,
):
    """Decorator combining circuit breaker, rate limiting, and retries."""

    def decorator(func: Callable) -> Callable:
        br = breaker or CircuitBreaker(expected_exception=expected_exception)
        rl = limiter

        @wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            while True:
                if rl is not None and not rl.acquire():
                    raise RateLimitExceeded("rate limit exceeded")
                try:
                    return br.call(func, *args, **kwargs)
                except expected_exception as exc:
                    attempt += 1
                    if attempt > retries:
                        raise
                    time.sleep(backoff * attempt)
                except CircuitBreakerOpenError:
                    raise

        wrapper.breaker = br
        wrapper.limiter = rl
        return wrapper

    return decorator


class RateLimitExceeded(Exception):
    pass


__all__ = [
    "CircuitBreaker",
    "CircuitBreakerOpenError",
    "CircuitState",
    "RateLimiter",
    "RateLimitExceeded",
    "resilient",
]
