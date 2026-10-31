from resilience.resilience import (
    CircuitBreaker,
    CircuitBreakerOpenError,
    CircuitState,
    RateLimiter,
    RateLimitExceeded,
    resilient,
)


def test_circuit_breaker_opens_after_threshold():
    cb = CircuitBreaker(failure_threshold=2, cooldown_seconds=0.1, expected_exception=ValueError)

    def fail():
        raise ValueError("boom")

    for _ in range(2):
        try:
            cb.call(fail)
        except ValueError:
            pass
    assert cb.state == CircuitState.OPEN
    try:
        cb.call(fail)
    except CircuitBreakerOpenError:
        pass
    else:
        raise AssertionError("expected CircuitBreakerOpenError")


def test_circuit_breaker_half_open_recovers():
    cb = CircuitBreaker(failure_threshold=1, cooldown_seconds=0.05, expected_exception=ValueError)

    def fail_once():
        raise ValueError("boom")

    def succeed():
        return "ok"

    try:
        cb.call(fail_once)
    except ValueError:
        pass
    assert cb.state == CircuitState.OPEN

    import time

    time.sleep(0.06)
    assert cb.call(succeed) == "ok"
    assert cb.state == CircuitState.CLOSED


def test_rate_limiter_burst():
    rl = RateLimiter(rate=100.0, burst=2)
    assert rl.acquire() is True
    assert rl.acquire() is True
    assert rl.acquire() is False


def test_resilient_retries():
    calls = {"n": 0}

    @resilient(retries=2, backoff=0.01, expected_exception=ValueError)
    def flaky():
        calls["n"] += 1
        if calls["n"] < 2:
            raise ValueError("retry")
        return "success"

    assert flaky() == "success"
    assert calls["n"] == 2


def test_resilient_circuit_breaker_open():
    breaker = CircuitBreaker(failure_threshold=1, cooldown_seconds=60, expected_exception=ValueError)

    @resilient(breaker=breaker, retries=0, expected_exception=ValueError)
    def always_fail():
        raise ValueError("boom")

    try:
        always_fail()
    except ValueError:
        pass
    try:
        always_fail()
    except CircuitBreakerOpenError:
        pass
    else:
        raise AssertionError("expected CircuitBreakerOpenError")
