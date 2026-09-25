import asyncio

import pytest

from app.clients.payment_provider import (
    ProviderAmbiguousOutcomeError,
    ProviderPermanentError,
    ProviderUnavailableError,
)
from app.resilience.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerOpenError,
    CircuitBreakerState,
)


async def test_successful_call_keeps_breaker_closed() -> None:
    breaker = CircuitBreaker(
        failure_threshold=3,
        recovery_timeout_seconds=30,
    )

    async def successful_operation(
        value: str,
    ) -> str:
        return f"processed:{value}"

    result = await breaker.call(
        successful_operation,
        "payment-1",
    )

    assert result == "processed:payment-1"
    assert breaker.state is CircuitBreakerState.CLOSED
    assert breaker.failure_count == 0
    assert breaker.opened_at is None


async def test_opens_after_failure_threshold_is_reached() -> None:
    breaker = CircuitBreaker(
        failure_threshold=3,
        recovery_timeout_seconds=30,
    )

    attempts = 0

    async def failing_operation(
        _: str,
    ) -> None:
        nonlocal attempts

        attempts += 1

        raise ProviderUnavailableError(
            "provider unavailable",
        )

    for expected_failure_count in (1, 2):
        with pytest.raises(
            ProviderUnavailableError,
        ):
            await breaker.call(
                failing_operation,
                "payment-1",
            )

        assert breaker.state is CircuitBreakerState.CLOSED
        assert breaker.failure_count == expected_failure_count

    with pytest.raises(
        ProviderUnavailableError,
    ):
        await breaker.call(
            failing_operation,
            "payment-1",
        )

    assert attempts == 3
    assert breaker.failure_count == 3
    assert breaker.state is CircuitBreakerState.OPEN
    assert breaker.opened_at is not None


async def test_ambiguous_outcome_counts_as_failure() -> None:
    breaker = CircuitBreaker(
        failure_threshold=1,
        recovery_timeout_seconds=30,
    )

    async def ambiguous_operation(
        _: str,
    ) -> None:
        raise ProviderAmbiguousOutcomeError(
            "outcome unknown",
        )

    with pytest.raises(
        ProviderAmbiguousOutcomeError,
    ):
        await breaker.call(
            ambiguous_operation,
            "payment-1",
        )

    assert breaker.state is CircuitBreakerState.OPEN
    assert breaker.failure_count == 1


async def test_permanent_provider_error_does_not_count_as_failure() -> None:
    breaker = CircuitBreaker(
        failure_threshold=1,
        recovery_timeout_seconds=30,
    )

    async def permanent_failure(
        _: str,
    ) -> None:
        raise ProviderPermanentError(
            "request rejected",
            status_code=409,
        )

    with pytest.raises(
        ProviderPermanentError,
    ):
        await breaker.call(
            permanent_failure,
            "payment-1",
        )

    assert breaker.state is CircuitBreakerState.CLOSED
    assert breaker.failure_count == 0
    assert breaker.opened_at is None


async def test_open_breaker_rejects_call_without_running_operation() -> None:
    breaker = CircuitBreaker(
        failure_threshold=1,
        recovery_timeout_seconds=30,
    )

    attempts = 0

    async def failing_operation(
        _: str,
    ) -> None:
        nonlocal attempts

        attempts += 1

        raise ProviderUnavailableError(
            "provider unavailable",
        )

    with pytest.raises(
        ProviderUnavailableError,
    ):
        await breaker.call(
            failing_operation,
            "payment-1",
        )

    assert breaker.state is CircuitBreakerState.OPEN

    with pytest.raises(
        CircuitBreakerOpenError,
    ):
        await breaker.call(
            failing_operation,
            "payment-2",
        )

    assert attempts == 1


async def test_successful_half_open_call_closes_breaker() -> None:
    breaker = CircuitBreaker(
        failure_threshold=1,
        recovery_timeout_seconds=0,
    )

    async def failing_operation(
        _: str,
    ) -> None:
        raise ProviderUnavailableError(
            "provider unavailable",
        )

    async def successful_operation(
        value: str,
    ) -> str:
        assert breaker.state is CircuitBreakerState.HALF_OPEN

        return f"processed:{value}"

    with pytest.raises(
        ProviderUnavailableError,
    ):
        await breaker.call(
            failing_operation,
            "payment-1",
        )

    assert breaker.state is CircuitBreakerState.OPEN

    result = await breaker.call(
        successful_operation,
        "payment-2",
    )

    assert result == "processed:payment-2"
    assert breaker.state is CircuitBreakerState.CLOSED
    assert breaker.failure_count == 0
    assert breaker.opened_at is None


async def test_failed_half_open_call_reopens_breaker() -> None:
    breaker = CircuitBreaker(
        failure_threshold=1,
        recovery_timeout_seconds=0,
    )

    async def failing_operation(
        _: str,
    ) -> None:
        raise ProviderUnavailableError(
            "provider unavailable",
        )

    with pytest.raises(
        ProviderUnavailableError,
    ):
        await breaker.call(
            failing_operation,
            "payment-1",
        )

    assert breaker.state is CircuitBreakerState.OPEN

    with pytest.raises(
        ProviderUnavailableError,
    ):
        await breaker.call(
            failing_operation,
            "payment-2",
        )

    assert breaker.state is CircuitBreakerState.OPEN
    assert breaker.opened_at is not None


async def test_permanent_error_during_half_open_closes_breaker() -> None:
    breaker = CircuitBreaker(
        failure_threshold=1,
        recovery_timeout_seconds=0,
    )

    async def unavailable(
        _: str,
    ) -> None:
        raise ProviderUnavailableError(
            "provider unavailable",
        )

    async def permanent_failure(
        _: str,
    ) -> None:
        assert breaker.state is CircuitBreakerState.HALF_OPEN

        raise ProviderPermanentError(
            "request rejected",
            status_code=409,
        )

    with pytest.raises(
        ProviderUnavailableError,
    ):
        await breaker.call(
            unavailable,
            "payment-1",
        )

    with pytest.raises(
        ProviderPermanentError,
    ):
        await breaker.call(
            permanent_failure,
            "payment-2",
        )

    assert breaker.state is CircuitBreakerState.CLOSED
    assert breaker.failure_count == 0
    assert breaker.opened_at is None


async def test_stale_regular_success_does_not_close_half_open_breaker() -> None:
    breaker = CircuitBreaker(
        failure_threshold=1,
        recovery_timeout_seconds=0,
    )

    old_call_started = asyncio.Event()
    release_old_call = asyncio.Event()

    probe_started = asyncio.Event()
    release_probe = asyncio.Event()

    async def slow_success(
        value: str,
    ) -> str:
        old_call_started.set()

        await release_old_call.wait()

        return f"processed:{value}"

    async def failing_operation(
        _: str,
    ) -> None:
        raise ProviderUnavailableError(
            "provider unavailable",
        )

    async def successful_probe(
        value: str,
    ) -> str:
        probe_started.set()

        await release_probe.wait()

        return f"processed:{value}"

    old_task = asyncio.create_task(
        breaker.call(
            slow_success,
            "old-payment",
        )
    )

    await old_call_started.wait()

    with pytest.raises(
        ProviderUnavailableError,
    ):
        await breaker.call(
            failing_operation,
            "failed-payment",
        )

    probe_task = asyncio.create_task(
        breaker.call(
            successful_probe,
            "probe-payment",
        )
    )

    await probe_started.wait()

    assert breaker.state is CircuitBreakerState.HALF_OPEN

    release_old_call.set()

    assert await old_task == "processed:old-payment"
    assert breaker.state is CircuitBreakerState.HALF_OPEN

    release_probe.set()

    assert await probe_task == "processed:probe-payment"
    assert breaker.state is CircuitBreakerState.CLOSED


async def test_stale_regular_failure_does_not_reopen_half_open_breaker() -> None:
    breaker = CircuitBreaker(
        failure_threshold=1,
        recovery_timeout_seconds=0,
    )

    old_call_started = asyncio.Event()
    release_old_call = asyncio.Event()

    probe_started = asyncio.Event()
    release_probe = asyncio.Event()

    async def slow_failure(
        _: str,
    ) -> None:
        old_call_started.set()

        await release_old_call.wait()

        raise ProviderUnavailableError(
            "old failure",
        )

    async def failing_operation(
        _: str,
    ) -> None:
        raise ProviderUnavailableError(
            "provider unavailable",
        )

    async def successful_probe(
        value: str,
    ) -> str:
        probe_started.set()

        await release_probe.wait()

        return f"processed:{value}"

    old_task = asyncio.create_task(
        breaker.call(
            slow_failure,
            "old-payment",
        )
    )

    await old_call_started.wait()

    with pytest.raises(
        ProviderUnavailableError,
    ):
        await breaker.call(
            failing_operation,
            "failed-payment",
        )

    probe_task = asyncio.create_task(
        breaker.call(
            successful_probe,
            "probe-payment",
        )
    )

    await probe_started.wait()

    assert breaker.state is CircuitBreakerState.HALF_OPEN

    release_old_call.set()

    with pytest.raises(
        ProviderUnavailableError,
    ):
        await old_task

    assert breaker.state is CircuitBreakerState.HALF_OPEN

    release_probe.set()

    assert await probe_task == "processed:probe-payment"
    assert breaker.state is CircuitBreakerState.CLOSED


async def test_cancelled_half_open_probe_reopens_breaker() -> None:
    breaker = CircuitBreaker(
        failure_threshold=1,
        recovery_timeout_seconds=0,
    )

    probe_started = asyncio.Event()
    keep_probe_running = asyncio.Event()

    async def failing_operation(
        _: str,
    ) -> None:
        raise ProviderUnavailableError(
            "provider unavailable",
        )

    async def cancellable_probe(
        _: str,
    ) -> None:
        probe_started.set()

        await keep_probe_running.wait()

    with pytest.raises(
        ProviderUnavailableError,
    ):
        await breaker.call(
            failing_operation,
            "payment-1",
        )

    probe_task = asyncio.create_task(
        breaker.call(
            cancellable_probe,
            "payment-2",
        )
    )

    await probe_started.wait()

    assert breaker.state is CircuitBreakerState.HALF_OPEN

    probe_task.cancel()

    with pytest.raises(
        asyncio.CancelledError,
    ):
        await probe_task

    assert breaker.state is CircuitBreakerState.OPEN
    assert breaker.opened_at is not None


async def test_cancelled_regular_call_does_not_count_as_failure() -> None:
    breaker = CircuitBreaker(
        failure_threshold=1,
        recovery_timeout_seconds=30,
    )

    operation_started = asyncio.Event()
    keep_running = asyncio.Event()

    async def cancellable_operation(
        _: str,
    ) -> None:
        operation_started.set()

        await keep_running.wait()

    task = asyncio.create_task(
        breaker.call(
            cancellable_operation,
            "payment-1",
        )
    )

    await operation_started.wait()

    task.cancel()

    with pytest.raises(
        asyncio.CancelledError,
    ):
        await task

    assert breaker.state is CircuitBreakerState.CLOSED
    assert breaker.failure_count == 0
    assert breaker.opened_at is None


async def test_inconclusive_half_open_call_reopens_breaker() -> None:
    breaker = CircuitBreaker(
        failure_threshold=1,
        recovery_timeout_seconds=0,
    )

    async def provider_failure(
        _: str,
    ) -> None:
        raise ProviderUnavailableError(
            "provider unavailable",
        )

    async def inconclusive_probe(
        _: str,
    ) -> None:
        assert breaker.state is CircuitBreakerState.HALF_OPEN

        raise RuntimeError(
            "unexpected probe error",
        )

    with pytest.raises(
        ProviderUnavailableError,
    ):
        await breaker.call(
            provider_failure,
            "payment-1",
        )

    with pytest.raises(
        RuntimeError,
        match="unexpected probe error",
    ):
        await breaker.call(
            inconclusive_probe,
            "payment-2",
        )

    assert breaker.state is CircuitBreakerState.OPEN
    assert breaker.opened_at is not None


async def test_unknown_exception_in_closed_does_not_change_state() -> None:
    breaker = CircuitBreaker(
        failure_threshold=3,
        recovery_timeout_seconds=30,
    )

    async def provider_failure(
        _: str,
    ) -> None:
        raise ProviderUnavailableError(
            "provider unavailable",
        )

    async def unexpected_error(
        _: str,
    ) -> None:
        raise RuntimeError(
            "unexpected application error",
        )

    with pytest.raises(
        ProviderUnavailableError,
    ):
        await breaker.call(
            provider_failure,
            "payment-1",
        )

    assert breaker.failure_count == 1

    with pytest.raises(
        RuntimeError,
        match="unexpected application error",
    ):
        await breaker.call(
            unexpected_error,
            "payment-2",
        )

    assert breaker.state is CircuitBreakerState.CLOSED
    assert breaker.failure_count == 1
    assert breaker.opened_at is None
