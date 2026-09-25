import asyncio
from collections.abc import Awaitable, Callable
from enum import StrEnum
from time import monotonic

import structlog

from app.clients.payment_provider import (
    ProviderAmbiguousOutcomeError,
    ProviderPermanentError,
    ProviderUnavailableError,
)

logger = structlog.get_logger()


class CircuitBreakerState(StrEnum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreakerOpenError(RuntimeError):
    pass


class CircuitBreaker:
    def __init__(self, failure_threshold: int, recovery_timeout_seconds: float) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be at least 1")

        if recovery_timeout_seconds < 0:
            raise ValueError("recovery_timeout_seconds must not be negative")

        self.failure_threshold = failure_threshold
        self.recovery_timeout_seconds = recovery_timeout_seconds

        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.opened_at: float | None = None

    def _before_call(self) -> bool:
        if self.state is CircuitBreakerState.CLOSED:
            return False

        if self.state is CircuitBreakerState.HALF_OPEN:
            raise CircuitBreakerOpenError("Circuit Breaker is open")

        if self.state is CircuitBreakerState.OPEN:
            if self.opened_at is None:
                raise RuntimeError("Circuit Breaker is open without opened_at time")

            elapsed = monotonic() - self.opened_at

            if elapsed < self.recovery_timeout_seconds:
                raise CircuitBreakerOpenError("Circuit Breaker is open")

            self.state = CircuitBreakerState.HALF_OPEN

            logger.info(
                "circuit_breaker_state_changed",
                previous_state=CircuitBreakerState.OPEN.value,
                new_state=CircuitBreakerState.HALF_OPEN.value,
                reason="recovery_timeout_elapsed",
                elapsed_seconds=round(elapsed, 3),
                recovery_timeout_seconds=self.recovery_timeout_seconds,
            )

            return True

        raise RuntimeError(f"Unknown Circuit Breaker state: {self.state}")

    def _record_success(self, is_half_open_probe: bool) -> None:
        if is_half_open_probe:
            if self.state is not CircuitBreakerState.HALF_OPEN:
                return

            self.state = CircuitBreakerState.CLOSED
            self.failure_count = 0
            self.opened_at = None

            logger.info(
                "circuit_breaker_state_changed",
                previous_state=CircuitBreakerState.HALF_OPEN.value,
                new_state=CircuitBreakerState.CLOSED.value,
                reason="trial_call_succeeded",
            )

            return

        if self.state is not CircuitBreakerState.CLOSED:
            return

        self.failure_count = 0
        self.opened_at = None

    def _record_failure(self, is_half_open_probe: bool) -> None:
        if is_half_open_probe:
            if self.state is not CircuitBreakerState.HALF_OPEN:
                return

            self.state = CircuitBreakerState.OPEN
            self.opened_at = monotonic()

            logger.warning(
                "circuit_breaker_state_changed",
                previous_state=CircuitBreakerState.HALF_OPEN.value,
                new_state=CircuitBreakerState.OPEN.value,
                reason="trial_call_failed",
                recovery_timeout_seconds=self.recovery_timeout_seconds,
            )

            return

        if self.state is not CircuitBreakerState.CLOSED:
            return

        self.failure_count += 1

        if self.failure_count >= self.failure_threshold:
            self.state = CircuitBreakerState.OPEN
            self.opened_at = monotonic()

            logger.warning(
                "circuit_breaker_state_changed",
                previous_state=CircuitBreakerState.CLOSED.value,
                new_state=CircuitBreakerState.OPEN.value,
                reason="failure_threshold_reached",
                failure_count=self.failure_count,
                failure_threshold=self.failure_threshold,
            )

    def _record_cancelled_probe(self, is_half_open_probe: bool) -> None:
        if not is_half_open_probe:
            return

        if self.state is not CircuitBreakerState.HALF_OPEN:
            return

        self.state = CircuitBreakerState.OPEN
        self.opened_at = monotonic()

        logger.warning(
            "circuit_breaker_state_changed",
            previous_state=CircuitBreakerState.HALF_OPEN.value,
            new_state=CircuitBreakerState.OPEN.value,
            reason="trial_call_cancelled",
            recovery_timeout_seconds=self.recovery_timeout_seconds,
        )

    def _record_inconclusive_probe(self, is_half_open_probe: bool) -> None:
        if not is_half_open_probe:
            return

        if self.state is not CircuitBreakerState.HALF_OPEN:
            return

        self.state = CircuitBreakerState.OPEN
        self.opened_at = monotonic()

        logger.warning(
            "circuit_breaker_state_changed",
            previous_state=CircuitBreakerState.HALF_OPEN.value,
            new_state=CircuitBreakerState.OPEN.value,
            reason="trial_call_inconclusive",
            recovery_timeout_seconds=self.recovery_timeout_seconds,
        )

    async def call[ArgumentT, ResultT](
        self,
        operation: Callable[
            [ArgumentT],
            Awaitable[ResultT],
        ],
        argument: ArgumentT,
    ) -> ResultT:
        is_half_open_probe = self._before_call()

        try:
            result = await operation(argument)
        except asyncio.CancelledError:
            self._record_cancelled_probe(is_half_open_probe)
            raise
        except ProviderUnavailableError, ProviderAmbiguousOutcomeError:
            self._record_failure(is_half_open_probe)
            raise
        except ProviderPermanentError:
            # Provider ответил детерминированно.
            # Это не outage и не повод открывать breaker.
            self._record_success(is_half_open_probe)
            raise
        except Exception:
            self._record_inconclusive_probe(is_half_open_probe)
            raise

        self._record_success(is_half_open_probe)

        return result
