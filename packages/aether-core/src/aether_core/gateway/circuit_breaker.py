"""Circuit Breaker Pattern with Sliding Window Failure Tracking, SLA Latency Thresholds, and Exponential Jitter."""

from __future__ import annotations

import asyncio
import random
import time
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class CircuitState(StrEnum):
    """Deterministic finite states of the Smart Gateway Circuit Breaker."""

    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreakerOpenError(RuntimeError):
    """Raised when a call is rejected because the provider circuit is OPEN."""

    def __init__(self, provider: str, retry_after_ms: float) -> None:
        self.provider = provider
        self.retry_after_ms = retry_after_ms
        super().__init__(
            f"Circuit breaker OPEN for provider '{provider}'. Retry after {retry_after_ms:.1f}ms."
        )


class ProviderCallError(RuntimeError):
    """Represents an upstream model provider HTTP/RPC error (e.g. 429, 500, 503) or SLA breach."""

    def __init__(self, provider: str, status_code: int, message: str) -> None:
        self.provider = provider
        self.status_code = status_code
        super().__init__(f"[{provider} HTTP {status_code}] {message}")


class CircuitBreakerConfig(BaseModel):
    """Configuration for per-provider circuit breaker & SLA enforcement."""

    failure_threshold: int = Field(default=3, ge=1, description="Consecutive failures to trip OPEN")
    recovery_timeout_sec: float = Field(
        default=15.0, gt=0.0, description="Cooldown period before transitioning to HALF_OPEN"
    )
    half_open_max_calls: int = Field(
        default=1, ge=1, description="Successful probe calls required in HALF_OPEN to close circuit"
    )
    sla_max_latency_ms: float = Field(
        default=4500.0, gt=0.0, description="Hard SLA latency ceiling; exceeding counts as failure"
    )
    base_backoff_ms: float = Field(default=50.0, gt=0.0)
    max_backoff_ms: float = Field(default=2000.0, gt=0.0)


@dataclass
class CircuitMetrics:
    """Real-time telemetry metrics per model provider circuit."""

    state: CircuitState = CircuitState.CLOSED
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    total_calls: int = 0
    total_failures: int = 0
    total_sla_breaches: int = 0
    last_failure_timestamp: float = 0.0
    last_state_change_timestamp: float = field(default_factory=time.monotonic)
    latencies_ms: deque[float] = field(default_factory=lambda: deque(maxlen=500))

    @property
    def p95_latency_ms(self) -> float:
        if not self.latencies_ms:
            return 0.0
        sorted_lat = sorted(self.latencies_ms)
        idx = min(int(len(sorted_lat) * 0.95), len(sorted_lat) - 1)
        return round(sorted_lat[idx], 3)

    @property
    def p99_latency_ms(self) -> float:
        if not self.latencies_ms:
            return 0.0
        sorted_lat = sorted(self.latencies_ms)
        idx = min(int(len(sorted_lat) * 0.99), len(sorted_lat) - 1)
        return round(sorted_lat[idx], 3)


class CircuitBreaker:
    """Async-safe deterministic Circuit Breaker with SLA latency enforcement and Full Jitter backoff."""

    def __init__(self, provider: str, config: CircuitBreakerConfig | None = None) -> None:
        self.provider = provider
        self.config = config or CircuitBreakerConfig()
        self.metrics = CircuitMetrics()
        self._lock = asyncio.Lock()

    @property
    def state(self) -> CircuitState:
        """Evaluate current state including lazy OPEN -> HALF_OPEN cooldown transition."""
        if self.metrics.state == CircuitState.OPEN:
            elapsed = time.monotonic() - self.metrics.last_failure_timestamp
            if elapsed >= self.config.recovery_timeout_sec:
                self.metrics.state = CircuitState.HALF_OPEN
                self.metrics.consecutive_successes = 0
                self.metrics.last_state_change_timestamp = time.monotonic()
        return self.metrics.state

    def compute_backoff_with_jitter_ms(self, attempt: int) -> float:
        """AWS Architecture Full Jitter algorithm: sleep = random(0, min(cap, base * 2 ** attempt))."""
        exponential = min(
            self.config.max_backoff_ms,
            self.config.base_backoff_ms * (2 ** max(0, attempt)),
        )
        return random.uniform(0.0, exponential)  # noqa: S311

    async def can_execute(self) -> bool:
        async with self._lock:
            current = self.state
            return current in (CircuitState.CLOSED, CircuitState.HALF_OPEN)

    async def record_success(self, latency_ms: float) -> None:
        async with self._lock:
            self.metrics.total_calls += 1
            self.metrics.latencies_ms.append(latency_ms)

            if latency_ms > self.config.sla_max_latency_ms:
                self.metrics.total_sla_breaches += 1
                self._handle_failure_locked()
                return

            self.metrics.consecutive_failures = 0
            if self.metrics.state == CircuitState.HALF_OPEN:
                self.metrics.consecutive_successes += 1
                if self.metrics.consecutive_successes >= self.config.half_open_max_calls:
                    self.metrics.state = CircuitState.CLOSED
                    self.metrics.last_state_change_timestamp = time.monotonic()
            else:
                self.metrics.state = CircuitState.CLOSED

    async def record_failure(self, latency_ms: float | None = None) -> None:
        async with self._lock:
            self.metrics.total_calls += 1
            if latency_ms is not None:
                self.metrics.latencies_ms.append(latency_ms)
            self._handle_failure_locked()

    def _handle_failure_locked(self) -> None:
        self.metrics.total_failures += 1
        self.metrics.consecutive_failures += 1
        self.metrics.consecutive_successes = 0
        self.metrics.last_failure_timestamp = time.monotonic()

        if (
            self.metrics.state == CircuitState.HALF_OPEN
            or self.metrics.consecutive_failures >= self.config.failure_threshold
        ):
            self.metrics.state = CircuitState.OPEN
            self.metrics.last_state_change_timestamp = time.monotonic()

    async def call(self, func: Callable[..., Awaitable[T]], *args: Any, **kwargs: Any) -> T:
        """Execute an async callable guarded by the circuit breaker and SLA timer."""
        if not await self.can_execute():
            elapsed = time.monotonic() - self.metrics.last_failure_timestamp
            remaining_ms = max(0.0, (self.config.recovery_timeout_sec - elapsed) * 1000.0)
            raise CircuitBreakerOpenError(self.provider, remaining_ms)

        start = time.perf_counter()
        try:
            result = await asyncio.wait_for(
                func(*args, **kwargs),
                timeout=self.config.sla_max_latency_ms / 1000.0,
            )
        except TimeoutError as exc:
            latency_ms = (time.perf_counter() - start) * 1000.0
            self.metrics.total_sla_breaches += 1
            await self.record_failure(latency_ms)
            raise ProviderCallError(
                self.provider,
                504,
                f"SLA timeout exceeded ({latency_ms:.1f}ms > {self.config.sla_max_latency_ms:.1f}ms)",
            ) from exc
        except Exception:
            latency_ms = (time.perf_counter() - start) * 1000.0
            await self.record_failure(latency_ms)
            raise

        latency_ms = (time.perf_counter() - start) * 1000.0
        await self.record_success(latency_ms)
        return result

    def snapshot(self) -> dict[str, Any]:
        """Serializable state snapshot for OpenTelemetry / REST health inspection."""
        return {
            "provider": self.provider,
            "state": str(self.state),
            "consecutive_failures": self.metrics.consecutive_failures,
            "total_calls": self.metrics.total_calls,
            "total_failures": self.metrics.total_failures,
            "total_sla_breaches": self.metrics.total_sla_breaches,
            "p95_latency_ms": self.metrics.p95_latency_ms,
            "p99_latency_ms": self.metrics.p99_latency_ms,
        }
