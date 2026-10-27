"""Smart Multi-Provider LLM Gateway with Failover, Context Cancellation, and Cost Auditing."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Callable
from enum import StrEnum
from typing import Any, Protocol

from pydantic import BaseModel, Field

from aether_core.gateway.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerOpenError,
    ProviderCallError,
)


class ModelProvider(StrEnum):
    """Supported enterprise LLM providers in priority/fallback topology."""

    GEMINI_2_PRO = "gemini-2.0-pro"
    CLAUDE_3_5_SONNET = "claude-3-5-sonnet"
    GPT_4O = "gpt-4o"
    LOCAL_VLLM = "vllm-llama-3.1-70b"


# USD cost per 1,000 tokens (input, output) for real-time financial auditing
PROVIDER_COST_PER_1K_TOKENS: dict[str, tuple[float, float]] = {
    ModelProvider.GEMINI_2_PRO: (0.00125, 0.0050),
    ModelProvider.CLAUDE_3_5_SONNET: (0.00300, 0.0150),
    ModelProvider.GPT_4O: (0.00250, 0.0100),
    ModelProvider.LOCAL_VLLM: (0.00015, 0.00015),
}


class GatewayRequest(BaseModel):
    """Canonical model invocation request across providers."""

    prompt: str = Field(..., min_length=1)
    system_prompt: str = Field(default="You are AETHER, a deterministic systems assistant.")
    temperature: float = Field(default=0.1, ge=0.0, le=2.0)
    max_tokens: int = Field(default=512, ge=1, le=32768)
    preferred_provider: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class StreamTokenChunk(BaseModel):
    """Atomic streaming token frame emitted over SSE / WebSockets."""

    token: str
    provider: str
    index: int
    cumulative_cost_usd: float
    latency_ms: float
    is_final: bool = False


class GatewayResponse(BaseModel):
    """Complete aggregated response with OpenTelemetry-ready metrics and financial audit."""

    content: str
    provider_used: str
    fallback_chain: list[str] = Field(default_factory=list)
    failover_time_ms: float = 0.0
    total_latency_ms: float
    prompt_tokens: int
    completion_tokens: int
    estimated_cost_usd: float


class ProviderAdapter(Protocol):
    """Protocol for LiteLLM / vLLM / Native SDK provider adapters."""

    async def complete(self, request: GatewayRequest) -> str: ...

    def stream(self, request: GatewayRequest) -> AsyncIterator[str]: ...


class DeterministicMockAdapter:
    """High-speed deterministic adapter for CI, benchmarks, and fault-injection testing."""

    def __init__(
        self,
        provider_name: str,
        simulated_latency_ms: float = 4.0,
        fail_status_code: int | None = None,
    ) -> None:
        self.provider_name = provider_name
        self.simulated_latency_ms = simulated_latency_ms
        self.fail_status_code = fail_status_code
        self.active_streams: int = 0
        self.aborted_streams: int = 0

    async def complete(self, request: GatewayRequest) -> str:
        if self.simulated_latency_ms > 0.0:
            await asyncio.sleep(self.simulated_latency_ms / 1000.0)
        if self.fail_status_code is not None:
            raise ProviderCallError(
                self.provider_name,
                self.fail_status_code,
                f"Upstream provider error {self.fail_status_code}",
            )
        return f"[{self.provider_name}] Verified deterministic execution for: {request.prompt[:80]}"

    async def stream(self, request: GatewayRequest) -> AsyncIterator[str]:
        if self.fail_status_code is not None:
            if self.simulated_latency_ms > 0.0:
                await asyncio.sleep(self.simulated_latency_ms / 1000.0)
            raise ProviderCallError(
                self.provider_name,
                self.fail_status_code,
                f"Stream initialization error {self.fail_status_code}",
            )
        tokens = [
            f"[{self.provider_name}]",
            " Executing",
            " mission-critical",
            " task:",
            f" {request.prompt[:32]}",
            " ->",
            " STATUS_OK.",
        ]
        self.active_streams += 1
        completed_all = False
        try:
            for tok in tokens:
                if self.simulated_latency_ms > 0.0:
                    await asyncio.sleep(self.simulated_latency_ms / 1000.0)
                yield tok
            completed_all = True
        finally:
            if not completed_all:
                self.aborted_streams += 1
            self.active_streams -= 1


class SmartGateway:
    """Multi-provider routing gateway with automatic Circuit Breaker failover and stream cancellation."""

    def __init__(
        self,
        provider_order: list[str] | None = None,
        breaker_config: CircuitBreakerConfig | None = None,
        max_retries_per_provider: int = 1,
    ) -> None:
        self.provider_order = provider_order or [
            ModelProvider.GEMINI_2_PRO,
            ModelProvider.CLAUDE_3_5_SONNET,
            ModelProvider.GPT_4O,
            ModelProvider.LOCAL_VLLM,
        ]
        self.breaker_config = breaker_config or CircuitBreakerConfig()
        self.max_retries_per_provider = max_retries_per_provider

        self.breakers: dict[str, CircuitBreaker] = {
            p: CircuitBreaker(p, self.breaker_config) for p in self.provider_order
        }
        self.adapters: dict[str, ProviderAdapter] = {
            p: DeterministicMockAdapter(p) for p in self.provider_order
        }

    def register_adapter(self, provider: str, adapter: ProviderAdapter) -> None:
        """Register or override a model provider adapter (LiteLLM, vLLM, or custom mock)."""
        if provider not in self.breakers:
            self.breakers[provider] = CircuitBreaker(provider, self.breaker_config)
            self.provider_order.append(provider)
        self.adapters[provider] = adapter

    @staticmethod
    def calculate_cost_usd(provider: str, prompt_tokens: int, completion_tokens: int) -> float:
        in_rate, out_rate = PROVIDER_COST_PER_1K_TOKENS.get(provider, (0.002, 0.006))
        cost = (prompt_tokens / 1000.0) * in_rate + (completion_tokens / 1000.0) * out_rate
        return round(cost, 8)

    def _resolve_routing_chain(self, preferred: str | None) -> list[str]:
        if preferred and preferred in self.provider_order:
            return [preferred] + [p for p in self.provider_order if p != preferred]
        return list(self.provider_order)

    async def execute(self, request: GatewayRequest) -> GatewayResponse:
        """Route completion through circuit breakers with exponential jitter and instant failover."""
        chain = self._resolve_routing_chain(request.preferred_provider)
        attempted_chain: list[str] = []
        overall_start = time.perf_counter()
        failover_start: float | None = None
        last_error: Exception | None = None

        for provider in chain:
            breaker = self.breakers[provider]
            adapter = self.adapters[provider]
            attempted_chain.append(provider)

            if not await breaker.can_execute():
                if failover_start is None:
                    failover_start = time.perf_counter()
                continue

            for attempt in range(self.max_retries_per_provider):
                try:
                    content = await breaker.call(adapter.complete, request)
                    total_latency_ms = (time.perf_counter() - overall_start) * 1000.0
                    failover_ms = (
                        (time.perf_counter() - failover_start) * 1000.0
                        if failover_start is not None
                        else 0.0
                    )
                    prompt_tokens = max(1, len(request.prompt.split()))
                    completion_tokens = max(1, len(content.split()))
                    return GatewayResponse(
                        content=content,
                        provider_used=provider,
                        fallback_chain=attempted_chain,
                        failover_time_ms=round(failover_ms, 3),
                        total_latency_ms=round(total_latency_ms, 3),
                        prompt_tokens=prompt_tokens,
                        completion_tokens=completion_tokens,
                        estimated_cost_usd=self.calculate_cost_usd(
                            provider, prompt_tokens, completion_tokens
                        ),
                    )
                except (ProviderCallError, CircuitBreakerOpenError) as exc:
                    last_error = exc
                    if failover_start is None:
                        failover_start = time.perf_counter()
                    if not await breaker.can_execute():
                        break
                    if attempt + 1 < self.max_retries_per_provider:
                        backoff_ms = breaker.compute_backoff_with_jitter_ms(attempt)
                        await asyncio.sleep(backoff_ms / 1000.0)

        raise ProviderCallError(
            "ALL_PROVIDERS_EXHAUSTED",
            503,
            f"All providers in fallback chain {attempted_chain} failed. Last error: {last_error}",
        )

    async def stream(
        self,
        request: GatewayRequest,
        cancel_event: asyncio.Event | None = None,
        token_interceptor: Callable[[str], None] | None = None,
    ) -> AsyncIterator[StreamTokenChunk]:
        """Stream tokens with context cancellation (stops GPU inference immediately on disconnect)."""
        chain = self._resolve_routing_chain(request.preferred_provider)
        prompt_tokens = max(1, len(request.prompt.split()))
        last_error: Exception | None = None

        for provider in chain:
            breaker = self.breakers[provider]
            adapter = self.adapters[provider]

            if not await breaker.can_execute():
                continue

            start = time.perf_counter()
            try:
                idx = 0
                stream_iter = adapter.stream(request).__aiter__()
                while True:
                    if cancel_event is not None and cancel_event.is_set():
                        # Explicitly close underlying async generator so GPU inference halts immediately
                        if hasattr(stream_iter, "aclose"):
                            await stream_iter.aclose()
                        raise asyncio.CancelledError("Client disconnected; GPU inference aborted.")

                    try:
                        token = await asyncio.wait_for(
                            stream_iter.__anext__(),
                            timeout=self.breaker_config.sla_max_latency_ms / 1000.0,
                        )
                    except StopAsyncIteration:
                        break

                    if token_interceptor is not None:
                        token_interceptor(token)

                    idx += 1
                    cost = self.calculate_cost_usd(provider, prompt_tokens, idx)
                    elapsed_ms = (time.perf_counter() - start) * 1000.0
                    yield StreamTokenChunk(
                        token=token,
                        provider=provider,
                        index=idx,
                        cumulative_cost_usd=cost,
                        latency_ms=round(elapsed_ms, 3),
                        is_final=False,
                    )

                total_ms = (time.perf_counter() - start) * 1000.0
                await breaker.record_success(total_ms)
                yield StreamTokenChunk(
                    token="",
                    provider=provider,
                    index=idx,
                    cumulative_cost_usd=self.calculate_cost_usd(provider, prompt_tokens, idx),
                    latency_ms=round(total_ms, 3),
                    is_final=True,
                )
                return

            except asyncio.CancelledError:
                raise
            except TimeoutError as exc:
                elapsed_ms = (time.perf_counter() - start) * 1000.0
                await breaker.record_failure(elapsed_ms)
                last_error = ProviderCallError(provider, 504, "Stream token SLA timeout")
                _ = exc
                continue
            except ProviderCallError as exc:
                elapsed_ms = (time.perf_counter() - start) * 1000.0
                await breaker.record_failure(elapsed_ms)
                last_error = exc
                continue

        raise ProviderCallError(
            "ALL_PROVIDERS_EXHAUSTED",
            503,
            f"Streaming failover exhausted all providers. Last error: {last_error}",
        )

    def telemetry_snapshot(self) -> dict[str, Any]:
        return {p: b.snapshot() for p, b in self.breakers.items()}
