"""Executable 500-Concurrent-Agent Stress Benchmark & 50-Case Golden Eval CLI."""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

from aether_core.gateway import (
    CircuitBreakerConfig,
    DeterministicMockAdapter,
    GatewayRequest,
    ModelProvider,
    SmartGateway,
)
from aether_evals.evaluator import HybridEvaluationRunner
from aether_guardrails import StreamingGuardrailEngine


async def run_500_concurrent_agents_benchmark() -> dict[str, Any]:
    """Simulate 500 concurrent agents streaming tokens through SmartGateway + Guardrails."""
    cfg = CircuitBreakerConfig(failure_threshold=3, sla_max_latency_ms=1000.0)
    gw = SmartGateway(breaker_config=cfg)
    guardrails = StreamingGuardrailEngine()

    for provider in gw.provider_order:
        gw.register_adapter(
            provider,
            DeterministicMockAdapter(provider, simulated_latency_ms=0.0),
        )

    routing_latencies_ms: list[float] = []
    total_tokens = 0

    async def single_agent_worker(agent_id: int) -> int:
        req = GatewayRequest(prompt=f"Agent #{agent_id} mission-critical execution plan")
        t0 = time.perf_counter()
        tokens_count = 0
        rolling = ""
        async for chunk in gw.stream(req):
            if not chunk.is_final:
                tokens_count += 1
                rolling = (rolling + chunk.token)[-128:]
                guardrails.inspect_rolling_buffer(rolling)
        routing_latencies_ms.append((time.perf_counter() - t0) * 1000.0)
        return tokens_count

    wall_start = time.perf_counter()
    counts = await asyncio.gather(*(single_agent_worker(i) for i in range(500)))
    wall_elapsed_sec = max(0.001, time.perf_counter() - wall_start)
    total_tokens = sum(counts)
    throughput_tok_per_sec = round(total_tokens / wall_elapsed_sec, 1)

    sorted_lat = sorted(routing_latencies_ms)
    p50 = round(sorted_lat[int(len(sorted_lat) * 0.50)], 3)
    p95 = round(sorted_lat[int(len(sorted_lat) * 0.95)], 3)
    p99 = round(sorted_lat[int(len(sorted_lat) * 0.99)], 3)

    # Measure deterministic failover time when primary Gemini fails 3x
    gw.register_adapter(
        ModelProvider.GEMINI_2_PRO,
        DeterministicMockAdapter(
            ModelProvider.GEMINI_2_PRO, simulated_latency_ms=0.0, fail_status_code=503
        ),
    )
    failover_resp = await gw.execute(GatewayRequest(prompt="Measure failover SLA"))

    eval_report = await HybridEvaluationRunner(
        min_accuracy_threshold=0.90, max_p95_latency_ms=12.0
    ).run_suite()

    return {
        "concurrent_agents": 500,
        "total_tokens_streamed": total_tokens,
        "throughput_tokens_per_sec": throughput_tok_per_sec,
        "routing_p50_ms": p50,
        "routing_p95_ms": p95,
        "routing_p99_ms": p99,
        "failover_time_ms": failover_resp.failover_time_ms,
        "golden_eval_accuracy": eval_report.overall_accuracy,
        "golden_eval_p95_ms": eval_report.p95_latency_ms,
        "quality_gate_passed": eval_report.quality_gate_passed,
    }


def main() -> None:
    results = asyncio.run(run_500_concurrent_agents_benchmark())
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
