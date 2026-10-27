"""Tests for the 50-Question Golden Evaluation Suite & Quality Gate."""

from __future__ import annotations

from aether_evals import HybridEvaluationRunner, build_golden_dataset_50
from aether_evals.bench_runner import run_500_concurrent_agents_benchmark


async def test_50_golden_dataset_quality_gate_exceeds_90_percent() -> None:
    dataset = build_golden_dataset_50()
    assert len(dataset) == 50

    runner = HybridEvaluationRunner(min_accuracy_threshold=0.90, max_p95_latency_ms=50.0)
    report = await runner.run_suite(dataset)

    assert report.total_cases == 50
    assert report.passed_cases == 50
    assert report.overall_accuracy >= 0.90
    assert report.p95_latency_ms < 12.0
    assert report.quality_gate_passed is True

    bench = await run_500_concurrent_agents_benchmark()
    assert bench["concurrent_agents"] == 500
    assert bench["throughput_tokens_per_sec"] > 2400
    assert bench["failover_time_ms"] < 150.0
    assert bench["quality_gate_passed"] is True
