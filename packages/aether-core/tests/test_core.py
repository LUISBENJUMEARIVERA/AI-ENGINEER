"""Unit & Integration tests for SmartGateway, CircuitBreaker, DAG Event Sourcing, and Memory."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from aether_core.gateway import (
    CircuitBreakerConfig,
    CircuitState,
    DeterministicMockAdapter,
    GatewayRequest,
    ModelProvider,
    ProviderCallError,
    SmartGateway,
)
from aether_core.graph import (
    CyclicDependencyError,
    InMemoryACIDEventStore,
    SecuritySandboxViolationError,
    StatefulDAG,
    ToolKind,
    execute_python_sandbox,
    execute_readonly_sql,
)
from aether_core.memory import HybridSemanticMemory


async def test_circuit_breaker_trips_after_3_failures_and_fails_over() -> None:
    """Phase 1: If Gemini fails 3 times in a row, traffic switches automatically to Claude in <150ms."""
    cfg = CircuitBreakerConfig(
        failure_threshold=3,
        recovery_timeout_sec=0.25,
        sla_max_latency_ms=500.0,
    )
    gw = SmartGateway(breaker_config=cfg, max_retries_per_provider=1)
    # Inject HTTP 503 failure on Gemini primary provider
    gw.register_adapter(
        ModelProvider.GEMINI_2_PRO,
        DeterministicMockAdapter(
            ModelProvider.GEMINI_2_PRO, simulated_latency_ms=2.0, fail_status_code=503
        ),
    )
    gw.register_adapter(
        ModelProvider.CLAUDE_3_5_SONNET,
        DeterministicMockAdapter(ModelProvider.CLAUDE_3_5_SONNET, simulated_latency_ms=2.0),
    )

    for _ in range(3):
        resp = await gw.execute(GatewayRequest(prompt="Execute distributed state transition"))
        assert resp.provider_used == ModelProvider.CLAUDE_3_5_SONNET
        assert resp.failover_time_ms < 150.0

    # After 3 consecutive failures, Gemini circuit MUST be OPEN
    gemini_breaker = gw.breakers[ModelProvider.GEMINI_2_PRO]
    assert gemini_breaker.state == CircuitState.OPEN
    assert gemini_breaker.metrics.consecutive_failures == 3

    # Wait for recovery cooldown -> transitions to HALF_OPEN -> probe succeeds -> CLOSED
    await asyncio.sleep(0.28)
    assert gemini_breaker.state == CircuitState.HALF_OPEN
    gw.register_adapter(
        ModelProvider.GEMINI_2_PRO,
        DeterministicMockAdapter(ModelProvider.GEMINI_2_PRO, simulated_latency_ms=1.5),
    )
    recovered = await gw.execute(GatewayRequest(prompt="Probe Gemini recovery"))
    assert recovered.provider_used == ModelProvider.GEMINI_2_PRO
    assert gemini_breaker.state == CircuitState.CLOSED


async def test_streaming_context_cancellation_stops_gpu_immediately() -> None:
    """Phase 1: When client closes tab/cancels context, LLM inference aborts immediately."""
    gw = SmartGateway()
    mock_adapter = DeterministicMockAdapter(
        ModelProvider.GEMINI_2_PRO, simulated_latency_ms=5.0
    )
    gw.register_adapter(ModelProvider.GEMINI_2_PRO, mock_adapter)

    cancel_event = asyncio.Event()
    received_tokens: list[str] = []

    with pytest.raises(asyncio.CancelledError):
        async for chunk in gw.stream(
            GatewayRequest(prompt="Stream tokens until client disconnects"),
            cancel_event=cancel_event,
        ):
            received_tokens.append(chunk.token)
            if len(received_tokens) == 2:
                cancel_event.set()

    assert len(received_tokens) == 2
    assert mock_adapter.active_streams == 0
    assert mock_adapter.aborted_streams == 1


async def test_dag_crash_at_step_4_resumes_exactly_at_step_4_without_duplicate_llm_calls(
    tmp_path: Path,
) -> None:
    """Phase 2: Worker dies at step 4 -> resumes at step 4 without re-running steps 1..3 or extra cost."""
    sqlite_journal = tmp_path / "aether_events.db"
    store = InMemoryACIDEventStore(sqlite_path=sqlite_journal)
    call_counts: dict[str, int] = {"step_1": 0, "step_2": 0, "step_3": 0, "step_4": 0}
    should_crash_step_4 = True

    def build_dag() -> StatefulDAG:
        dag = StatefulDAG("mission-critical-workflow", event_store=store)

        async def s1(state: dict[str, Any]) -> dict[str, Any]:
            call_counts["step_1"] += 1
            return {"s1": "ingested", "_cost_usd": 0.01}

        async def s2(state: dict[str, Any]) -> dict[str, Any]:
            call_counts["step_2"] += 1
            return {"s2": "analyzed", "_cost_usd": 0.02}

        async def s3(state: dict[str, Any]) -> dict[str, Any]:
            call_counts["step_3"] += 1
            return {"s3": "validated", "_cost_usd": 0.03}

        async def s4(state: dict[str, Any]) -> dict[str, Any]:
            call_counts["step_4"] += 1
            if should_crash_step_4:
                raise RuntimeError("SIGKILL worker crash at step 4")
            return {"s4": "synthesized_final", "_cost_usd": 0.04}

        dag.add_node("step_1", s1, kind=ToolKind.WEB_SEARCH)
        dag.add_node("step_2", s2, dependencies=["step_1"], kind=ToolKind.SQL_READONLY)
        dag.add_node("step_3", s3, dependencies=["step_2"], kind=ToolKind.PYTHON_SANDBOX)
        dag.add_node("step_4", s4, dependencies=["step_3"], kind=ToolKind.LLM_INFERENCE)
        return dag

    # First execution crashes at step 4
    dag_worker_1 = build_dag()
    with pytest.raises(RuntimeError, match="SIGKILL worker crash at step 4"):
        await dag_worker_1.execute("run-tx-9001", initial_state={"seed": 42})

    assert call_counts == {"step_1": 1, "step_2": 1, "step_3": 1, "step_4": 1}

    # Second worker starts fresh instance reading from SQLite ACID journal and resumes at step 4
    should_crash_step_4 = False
    store_reloaded = InMemoryACIDEventStore(sqlite_path=sqlite_journal)
    dag_worker_2 = StatefulDAG("mission-critical-workflow", event_store=store_reloaded)
    for nid, node in dag_worker_1.nodes.items():
        dag_worker_2.add_node(
            nid,
            dag_worker_1.handlers[nid],
            dependencies=node.dependencies,
            kind=node.kind,
        )

    report = await dag_worker_2.execute("run-tx-9001", initial_state={"seed": 42})
    assert report.resumed_from_checkpoint_nodes == ["step_1", "step_2", "step_3"]
    assert report.executed_nodes == ["step_4"]
    # Steps 1, 2, 3 were NEVER invoked a second time!
    assert call_counts == {"step_1": 1, "step_2": 1, "step_3": 1, "step_4": 2}
    assert report.total_cost_usd == pytest.approx(0.10)
    assert report.final_state["s4"] == "synthesized_final"


def test_dag_cycle_detection_and_sandbox_security() -> None:
    dag = StatefulDAG("cyclic-test")

    async def noop(s: dict[str, Any]) -> dict[str, Any]:
        return s

    dag.add_node("A", noop, dependencies=["B"])
    dag.add_node("B", noop, dependencies=["A"])
    with pytest.raises(CyclicDependencyError):
        dag.topological_sort()

    # Verify AST Python sandbox blocks imports and dunder escapes
    with pytest.raises(SecuritySandboxViolationError):
        execute_python_sandbox("import os")

    # Verify SQL sandbox blocks DROP TABLE
    with pytest.raises(SecuritySandboxViolationError):
        execute_readonly_sql("DROP TABLE users")


def test_hybrid_semantic_memory_window_and_pgvector_retrieval() -> None:
    mem = HybridSemanticMemory(max_short_term_tokens=10, embedding_dims=64)
    mem.ingest("PostgreSQL pgvector HNSW index stores embeddings with ACID durability")
    mem.ingest("Circuit breaker trips on HTTP 429 rate limits and routes to Claude 3.5")
    mem.ingest("OpenTelemetry exports distributed W3C spans to Jaeger and Langfuse")

    # Ring buffer evicted older chunks to stay <= 10 tokens, while L2 vector store retained all 3
    assert mem.vector_count == 3
    assert mem.short_term_token_count <= 10

    hits = mem.retrieve_hybrid("pgvector HNSW embeddings ACID", top_k=2)
    assert len(hits) >= 1
    assert "pgvector" in hits[0].record.content
    assert "CREATE EXTENSION IF NOT EXISTS vector;" in mem.pgvector_ddl()
    assert ProviderCallError("test", 429, "rate limit").status_code == 429
