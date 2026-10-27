"""AETHER FastAPI Control Plane: REST, SSE/WebSocket Token Streaming, Guardrails & OpenTelemetry."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from aether_core.gateway import GatewayRequest, GatewayResponse, SmartGateway
from aether_core.graph import (
    DAGExecutionReport,
    InMemoryACIDEventStore,
    StatefulDAG,
    ToolKind,
    execute_python_sandbox,
    execute_readonly_sql,
)
from aether_core.memory import HybridSemanticMemory, SemanticSearchResult
from aether_core.telemetry import AetherTracer
from aether_evals import BenchmarkSuiteReport, HybridEvaluationRunner
from aether_guardrails import GuardrailViolationError, StreamingGuardrailEngine


class DAGRunPayload(BaseModel):
    """Request schema to trigger or resume a stateful 4-step enterprise DAG."""

    run_id: str = Field(..., min_length=1)
    query: str = Field(..., min_length=1)
    simulate_crash_at_step: int | None = Field(default=None, ge=1, le=4)


class MemoryIngestPayload(BaseModel):
    content: str = Field(..., min_length=1)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ControlPlaneState:
    """Singleton state container wired into the FastAPI application."""

    def __init__(self) -> None:
        self.gateway = SmartGateway()
        self.event_store = InMemoryACIDEventStore()
        self.memory = HybridSemanticMemory(max_short_term_tokens=2048, embedding_dims=64)
        self.guardrails = StreamingGuardrailEngine(rolling_window_chars=256)
        self.tracer = AetherTracer(service_name="aether-engine-api")
        self.eval_runner = HybridEvaluationRunner()


def create_app(state: ControlPlaneState | None = None) -> FastAPI:
    cp = state or ControlPlaneState()
    app = FastAPI(
        title="AETHER Engine Control Plane",
        version="0.4.2",
        description="Autonomous Event-driven Task & Hybrid Evaluation Runtime",
    )
    app.state.cp = cp

    @app.get("/health")
    async def health() -> dict[str, Any]:
        return {
            "status": "HEALTHY",
            "version": "0.4.2",
            "circuits": cp.gateway.telemetry_snapshot(),
            "memory_vectors": cp.memory.vector_count,
            "active_spans": len(cp.tracer.export_spans()),
        }

    @app.post("/v1/gateway/complete", response_model=GatewayResponse)
    async def gateway_complete(req: GatewayRequest) -> GatewayResponse:
        with cp.tracer.start_span("gateway.complete", attributes={"prompt_len": len(req.prompt)}) as span:
            try:
                cp.guardrails.validate_inbound_prompt(req.prompt)
            except GuardrailViolationError as exc:
                span.attributes["guardrail.blocked"] = str(exc.category)
                raise HTTPException(status_code=400, detail=str(exc)) from exc

            response = await cp.gateway.execute(req)
            span.attributes["provider.used"] = response.provider_used
            span.attributes["cost.usd"] = response.estimated_cost_usd
            return response

    @app.post("/v1/gateway/stream")
    async def gateway_stream_sse(req: GatewayRequest, http_request: Request) -> StreamingResponse:
        try:
            cp.guardrails.validate_inbound_prompt(req.prompt)
        except GuardrailViolationError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        cancel_event = asyncio.Event()
        rolling_buffer = ""

        def inspect_chunk(tok: str) -> None:
            nonlocal rolling_buffer
            rolling_buffer = (rolling_buffer + tok)[-cp.guardrails.rolling_window_chars :]
            cp.guardrails.inspect_rolling_buffer(rolling_buffer)

        async def event_generator() -> AsyncIterator[str]:
            try:
                async for chunk in cp.gateway.stream(
                    req,
                    cancel_event=cancel_event,
                    token_interceptor=inspect_chunk,
                ):
                    if await http_request.is_disconnected():
                        cancel_event.set()
                        break
                    yield f"data: {chunk.model_dump_json()}\n\n"
            except GuardrailViolationError as exc:
                err_payload = json.dumps({"error": str(exc), "category": str(exc.category)})
                yield f"event: guardrail_violation\ndata: {err_payload}\n\n"

        return StreamingResponse(event_generator(), media_type="text/event-stream")

    @app.websocket("/v1/gateway/ws")
    async def gateway_stream_ws(websocket: WebSocket) -> None:
        await websocket.accept()
        cancel_event = asyncio.Event()
        try:
            raw = await websocket.receive_text()
            req = GatewayRequest.model_validate_json(raw)
            cp.guardrails.validate_inbound_prompt(req.prompt)

            rolling_buffer = ""

            def inspect_chunk(tok: str) -> None:
                nonlocal rolling_buffer
                rolling_buffer = (rolling_buffer + tok)[-cp.guardrails.rolling_window_chars :]
                cp.guardrails.inspect_rolling_buffer(rolling_buffer)

            async for chunk in cp.gateway.stream(
                req,
                cancel_event=cancel_event,
                token_interceptor=inspect_chunk,
            ):
                await websocket.send_text(chunk.model_dump_json())
            await websocket.close()
        except GuardrailViolationError as exc:
            await websocket.send_text(
                json.dumps({"error": str(exc), "category": str(exc.category)})
            )
            await websocket.close(code=1008)
        except WebSocketDisconnect:
            cancel_event.set()

    @app.post("/v1/dag/execute", response_model=DAGExecutionReport)
    async def execute_resilient_dag(payload: DAGRunPayload) -> DAGExecutionReport:
        dag = StatefulDAG(name="enterprise-agent-pipeline", event_store=cp.event_store)

        async def step_1_retrieve(state: dict[str, Any]) -> dict[str, Any]:
            if payload.simulate_crash_at_step == 1:
                raise RuntimeError("Simulated worker crash at step 1")
            hits = cp.memory.retrieve_hybrid(payload.query, top_k=2)
            return {"retrieved_docs": [h.record.content for h in hits], "_cost_usd": 0.0001}

        async def step_2_sql_audit(state: dict[str, Any]) -> dict[str, Any]:
            if payload.simulate_crash_at_step == 2:
                raise RuntimeError("Simulated worker crash at step 2")
            rows = execute_readonly_sql(
                "SELECT 'SLA_OK' AS policy_status, 99.95 AS uptime_pct"
            )
            return {"sql_audit": rows[0], "_cost_usd": 0.0001}

        async def step_3_python_sandbox(state: dict[str, Any]) -> dict[str, Any]:
            if payload.simulate_crash_at_step == 3:
                raise RuntimeError("Simulated worker crash at step 3")
            sandbox_out = execute_python_sandbox(
                "score = round(uptime * 1.0, 2)",
                context={"uptime": state["sql_audit"]["uptime_pct"]},
            )
            return {"sandbox_score": sandbox_out["score"], "_cost_usd": 0.0001}

        async def step_4_llm_synthesis(state: dict[str, Any]) -> dict[str, Any]:
            if payload.simulate_crash_at_step == 4:
                raise RuntimeError("Simulated worker crash at step 4")
            resp = await cp.gateway.execute(
                GatewayRequest(
                    prompt=f"Synthesize result for '{payload.query}' with score={state['sandbox_score']}"
                )
            )
            return {
                "synthesis": resp.content,
                "provider": resp.provider_used,
                "_cost_usd": resp.estimated_cost_usd,
            }

        dag.add_node("step_1_retrieve", step_1_retrieve, kind=ToolKind.WEB_SEARCH)
        dag.add_node(
            "step_2_sql_audit",
            step_2_sql_audit,
            dependencies=["step_1_retrieve"],
            kind=ToolKind.SQL_READONLY,
        )
        dag.add_node(
            "step_3_python_sandbox",
            step_3_python_sandbox,
            dependencies=["step_2_sql_audit"],
            kind=ToolKind.PYTHON_SANDBOX,
        )
        dag.add_node(
            "step_4_llm_synthesis",
            step_4_llm_synthesis,
            dependencies=["step_3_python_sandbox"],
            kind=ToolKind.LLM_INFERENCE,
        )

        with cp.tracer.start_span("dag.execute", attributes={"run_id": payload.run_id}):
            try:
                return await dag.execute(payload.run_id, initial_state={"query": payload.query})
            except RuntimeError as exc:
                raise HTTPException(status_code=500, detail=str(exc)) from exc

    @app.post("/v1/memory/ingest")
    async def memory_ingest(payload: MemoryIngestPayload) -> dict[str, Any]:
        rec = cp.memory.ingest(payload.content, payload.metadata)
        return {"record_id": rec.record_id, "token_count": rec.token_count}

    @app.get("/v1/memory/search", response_model=list[SemanticSearchResult])
    async def memory_search(q: str, top_k: int = 5) -> list[SemanticSearchResult]:
        return cp.memory.retrieve_hybrid(q, top_k=top_k)

    @app.post("/v1/evals/run", response_model=BenchmarkSuiteReport)
    async def run_evals() -> BenchmarkSuiteReport:
        return await cp.eval_runner.run_suite()

    @app.get("/v1/telemetry/spans")
    async def get_spans() -> list[dict[str, Any]]:
        return cp.tracer.export_spans()

    return app


app = create_app()
