"""Transactional Event-Sourced DAG Runner with ACID Checkpoint Resumption."""

from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
import time
from collections import defaultdict, deque
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol

from pydantic import BaseModel, Field


class NodeStatus(StrEnum):
    """Execution states of a DAG node."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class ToolKind(StrEnum):
    """Built-in enterprise sandboxed tool executors."""

    LLM_INFERENCE = "LLM_INFERENCE"
    PYTHON_SANDBOX = "PYTHON_SANDBOX"
    SQL_READONLY = "SQL_READONLY"
    WEB_SEARCH = "WEB_SEARCH"
    CUSTOM = "CUSTOM"


class ExecutionEvent(BaseModel):
    """Immutable Event-Sourcing journal entry persisted per state transition."""

    run_id: str
    node_id: str
    status: NodeStatus
    step_index: int
    state_delta: dict[str, Any] = Field(default_factory=dict)
    cost_usd: float = 0.0
    latency_ms: float = 0.0
    error_message: str | None = None
    event_hash: str = ""
    created_at: str = Field(default_factory=lambda: datetime.now(UTC).isoformat())

    def compute_integrity_hash(self) -> str:
        payload = json.dumps(
            {
                "run_id": self.run_id,
                "node_id": self.node_id,
                "status": str(self.status),
                "step_index": self.step_index,
                "state_delta": self.state_delta,
                "cost_usd": self.cost_usd,
            },
            sort_keys=True,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class EventStore(Protocol):
    """Port for ACID Event-Sourcing persistence (PostgreSQL / Redis / SQLite)."""

    async def append_event(self, event: ExecutionEvent) -> None: ...

    async def get_events(self, run_id: str) -> list[ExecutionEvent]: ...

    async def get_completed_nodes(self, run_id: str) -> dict[str, ExecutionEvent]: ...


class InMemoryACIDEventStore:
    """In-memory and optional SQLite-backed ACID event journal for local & CI deterministic runs."""

    def __init__(self, sqlite_path: str | Path | None = None) -> None:
        self._events: dict[str, list[ExecutionEvent]] = defaultdict(list)
        self._lock = asyncio.Lock()
        self.sqlite_path = str(sqlite_path) if sqlite_path else None
        if self.sqlite_path:
            self._init_sqlite()

    def _init_sqlite(self) -> None:
        assert self.sqlite_path is not None
        with sqlite3.connect(self.sqlite_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS aether_event_journal (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    step_index INTEGER NOT NULL,
                    payload_json TEXT NOT NULL,
                    event_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_run_node ON aether_event_journal(run_id, node_id)"
            )
            conn.commit()

    async def append_event(self, event: ExecutionEvent) -> None:
        async with self._lock:
            event.event_hash = event.compute_integrity_hash()
            self._events[event.run_id].append(event)
            if self.sqlite_path:
                with sqlite3.connect(self.sqlite_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO aether_event_journal
                        (run_id, node_id, status, step_index, payload_json, event_hash, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            event.run_id,
                            event.node_id,
                            str(event.status),
                            event.step_index,
                            event.model_dump_json(),
                            event.event_hash,
                            event.created_at,
                        ),
                    )
                    conn.commit()

    async def get_events(self, run_id: str) -> list[ExecutionEvent]:
        async with self._lock:
            if self.sqlite_path and run_id not in self._events:
                with sqlite3.connect(self.sqlite_path) as conn:
                    rows = conn.execute(
                        "SELECT payload_json FROM aether_event_journal WHERE run_id = ? ORDER BY id ASC",
                        (run_id,),
                    ).fetchall()
                self._events[run_id] = [
                    ExecutionEvent.model_validate_json(row[0]) for row in rows
                ]
            return list(self._events.get(run_id, []))

    async def get_completed_nodes(self, run_id: str) -> dict[str, ExecutionEvent]:
        events = await self.get_events(run_id)
        completed: dict[str, ExecutionEvent] = {}
        for ev in events:
            if ev.status == NodeStatus.COMPLETED:
                completed[ev.node_id] = ev
        return completed


NodeHandler = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]


class DAGNode(BaseModel):
    """Single executable vertex in an AETHER Stateful DAG."""

    node_id: str = Field(..., min_length=1)
    kind: ToolKind = ToolKind.CUSTOM
    dependencies: list[str] = Field(default_factory=list)
    description: str = ""
    cost_estimate_usd: float = 0.0005


class DAGExecutionReport(BaseModel):
    """Audit report emitted upon DAG completion or partial crash recovery."""

    run_id: str
    final_state: dict[str, Any]
    executed_nodes: list[str]
    resumed_from_checkpoint_nodes: list[str]
    total_cost_usd: float
    total_latency_ms: float
    event_count: int


class CyclicDependencyError(ValueError):
    """Raised when a cycle is detected during Kahn's topological sort."""


class StatefulDAG:
    """Directed Acyclic Graph with topological validation and transactional checkpoint recovery."""

    def __init__(self, name: str, event_store: EventStore | None = None) -> None:
        self.name = name
        self.event_store: EventStore = event_store or InMemoryACIDEventStore()
        self.nodes: dict[str, DAGNode] = {}
        self.handlers: dict[str, NodeHandler] = {}

    def add_node(
        self,
        node_id: str,
        handler: NodeHandler,
        *,
        dependencies: list[str] | None = None,
        kind: ToolKind = ToolKind.CUSTOM,
        description: str = "",
        cost_estimate_usd: float = 0.0005,
    ) -> None:
        deps = dependencies or []
        self.nodes[node_id] = DAGNode(
            node_id=node_id,
            kind=kind,
            dependencies=deps,
            description=description,
            cost_estimate_usd=cost_estimate_usd,
        )
        self.handlers[node_id] = handler

    def topological_sort(self) -> list[str]:
        """Deterministic Kahn's algorithm ensuring acyclic execution order."""
        in_degree: dict[str, int] = dict.fromkeys(self.nodes, 0)
        adj: dict[str, list[str]] = defaultdict(list)

        for nid, node in self.nodes.items():
            for dep in node.dependencies:
                if dep not in self.nodes:
                    raise KeyError(f"Node '{nid}' depends on unknown node '{dep}'.")
                adj[dep].append(nid)
                in_degree[nid] += 1

        queue: deque[str] = deque(sorted(nid for nid, deg in in_degree.items() if deg == 0))
        ordered: list[str] = []

        while queue:
            current = queue.popleft()
            ordered.append(current)
            for neighbor in sorted(adj[current]):
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if len(ordered) != len(self.nodes):
            raise CyclicDependencyError(
                f"Cycle detected in DAG '{self.name}'. Ordered {len(ordered)}/{len(self.nodes)} nodes."
            )
        return ordered

    async def execute(
        self,
        run_id: str,
        initial_state: dict[str, Any] | None = None,
    ) -> DAGExecutionReport:
        """Execute DAG with ACID event sourcing. Resumes at the exact unresolved node after crash."""
        ordered_nodes = self.topological_sort()
        state: dict[str, Any] = dict(initial_state or {})

        completed_checkpoints = await self.event_store.get_completed_nodes(run_id)
        executed_now: list[str] = []
        resumed_nodes: list[str] = []
        total_cost = 0.0
        overall_start = time.perf_counter()

        for step_idx, node_id in enumerate(ordered_nodes, start=1):
            node = self.nodes[node_id]

            # Exact state replay if node already succeeded in a prior crashed worker
            if node_id in completed_checkpoints:
                checkpoint_ev = completed_checkpoints[node_id]
                state.update(checkpoint_ev.state_delta)
                resumed_nodes.append(node_id)
                total_cost += checkpoint_ev.cost_usd
                continue

            await self.event_store.append_event(
                ExecutionEvent(
                    run_id=run_id,
                    node_id=node_id,
                    status=NodeStatus.RUNNING,
                    step_index=step_idx,
                )
            )

            node_start = time.perf_counter()
            try:
                handler = self.handlers[node_id]
                delta = await handler(dict(state))
                node_latency_ms = (time.perf_counter() - node_start) * 1000.0
                node_cost = float(delta.get("_cost_usd", node.cost_estimate_usd))

                state.update(delta)
                executed_now.append(node_id)
                total_cost += node_cost

                await self.event_store.append_event(
                    ExecutionEvent(
                        run_id=run_id,
                        node_id=node_id,
                        status=NodeStatus.COMPLETED,
                        step_index=step_idx,
                        state_delta=delta,
                        cost_usd=node_cost,
                        latency_ms=round(node_latency_ms, 3),
                    )
                )
            except Exception as exc:
                node_latency_ms = (time.perf_counter() - node_start) * 1000.0
                await self.event_store.append_event(
                    ExecutionEvent(
                        run_id=run_id,
                        node_id=node_id,
                        status=NodeStatus.FAILED,
                        step_index=step_idx,
                        latency_ms=round(node_latency_ms, 3),
                        error_message=str(exc),
                    )
                )
                raise

        all_events = await self.event_store.get_events(run_id)
        total_latency_ms = (time.perf_counter() - overall_start) * 1000.0
        return DAGExecutionReport(
            run_id=run_id,
            final_state=state,
            executed_nodes=executed_now,
            resumed_from_checkpoint_nodes=resumed_nodes,
            total_cost_usd=round(total_cost, 8),
            total_latency_ms=round(total_latency_ms, 3),
            event_count=len(all_events),
        )
