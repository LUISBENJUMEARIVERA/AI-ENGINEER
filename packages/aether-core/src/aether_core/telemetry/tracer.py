"""OpenTelemetry-Compatible Distributed Tracing & Token Cost Auditing Engine."""

from __future__ import annotations

import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any


@dataclass
class TraceSpan:
    """W3C TraceContext compatible span for DAG nodes, Gateway calls, and Guardrail checks."""

    trace_id: str
    span_id: str
    parent_span_id: str | None
    operation_name: str
    start_time_ns: int = field(default_factory=time.time_ns)
    end_time_ns: int = 0
    attributes: dict[str, Any] = field(default_factory=dict)
    status: str = "OK"

    @property
    def duration_ms(self) -> float:
        if self.end_time_ns <= 0:
            return 0.0
        return round((self.end_time_ns - self.start_time_ns) / 1_000_000.0, 3)

    def to_otlp_dict(self) -> dict[str, Any]:
        return {
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "parent_span_id": self.parent_span_id,
            "operation_name": self.operation_name,
            "duration_ms": self.duration_ms,
            "status": self.status,
            "attributes": self.attributes,
        }


class AetherTracer:
    """Zero-overhead telemetry collector exporting OTLP/Jaeger/Langfuse compatible spans."""

    def __init__(self, service_name: str = "aether-control-plane") -> None:
        self.service_name = service_name
        self._spans: list[TraceSpan] = []

    @contextmanager
    def start_span(
        self,
        operation_name: str,
        *,
        trace_id: str | None = None,
        parent_span_id: str | None = None,
        attributes: dict[str, Any] | None = None,
    ) -> Iterator[TraceSpan]:
        span = TraceSpan(
            trace_id=trace_id or uuid.uuid4().hex,
            span_id=uuid.uuid4().hex[:16],
            parent_span_id=parent_span_id,
            operation_name=operation_name,
            attributes={"service.name": self.service_name, **(attributes or {})},
        )
        try:
            yield span
        except Exception as exc:
            span.status = "ERROR"
            span.attributes["error.type"] = type(exc).__name__
            span.attributes["error.message"] = str(exc)
            raise
        finally:
            span.end_time_ns = time.time_ns()
            self._spans.append(span)

    def export_spans(self) -> list[dict[str, Any]]:
        return [s.to_otlp_dict() for s in self._spans]

    def clear(self) -> None:
        self._spans.clear()
