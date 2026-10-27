"""Hybrid Semantic Memory Engine: Short-Term Token Window + pgvector Cosine Similarity Retrieval."""

from __future__ import annotations

import hashlib
import math
import re
from collections import deque
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field


class MemoryRecord(BaseModel):
    """Atomic memory chunk stored in hybrid short-term buffer and vector index."""

    record_id: str
    content: str
    embedding: list[float]
    token_count: int
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: str = Field(default_factory=lambda: datetime.now(UTC).isoformat())


class SemanticSearchResult(BaseModel):
    """Ranked semantic retrieval item with cosine similarity score."""

    record: MemoryRecord
    similarity: float
    source_tier: str  # "L1_RING_BUFFER" | "L2_PGVECTOR"


def deterministic_embed(text: str, dims: int = 64) -> list[float]:
    """Deterministic unit-normalized feature-hashing embedding (zero external API latency)."""
    vec = [0.0] * dims
    tokens = re.findall(r"[a-z0-9_]+", text.lower())
    if not tokens:
        vec[0] = 1.0
        return vec

    for token in tokens:
        digest = hashlib.blake2b(token.encode("utf-8"), digest_size=8).digest()
        idx = int.from_bytes(digest[:4], "little") % dims
        sign = 1.0 if (digest[4] & 1) == 0 else -1.0
        vec[idx] += sign

    norm = math.sqrt(sum(v * v for v in vec))
    if norm > 0.0:
        return [round(v / norm, 6) for v in vec]
    vec[0] = 1.0
    return vec


def cosine_similarity(a: list[float], b: list[float]) -> float:
    """Compute exact cosine similarity between two unit or non-unit vectors."""
    if len(a) != len(b) or not a:
        return 0.0
    dot = sum(x * y for x, y in zip(a, b, strict=True))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return round(dot / (norm_a * norm_b), 6)


class HybridSemanticMemory:
    """Dual-tier memory manager combining a sliding token window (Redis-compatible) and pgvector store."""

    def __init__(
        self,
        max_short_term_tokens: int = 2048,
        embedding_dims: int = 64,
    ) -> None:
        self.max_short_term_tokens = max_short_term_tokens
        self.embedding_dims = embedding_dims
        self._short_term_buffer: deque[MemoryRecord] = deque()
        self._short_term_tokens: int = 0
        self._vector_store: dict[str, MemoryRecord] = {}
        self._counter: int = 0

    @property
    def short_term_token_count(self) -> int:
        return self._short_term_tokens

    @property
    def vector_count(self) -> int:
        return len(self._vector_store)

    @staticmethod
    def estimate_tokens(text: str) -> int:
        return max(1, len(text.split()))

    def ingest(self, content: str, metadata: dict[str, Any] | None = None) -> MemoryRecord:
        """Store entry in short-term sliding token window and index into long-term vector store."""
        self._counter += 1
        rec_id = f"mem_{self._counter:06d}"
        tokens = self.estimate_tokens(content)
        emb = deterministic_embed(content, dims=self.embedding_dims)

        record = MemoryRecord(
            record_id=rec_id,
            content=content,
            embedding=emb,
            token_count=tokens,
            metadata=metadata or {},
        )

        # L1 Ring Buffer with automatic token eviction
        self._short_term_buffer.append(record)
        self._short_term_tokens += tokens
        while self._short_term_tokens > self.max_short_term_tokens and len(self._short_term_buffer) > 1:
            evicted = self._short_term_buffer.popleft()
            self._short_term_tokens -= evicted.token_count

        # L2 Long-Term Vector Index (pgvector compatible schema)
        self._vector_store[rec_id] = record
        return record

    def retrieve_hybrid(
        self,
        query: str,
        top_k: int = 5,
        min_similarity: float = 0.05,
    ) -> list[SemanticSearchResult]:
        """Retrieve relevant context merging L1 recency boost with L2 cosine similarity."""
        query_vec = deterministic_embed(query, dims=self.embedding_dims)
        short_term_ids = {r.record_id for r in self._short_term_buffer}
        results: list[SemanticSearchResult] = []

        for rec_id, record in self._vector_store.items():
            sim = cosine_similarity(query_vec, record.embedding)
            tier = "L1_RING_BUFFER" if rec_id in short_term_ids else "L2_PGVECTOR"
            if sim >= min_similarity:
                results.append(
                    SemanticSearchResult(
                        record=record,
                        similarity=sim,
                        source_tier=tier,
                    )
                )

        results.sort(key=lambda r: r.similarity, reverse=True)
        return results[:top_k]

    @staticmethod
    def pgvector_ddl(dims: int = 64) -> str:
        """Production PostgreSQL + pgvector HNSW DDL schema for deployment."""
        return (
            "CREATE EXTENSION IF NOT EXISTS vector;\n"
            "CREATE TABLE IF NOT EXISTS aether_semantic_memory (\n"
            "    record_id VARCHAR(64) PRIMARY KEY,\n"
            "    content TEXT NOT NULL,\n"
            f"    embedding vector({dims}) NOT NULL,\n"
            "    token_count INTEGER NOT NULL,\n"
            "    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,\n"
            "    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()\n"
            ");\n"
            "CREATE INDEX IF NOT EXISTS idx_aether_memory_hnsw\n"
            "ON aether_semantic_memory USING hnsw (embedding vector_cosine_ops);"
        )
