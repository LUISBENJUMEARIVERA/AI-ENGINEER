"""Real-Time Streaming Guardrails: Prompt Injection, PII Exfiltration, and Toxicity Interception."""

from __future__ import annotations

import re
from collections.abc import AsyncIterator
from enum import StrEnum

from pydantic import BaseModel, Field


class GuardrailCategory(StrEnum):
    """Security classification of intercepted streaming violations."""

    PROMPT_INJECTION = "PROMPT_INJECTION"
    PII_LEAKAGE = "PII_LEAKAGE"
    SECRET_EXFILTRATION = "SECRET_EXFILTRATION"
    TOXICITY_POLICY = "TOXICITY_POLICY"


class GuardrailViolationError(RuntimeError):
    """Raised immediately when an inbound prompt or outbound token stream violates security policy."""

    def __init__(self, category: GuardrailCategory, rule_id: str, snippet: str) -> None:
        self.category = category
        self.rule_id = rule_id
        self.snippet = snippet
        super().__init__(f"[GUARDRAIL_TRIPPED:{category}:{rule_id}] Blocked unsafe content.")


class GuardrailAuditEvent(BaseModel):
    """Structured security telemetry emitted when a stream is inspected or halted."""

    passed: bool
    category: GuardrailCategory | None = None
    rule_id: str | None = None
    inspected_tokens: int = 0
    redacted_preview: str = Field(default="")


PROMPT_INJECTION_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    (
        "PI_IGNORE_SYSTEM",
        re.compile(
            r"ignore\s+(all\s+)?(previous|prior|above|system)\s+(instructions|prompts|rules)",
            re.IGNORECASE,
        ),
    ),
    (
        "PI_DEVELOPER_OVERRIDE",
        re.compile(
            r"(system\s+override|developer\s+mode\s+enabled|reveal\s+your\s+system\s+prompt|jailbreak)",
            re.IGNORECASE,
        ),
    ),
    (
        "PI_SQL_CMD_INJECTION",
        re.compile(r"(;\s*DROP\s+TABLE|UNION\s+SELECT\s+NULL|/etc/passwd|rm\s+-rf\s+/)", re.IGNORECASE),
    ),
]

PII_AND_SECRET_PATTERNS: list[tuple[GuardrailCategory, str, re.Pattern[str]]] = [
    (
        GuardrailCategory.PII_LEAKAGE,
        "PII_US_SSN",
        re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
    ),
    (
        GuardrailCategory.PII_LEAKAGE,
        "PII_CREDIT_CARD",
        re.compile(r"\b(?:\d[ -]*?){13,16}\b"),
    ),
    (
        GuardrailCategory.SECRET_EXFILTRATION,
        "SEC_API_PRIVATE_KEY",
        re.compile(r"\b(?:sk-[a-zA-Z0-9]{16,}|AIza[0-9A-Za-z\-_]{20,}|AKIA[0-9A-Z]{16})\b"),
    ),
]

TOXICITY_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    (
        "TOX_MALWARE_SYNTHESIS",
        re.compile(
            r"(synthesize\s+ransomware\s+payload|exploit\s+zero-day\s+shellcode|build\s+a\s+botnet)",
            re.IGNORECASE,
        ),
    ),
]


class StreamingGuardrailEngine:
    """Zero-allocation rolling window token inspector for SSE and WebSocket LLM streams."""

    def __init__(self, rolling_window_chars: int = 256) -> None:
        self.rolling_window_chars = rolling_window_chars

    def validate_inbound_prompt(self, prompt: str) -> GuardrailAuditEvent:
        """Inspect user input before dispatching to any LLM provider."""
        for rule_id, pattern in PROMPT_INJECTION_PATTERNS:
            match = pattern.search(prompt)
            if match:
                raise GuardrailViolationError(
                    GuardrailCategory.PROMPT_INJECTION,
                    rule_id,
                    match.group(0)[:32],
                )
        for rule_id, pattern in TOXICITY_PATTERNS:
            match = pattern.search(prompt)
            if match:
                raise GuardrailViolationError(
                    GuardrailCategory.TOXICITY_POLICY,
                    rule_id,
                    match.group(0)[:32],
                )
        return GuardrailAuditEvent(passed=True, inspected_tokens=len(prompt.split()))

    def inspect_rolling_buffer(self, buffer_text: str) -> None:
        """Inspect rolling stream buffer for multi-token PII, secrets, or injection payloads."""
        for category, rule_id, pattern in PII_AND_SECRET_PATTERNS:
            match = pattern.search(buffer_text)
            if match:
                raise GuardrailViolationError(category, rule_id, "[REDACTED_PII_OR_SECRET]")

        for rule_id, pattern in PROMPT_INJECTION_PATTERNS:
            match = pattern.search(buffer_text)
            if match:
                raise GuardrailViolationError(
                    GuardrailCategory.PROMPT_INJECTION,
                    rule_id,
                    match.group(0)[:32],
                )

        for rule_id, pattern in TOXICITY_PATTERNS:
            match = pattern.search(buffer_text)
            if match:
                raise GuardrailViolationError(
                    GuardrailCategory.TOXICITY_POLICY,
                    rule_id,
                    match.group(0)[:32],
                )

    async def guard_stream(self, token_stream: AsyncIterator[str]) -> AsyncIterator[str]:
        """Wrap an async token generator and abort immediately if a policy violation emerges mid-stream."""
        rolling_window = ""
        try:
            async for token in token_stream:
                rolling_window = (rolling_window + token)[-self.rolling_window_chars :]
                self.inspect_rolling_buffer(rolling_window)
                yield token
        finally:
            aclose = getattr(token_stream, "aclose", None)
            if callable(aclose):
                await aclose()
