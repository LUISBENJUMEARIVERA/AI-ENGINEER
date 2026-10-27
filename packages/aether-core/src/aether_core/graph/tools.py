"""Built-in Sandboxed Tool Executors for the AETHER DAG State Machine."""

from __future__ import annotations

import ast
import sqlite3
from typing import Any


class SecuritySandboxViolationError(PermissionError):
    """Raised when a tool invocation attempts forbidden syscalls, imports, or destructive SQL."""


FORBIDDEN_AST_NODES = (
    ast.Import,
    ast.ImportFrom,
    ast.Global,
    ast.Nonlocal,
)

FORBIDDEN_BUILTINS = {"eval", "exec", "open", "compile", "__import__", "input", "breakpoint"}


def execute_python_sandbox(code: str, context: dict[str, Any] | None = None) -> dict[str, Any]:
    """Execute pure deterministic Python math/data transformations inside an AST-verified sandbox."""
    tree = ast.parse(code, mode="exec")
    for node in ast.walk(tree):
        if isinstance(node, FORBIDDEN_AST_NODES):
            raise SecuritySandboxViolationError(
                f"Forbidden AST construct '{type(node).__name__}' in Python sandbox."
            )
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            if node.func.id in FORBIDDEN_BUILTINS:
                raise SecuritySandboxViolationError(
                    f"Forbidden builtin call '{node.func.id}()' in Python sandbox."
                )
        if isinstance(node, ast.Attribute) and node.attr.startswith("__"):
            raise SecuritySandboxViolationError(
                f"Forbidden dunder attribute access '{node.attr}' in Python sandbox."
            )

    safe_builtins = {
        "abs": abs,
        "all": all,
        "any": any,
        "bool": bool,
        "dict": dict,
        "enumerate": enumerate,
        "filter": filter,
        "float": float,
        "int": int,
        "len": len,
        "list": list,
        "map": map,
        "max": max,
        "min": min,
        "pow": pow,
        "range": range,
        "round": round,
        "set": set,
        "sorted": sorted,
        "str": str,
        "sum": sum,
        "tuple": tuple,
        "zip": zip,
    }
    local_env: dict[str, Any] = dict(context or {})
    compiled = compile(tree, filename="<aether-sandbox>", mode="exec")
    exec(compiled, {"__builtins__": safe_builtins}, local_env)  # noqa: S102
    return {k: v for k, v in local_env.items() if not k.startswith("_")}


def execute_readonly_sql(
    query: str,
    seed_sql: str | None = None,
) -> list[dict[str, Any]]:
    """Execute read-only analytical SQL queries against an isolated in-memory database."""
    normalized = query.strip().upper()
    if not normalized.startswith("SELECT"):
        raise SecuritySandboxViolationError(
            "Only read-only SELECT statements are permitted in the SQL tool node."
        )
    forbidden_keywords = ("DROP ", "DELETE ", "UPDATE ", "INSERT ", "ALTER ", "ATTACH ", "PRAGMA ")
    for kw in forbidden_keywords:
        if kw in normalized:
            raise SecuritySandboxViolationError(
                f"Forbidden mutating SQL keyword '{kw.strip()}' detected."
            )

    with sqlite3.connect(":memory:") as conn:
        conn.row_factory = sqlite3.Row
        if seed_sql:
            conn.executescript(seed_sql)
        cursor = conn.execute(query)
        return [dict(row) for row in cursor.fetchall()]
