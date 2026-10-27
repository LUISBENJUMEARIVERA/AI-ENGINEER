.PHONY: install lint typecheck test evals bench dev clean

install:
	uv sync --extra dev

lint:
	uv run ruff check .

typecheck:
	uv run mypy packages/aether-core/src packages/aether-guardrails/src packages/aether-evals/src server/src

test:
	uv run pytest --cov=aether_core --cov=aether_guardrails --cov=aether_evals --cov=aether_server --cov-fail-under=90

evals:
	uv run python -m aether_evals.bench_runner

bench:
	uv run python -m aether_evals.bench_runner

dev:
	uv run uvicorn aether_server.app:app --reload --host 0.0.0.0 --port 8000
