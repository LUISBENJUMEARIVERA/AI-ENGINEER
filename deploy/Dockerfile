FROM python:3.12-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir uv

COPY pyproject.toml README.md ./
COPY packages ./packages
COPY server ./server

RUN uv pip install --system --no-cache .

EXPOSE 8000
CMD ["uvicorn", "aether_server.app:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]
