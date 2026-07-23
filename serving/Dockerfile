# syntax=docker/dockerfile:1

# Multi-stage build (same pattern as producer/Dockerfile): a "builder" installs
# the locked serving deps into a venv, then a slim "runtime" copies only that
# venv + the app code. No JVM anywhere — delta-rs reads Delta from MinIO natively.

# ---- Builder: install locked serving deps with uv ----
FROM python:3.11-slim-bookworm AS builder

COPY --from=ghcr.io/astral-sh/uv:0.7.13 /uv /bin/uv

WORKDIR /app
ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PROJECT_ENVIRONMENT=/app/.venv

COPY pyproject.toml uv.lock ./

# --no-dev              : skip ruff/mypy/pytest (dev tooling)
# --group serving       : add the serving runtime deps (fastapi/uvicorn/delta-rs)
# --no-install-project  : this repo isn't a package (tool.uv package=false)
RUN uv sync --frozen --no-dev --group serving --no-install-project \
    --python-preference=only-system

# ---- Runtime: slim image with just the venv + source ----
FROM python:3.11-slim-bookworm AS runtime

WORKDIR /app
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

COPY --from=builder /app/.venv /app/.venv
COPY serving/ ./serving/

# Run as an unprivileged user (never run app containers as root).
RUN useradd --create-home --uid 10001 appuser \
    && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000
CMD ["uvicorn", "serving.main:app", "--host", "0.0.0.0", "--port", "8000"]
