# syntax=docker/dockerfile:1

# Multi-stage build: a "builder" installs the locked deps into a venv, then a
# slim "runtime" copies only that venv + the app code. Keeps the final image
# small and free of build tooling.

# ---- Builder: install locked runtime deps with uv ----
FROM python:3.11-slim-bookworm AS builder

# Grab the uv binary from its official image (pinned for reproducibility).
COPY --from=ghcr.io/astral-sh/uv:0.7.13 /uv /bin/uv

WORKDIR /app
ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PROJECT_ENVIRONMENT=/app/.venv

# Copy only the dependency manifests first so this layer is cached until the
# lockfile actually changes (fast rebuilds when only app code changes).
COPY pyproject.toml uv.lock ./

# --frozen              : install exactly what's in uv.lock (no re-resolve)
# --no-dev              : skip ruff/mypy/pytest — runtime doesn't need them
# --no-install-project  : this repo isn't a package (tool.uv package=false)
# --python-preference=only-system : use the base image's Python, not a managed
#   download (the host uses only-managed; in-container we want the system one).
RUN uv sync --frozen --no-dev --no-install-project --python-preference=only-system

# ---- Runtime: slim image with just the venv + source ----
FROM python:3.11-slim-bookworm AS runtime

WORKDIR /app
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# The venv's interpreter symlink points at /usr/local/bin/python3.11, which
# exists in this identical base image — so copying the venv "just works".
COPY --from=builder /app/.venv /app/.venv
COPY producer/ ./producer/

# Run as an unprivileged user (never run app containers as root).
RUN useradd --create-home --uid 10001 appuser \
    && chown -R appuser:appuser /app
USER appuser

CMD ["python", "-m", "producer.main"]
