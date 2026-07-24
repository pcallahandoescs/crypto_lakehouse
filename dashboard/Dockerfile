# syntax=docker/dockerfile:1

# Multi-stage build (same pattern as producer/serving): a "builder" installs the
# locked dashboard deps into a venv, then a slim "runtime" copies only that venv
# + the app code.

# ---- Builder: install locked dashboard deps with uv ----
FROM python:3.11-slim-bookworm AS builder

COPY --from=ghcr.io/astral-sh/uv:0.7.13 /uv /bin/uv

WORKDIR /app
ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PROJECT_ENVIRONMENT=/app/.venv

COPY pyproject.toml uv.lock ./

RUN uv sync --frozen --no-dev --group dashboard --no-install-project \
    --python-preference=only-system

# ---- Runtime: slim image with just the venv + source ----
FROM python:3.11-slim-bookworm AS runtime

WORKDIR /app
# `streamlit run dashboard/app.py` puts the script's folder (/app/dashboard) on
# sys.path, not /app — so PYTHONPATH=/app is what lets `import dashboard.client`
# (the absolute import used everywhere) resolve inside the container.
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false \
    STREAMLIT_SERVER_HEADLESS=true

COPY --from=builder /app/.venv /app/.venv
COPY dashboard/ ./dashboard/

# Run as an unprivileged user (never run app containers as root).
RUN useradd --create-home --uid 10001 appuser \
    && chown -R appuser:appuser /app
USER appuser

EXPOSE 8501
CMD ["streamlit", "run", "dashboard/app.py", \
     "--server.port", "8501", "--server.address", "0.0.0.0"]
