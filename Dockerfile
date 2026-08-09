# One image for all four services. Compose/k8s pick the entrypoint via `command`.
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Dependency layer first — cached until a manifest or the lockfile changes.
COPY pyproject.toml uv.lock ./
COPY packages/argus/pyproject.toml packages/argus/
COPY services/chat_app/pyproject.toml services/chat_app/
COPY services/ingestion/pyproject.toml services/ingestion/
COPY services/worker/pyproject.toml services/worker/
COPY services/dashboard/pyproject.toml services/dashboard/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --all-packages --no-install-workspace

# Source layer — changes on every edit, so it comes last.
COPY . .
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --all-packages

ENV PATH="/app/.venv/bin:$PATH"
