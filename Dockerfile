FROM ghcr.io/astral-sh/uv:0.12.5 AS uv
FROM python:3.12-slim-bookworm

COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /app
ENV UV_PYTHON_DOWNLOADS=never \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/app/.venv/bin:$PATH"

COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project \
    && groupadd --system --gid 10001 billing \
    && useradd --system --uid 10001 --gid 10001 --no-create-home billing

COPY billing ./billing
COPY alembic.ini ./
COPY migrations ./migrations
USER 10001:10001
