# Build context is the repository root (see docker-compose.yml), so the step can
# also copy the shared packages/prefect-log-handler library.
FROM python:3.15-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    DATA_DIR=/data

COPY --from=ghcr.io/astral-sh/uv:0.13.6 /uv /usr/local/bin/uv

# Mirror the repo layout so the relative path in pyproject.toml
# (../../packages/prefect-log-handler) still resolves inside the image.
WORKDIR /src/steps/transform

COPY steps/transform/pyproject.toml steps/transform/uv.lock ./
COPY packages/prefect-log-handler /src/packages/prefect-log-handler
RUN uv sync --locked --no-dev --no-install-project --no-editable

COPY steps/transform/main.py ./

ENV PATH="/src/steps/transform/.venv/bin:$PATH"

EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
