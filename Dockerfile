# GTI Route Planner — solver service (Render)
#
# OR-Tools ships pre-built wheels for cp312 + manylinux on PyPI, so we
# don't need build-essential or any C++ toolchain on the image.

FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install Python deps first so layer cache survives source edits.
COPY pyproject.toml ./
RUN pip install --upgrade pip && pip install \
      "fastapi>=0.115" \
      "uvicorn[standard]>=0.32" \
      "pydantic>=2.10" \
      "ortools>=9.11" \
      "httpx>=0.28"

# App code
COPY src/ ./src/

# Render injects $PORT — defaults to 10000 if running locally.
ENV PORT=10000
EXPOSE 10000

# Single uvicorn worker is plenty for our scale (≤4 engineers × ≤16 jobs).
# `--proxy-headers` needed so FastAPI sees the real client IP behind
# Render's load balancer.
CMD uvicorn solver.main:app \
      --app-dir src \
      --host 0.0.0.0 \
      --port ${PORT} \
      --proxy-headers
