# syntax=docker/dockerfile:1
#
# One image serves all three DataTrust application roles - the FastAPI service,
# the Streamlit dashboard, and the one-shot table initialiser. They differ only
# by the command docker-compose gives them, so there is a single build to keep
# in sync and a single set of pinned dependencies.

FROM python:3.13-slim

# PYTHONUNBUFFERED keeps container logs in real time; PYTHONPATH lets
# `src.` and `dashboard.` imports resolve the same way they do locally.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app

WORKDIR /app

# Dependencies are installed before the source is copied so that editing code
# does not invalidate the (slow) pip layer. Versions come from the project's
# own pinned requirements.txt - nothing is upgraded here.
COPY requirements.txt ./
RUN python -m pip install --upgrade pip \
    && python -m pip install -r requirements.txt

COPY . .

# Run as an unprivileged user. /app is owned by it so the bind-mounted
# logs/ and reports/ directories stay writable.
RUN useradd --create-home --uid 10001 datatrust \
    && chown -R datatrust:datatrust /app
USER datatrust

# 8000 = FastAPI, 8501 = Streamlit. Which one is used depends on the command.
EXPOSE 8000 8501

# Default role is the API. docker-compose overrides this for the other two.
CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
