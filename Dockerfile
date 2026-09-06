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

# The image is a deployment artefact, so it defaults to production and keeps
# Uvicorn's autoreloader off. src/config.py still defaults to development for
# someone running the code directly, outside a container. Declared after the
# dependency install so changing it cannot invalidate the cached pip layer.
ENV APP_ENV=production

# 8000 = FastAPI, 8501 = Streamlit. Which one is used depends on the command.
EXPOSE 8000 8501

# Default role is the API. docker-compose overrides this for the other two.
#
# run_api.py rather than a hardcoded `uvicorn --port 8000`: it reads HOST and
# PORT from the environment, so a platform that assigns a port at runtime is
# served on the port it actually routes to, while still defaulting to 8000
# when PORT is unset.
CMD ["python", "run_api.py"]
