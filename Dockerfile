# Stralis runtime image.
#
# Base tag note: pyproject declares requires-python = ">=3.11", and 3.12-slim
# is a safe, widely mirrored choice today. python:3.14-slim is also fine once
# official GA images are broadly available -- just swap the tag below; the
# package targets >=3.11 and runs on 3.14.
FROM python:3.12-slim

# Never prompt, never keep a pip cache, keep stdout unbuffered for log sinks.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# --- Media toolkit layer ---------------------------------------------------
# The slim base image does NOT ship ffmpeg. Workers that execute the
# `ffmpeg.transcode` handler (and any `shell` node that shells out to
# ffmpeg/ffprobe) need the binary present. Installing it here bakes ~300MB
# into the image; drop this layer and slim the image further if your pipelines
# are pure-Python/shell and never touch ffmpeg.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# --- Python dependencies ---------------------------------------------------
# Copy project metadata first so the (expensive) dependency layer caches
# across source-only changes. The package uses setuptools; the layout is not
# editable-only at build time, so `pip install ".[postgres]"` installs a real
# copy of stralis. The `postgres` extra pulls in psycopg for Postgres DSNs;
# SQLite works with no extra.
COPY pyproject.toml README.md ./
COPY stralis ./stralis
RUN pip install --no-cache-dir ".[postgres]"

# --- Non-root runtime ------------------------------------------------------
# Create an unprivileged user to own the app and the worker scratch volume.
RUN useradd --create-home --shell /bin/bash stralis \
    && mkdir -p /data/work \
    && chown -R stralis:stralis /app /data

USER stralis

# No CMD/ENTRYPOINT baked in: docker-compose supplies the per-service command
# (`api` runs uvicorn, `orchestrator`/`worker` run the `stralis` console
# script). This keeps the image reusable across all three roles.
