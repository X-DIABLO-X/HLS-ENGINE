# syntax=docker/dockerfile:1

ARG BASE_IMAGE=python:3.11.15-slim@sha256:db3ff2e1800a8581e2c48a27c3995339d47bdf046da21c7627accd3d51053a93
FROM ${BASE_IMAGE}

LABEL org.opencontainers.image.source="https://github.com/X-DIABLO-X/HLS-ENGINE" \
      org.opencontainers.image.licenses="GPL-2.0-or-later"

# Install FFmpeg (with libnvidia-encode available via LD_LIBRARY_PATH mount).
# WSL2's NVIDIA user-space libs are mounted at /usr/lib/wsl/lib — see compose volumes.
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg wget && \
    rm -rf /var/lib/apt/lists/*

# Point the dynamic linker at WSL2 NVIDIA libs so ffmpeg can dlopen them at runtime
ENV LD_LIBRARY_PATH="/usr/lib/wsl/lib"

COPY THIRD_PARTY_NOTICES.md /usr/share/doc/hls-engine/THIRD_PARTY_NOTICES.md

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY celery-worker-start.sh .
RUN chmod +x celery-worker-start.sh

EXPOSE 8080

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
