# syntax=docker/dockerfile:1
FROM python:3.11.15-slim

# Install FFmpeg (with libnvidia-encode available via LD_LIBRARY_PATH mount).
# WSL2's NVIDIA user-space libs are mounted at /usr/lib/wsl/lib — see compose volumes.
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg wget && \
    rm -rf /var/lib/apt/lists/*

# Point the dynamic linker at WSL2 NVIDIA libs so ffmpeg can dlopen them at runtime
ENV LD_LIBRARY_PATH="/usr/lib/wsl/lib"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY celery-worker-start.sh .
RUN chmod +x celery-worker-start.sh

EXPOSE 8080

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
