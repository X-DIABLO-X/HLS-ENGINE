FROM golang:1.26.5-alpine AS builder

WORKDIR /src
COPY go.mod ./
COPY *.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /cache-purger .

FROM nginx:1.31.3-alpine

COPY --from=builder /cache-purger /app/cache-purger

ENV HTTP_ADDR=:8080 \
    NGINX_CACHE_ROOT=/var/cache/nginx/hls \
    NGINX_CACHE_TOMBSTONE_ROOT=/var/cache/nginx/tombstones

# The short root entrypoint only creates the two bounded directories. The
# service itself, including its HTTP listener and every cache operation, runs
# as the same unprivileged uid as the nginx workers.
ENTRYPOINT ["sh", "-c", "mkdir -p \"$NGINX_CACHE_ROOT\" \"$NGINX_CACHE_TOMBSTONE_ROOT\" && chown -R nginx:nginx \"$NGINX_CACHE_ROOT\" \"$NGINX_CACHE_TOMBSTONE_ROOT\" && exec su nginx -s /bin/sh -c '/app/cache-purger'"]
