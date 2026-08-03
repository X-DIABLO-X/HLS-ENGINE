FROM golang:1.26.5-alpine AS builder

WORKDIR /build

RUN apk add --no-cache git ca-certificates

COPY services/internal /build/services/internal
COPY services/cdn-origin/go.mod services/cdn-origin/go.sum* /build/services/cdn-origin/
WORKDIR /build/services/cdn-origin
RUN go mod download

COPY services/cdn-origin /build/services/cdn-origin
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /cdn-origin .

FROM alpine:3.24.1

RUN apk add --no-cache ca-certificates tzdata wget

WORKDIR /app

COPY --from=builder /cdn-origin /app/cdn-origin

RUN adduser -D -u 1000 hls && chown -R hls:hls /app
USER hls

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

ENTRYPOINT ["/app/cdn-origin"]
