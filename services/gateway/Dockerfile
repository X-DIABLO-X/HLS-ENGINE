# Build stage
FROM golang:1.26.6-alpine AS builder

WORKDIR /build

# Install dependencies
RUN apk add --no-cache git ca-certificates

# Copy shared internal module first
COPY services/internal /build/services/internal
COPY services/gateway/go.mod services/gateway/go.sum* /build/services/gateway/
WORKDIR /build/services/gateway
RUN go mod download

COPY services/gateway /build/services/gateway
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /gateway .

# Runtime stage
FROM alpine:3.24.1

RUN apk add --no-cache ca-certificates tzdata wget

WORKDIR /app

COPY --from=builder /gateway /app/gateway

RUN adduser -D -u 1000 hls && chown -R hls:hls /app
USER hls

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

ENTRYPOINT ["/app/gateway"]
