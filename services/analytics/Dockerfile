# Build stage
FROM golang:1.26.6-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/analytics .

# Runtime stage
FROM alpine:3.24.1

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /bin/analytics /app/analytics

# Run as non-root.
RUN adduser -D -u 1000 hls && chown -R hls:hls /app
USER hls

EXPOSE 8080

ENV ANALYTICS_PORT=8080
ENV LOG_LEVEL=info

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD wget --no-verbose --tries=1 -O /dev/null http://localhost:8080/health || exit 1

ENTRYPOINT ["/app/analytics"]
