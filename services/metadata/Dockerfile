FROM golang:1.26.5-alpine AS builder

WORKDIR /build

RUN apk add --no-cache git ca-certificates

COPY services/internal /build/services/internal
COPY services/metadata/go.mod services/metadata/go.sum* /build/services/metadata/
WORKDIR /build/services/metadata
RUN go mod download

COPY services/metadata /build/services/metadata
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /metadata .

FROM alpine:3.23.5

RUN apk add --no-cache ca-certificates tzdata wget

WORKDIR /app

COPY --from=builder /metadata /app/metadata

RUN adduser -D -u 1000 hls && chown -R hls:hls /app
USER hls

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

ENTRYPOINT ["/app/metadata"]
