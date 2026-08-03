FROM golang:1.26.5-alpine AS builder

WORKDIR /src
COPY go.mod main.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /test-origin .

FROM alpine:3.23.5
COPY --from=builder /test-origin /test-origin
USER 65534:65534
ENTRYPOINT ["/test-origin"]
