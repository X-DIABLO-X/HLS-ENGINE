module hls-engine/gateway

go 1.26

require (
	github.com/go-chi/chi/v5 v5.3.1
	github.com/rs/zerolog v1.35.1
	hls-engine/internal v0.0.0
)

require (
	github.com/golang-jwt/jwt/v5 v5.3.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	golang.org/x/sys v0.29.0 // indirect
)

replace hls-engine/internal => ../internal
