module hls-engine/gateway

go 1.26

require (
	github.com/go-chi/chi/v5 v5.1.0
	github.com/rs/zerolog v1.33.0
	hls-engine/internal v0.0.0
)

require (
	github.com/golang-jwt/jwt/v5 v5.2.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	golang.org/x/sys v0.22.0 // indirect
)

replace hls-engine/internal => ../internal
