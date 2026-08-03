package logger

import (
	"os"
	"strconv"
	"time"

	"github.com/rs/zerolog"
)

var L zerolog.Logger

func init() {
	L = New("info")
}

func New(level string) zerolog.Logger {
	l, err := zerolog.ParseLevel(level)
	if err != nil {
		l = zerolog.InfoLevel
	}
	zerolog.TimeFieldFormat = time.RFC3339Nano
	return zerolog.New(os.Stdout).
		With().
		Timestamp().
		Caller().
		Logger().
		Level(l)
}

func WithService(name string) zerolog.Logger {
	return L.With().Str("service", name).Logger()
}

func WithRequestID(id string) zerolog.Logger {
	return L.With().Str("request_id", id).Logger()
}

func Atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}
