package main

import (
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	var slowStarted atomic.Bool
	releaseSlow := make(chan struct{})
	var releaseOnce sync.Once

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "healthy\n")
	})
	mux.HandleFunc("GET /control/started", func(w http.ResponseWriter, _ *http.Request) {
		if !slowStarted.Load() {
			http.Error(w, "not started", http.StatusTooEarly)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /control/release", func(w http.ResponseWriter, _ *http.Request) {
		releaseOnce.Do(func() { close(releaseSlow) })
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /hls/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/slow.m4s") {
			slowStarted.Store(true)
			select {
			case <-releaseSlow:
			case <-r.Context().Done():
				return
			}
		}
		w.Header().Set("Content-Type", "video/mp4")
		w.Header().Set("Content-Length", "12")
		_, _ = io.WriteString(w, "media-bytes!")
	})

	server := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	log.Fatal(server.ListenAndServe())
}
