package proxy

import (
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
)

// Target describes an upstream service.
type Target struct {
	URL    *url.URL
	Proxy  *httputil.ReverseProxy
	Prefix string
}

// Registry maps route prefixes to upstream targets.
type Registry struct {
	upstreams map[string]*Target
}

func NewRegistry(targets map[string]string) (*Registry, error) {
	upstreams := make(map[string]*Target)
	for prefix, raw := range targets {
		u, err := url.Parse(raw)
		if err != nil {
			return nil, err
		}
		upstreams[prefix] = &Target{
			URL:    u,
			Proxy:  httputil.NewSingleHostReverseProxy(u),
			Prefix: prefix,
		}
	}
	return &Registry{upstreams: upstreams}, nil
}

func (r *Registry) Handler(w http.ResponseWriter, req *http.Request) {
	path := req.URL.Path
	for prefix, target := range r.upstreams {
		if strings.HasPrefix(path, prefix) {
			// Forward the full path to the upstream service.
			target.Proxy.ServeHTTP(w, req)
			return
		}
	}
	http.NotFound(w, req)
}

func (r *Registry) Add(prefix, raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return err
	}
	r.upstreams[prefix] = &Target{
		URL:    u,
		Proxy:  httputil.NewSingleHostReverseProxy(u),
		Prefix: prefix,
	}
	return nil
}

// ProxyTo returns a handler that forwards requests to the specified upstream prefix.
func (r *Registry) ProxyTo(prefix string) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		target, ok := r.upstreams[prefix]
		if !ok {
			http.NotFound(w, req)
			return
		}
		target.Proxy.ServeHTTP(w, req)
	}
}
