// Package webui serves the recruiter-facing project console.
package webui

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
)

//go:embed static/*
var files embed.FS

// New combines the static console with the existing API handler.
func New(api http.Handler) http.Handler {
	assets, err := fs.Sub(files, "static")
	if err != nil {
		panic(err)
	}
	static := http.NewServeMux()
	static.Handle("GET /assets/", http.StripPrefix("/assets/", cacheAssets(http.FileServer(http.FS(assets)))))
	static.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		http.ServeFileFS(w, r, assets, "index.html")
	})
	dispatch := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/v1/") || r.URL.Path == "/healthz" || r.URL.Path == "/metrics" {
			api.ServeHTTP(w, r)
			return
		}
		static.ServeHTTP(w, r)
	})
	return securityHeaders(dispatch)
}

func cacheAssets(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=3600")
		next.ServeHTTP(w, r)
	})
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Security-Policy", "default-src 'self'; style-src 'self'; script-src 'self'; connect-src 'self'; img-src 'self' data:; font-src 'self'; base-uri 'none'; frame-ancestors 'none'")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		next.ServeHTTP(w, r)
	})
}
