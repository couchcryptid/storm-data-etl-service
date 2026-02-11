package httpadapter

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	sharedobs "github.com/couchcryptid/storm-data-shared/observability"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server exposes health, readiness, and metrics HTTP endpoints.
type Server struct {
	httpServer *http.Server
	logger     *slog.Logger
}

// NewServer creates an HTTP server with /healthz, /readyz, and /metrics routes.
func NewServer(addr string, ready sharedobs.ReadinessChecker, logger *slog.Logger) *Server {
	mux := http.NewServeMux()

	s := &Server{
		httpServer: &http.Server{
			Addr:         addr,
			Handler:      mux,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 10 * time.Second,
			IdleTimeout:  60 * time.Second,
		},
		logger: logger,
	}

	mux.HandleFunc("GET /healthz", sharedobs.LivenessHandler())
	mux.HandleFunc("GET /readyz", sharedobs.ReadinessHandler(ready))
	mux.Handle("GET /metrics", promhttp.Handler())

	return s
}

// Start begins listening. Returns http.ErrServerClosed on graceful shutdown.
func (s *Server) Start() error {
	s.logger.Info("http server starting", "addr", s.httpServer.Addr)
	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully drains connections within the given context deadline.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

// ServeHTTP delegates to the underlying handler, useful for testing.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.httpServer.Handler.ServeHTTP(w, r)
}
