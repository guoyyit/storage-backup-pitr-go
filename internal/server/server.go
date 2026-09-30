// Package server exposes the engine and backup service over HTTP.
package server

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/guoyyit/storage-backup-pitr-go/internal/backup"
	"github.com/guoyyit/storage-backup-pitr-go/internal/engine"
)

// Config configures the HTTP server.
type Config struct {
	Addr         string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// Server wires the HTTP API.
type Server struct {
	cfg     Config
	eng     *engine.Engine
	backups *backup.Service
	mux     *http.ServeMux
	srv     *http.Server
}

// New builds a server from an engine and backup service.
func New(cfg Config, eng *engine.Engine, backups *backup.Service) *Server {
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = 30 * time.Second
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 5 * time.Minute
	}
	s := &Server{cfg: cfg, eng: eng, backups: backups, mux: http.NewServeMux()}
	s.routes()
	s.srv = &http.Server{
		Addr:         cfg.Addr,
		Handler:      s.mux,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}
	return s
}

// Handler returns the HTTP handler.
func (s *Server) Handler() http.Handler { return s.mux }

// Run starts serving until the context is canceled, then shuts down.
func (s *Server) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		err := s.srv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = s.srv.Shutdown(shutdownCtx)
		return <-errCh
	case err := <-errCh:
		return err
	}
}
