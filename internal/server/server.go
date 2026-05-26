// Package server manages the HTTP servers for the S3 API and web console.
package server

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/salvatorecorvaglia/objectra/internal/auth"
	"github.com/salvatorecorvaglia/objectra/internal/config"
	"github.com/salvatorecorvaglia/objectra/internal/console"
	"github.com/salvatorecorvaglia/objectra/internal/s3api"
	"github.com/salvatorecorvaglia/objectra/internal/storage"
)

// Server holds both the S3 API server and the web console server.
type Server struct {
	s3Server      *http.Server
	consoleServer *http.Server
	engine        storage.Engine
}

// New creates and configures both servers.
func New(cfg *config.Config) (*Server, error) {
	// Initialize storage engine
	engine, err := storage.NewFilesystemEngine(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize storage: %w", err)
	}

	creds := auth.NewCredentials(cfg.AccessKey, cfg.SecretKey)

	// S3 API server
	s3Router := s3api.NewRouter(engine, creds, cfg.Region)
	s3Server := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.S3Port),
		Handler:           s3Router,
		ReadHeaderTimeout: 30 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20, // 1MB
	}

	// Web console server
	consoleHandler := console.NewHandler(engine, creds)
	consoleServer := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.ConsolePort),
		Handler:           consoleHandler,
		ReadHeaderTimeout: 30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	return &Server{
		s3Server:      s3Server,
		consoleServer: consoleServer,
		engine:        engine,
	}, nil
}

// Start launches both servers concurrently.
func (s *Server) Start() error {
	errCh := make(chan error, 2)

	go func() {
		log.Printf("[S3 API] Listening on %s", s.s3Server.Addr)
		if err := s.s3Server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- fmt.Errorf("S3 API server: %w", err)
		}
	}()

	go func() {
		log.Printf("[Console] Listening on %s", s.consoleServer.Addr)
		if err := s.consoleServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- fmt.Errorf("Console server: %w", err)
		}
	}()

	return <-errCh
}

// Shutdown gracefully shuts down both servers.
func (s *Server) Shutdown(ctx context.Context) error {
	log.Println("[Server] Shutting down...")

	var firstErr error

	if err := s.s3Server.Shutdown(ctx); err != nil && firstErr == nil {
		firstErr = err
	}

	if err := s.consoleServer.Shutdown(ctx); err != nil && firstErr == nil {
		firstErr = err
	}

	if err := s.engine.Close(); err != nil && firstErr == nil {
		firstErr = err
	}

	return firstErr
}
