// Package server manages the HTTP servers for the S3 API and web console.
package server

import (
	"context"
	"crypto/tls"
	"errors"
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

// Server holds both the S3 API server and the web console server, plus background workers.
type Server struct {
	s3Server      *http.Server
	consoleServer *http.Server
	engine        storage.Engine
	cleanupStop   chan struct{}
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
	s3Router := s3api.NewRouter(engine, creds, cfg.Region, cfg.Domain)
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

	// Setup TLS if enabled
	if cfg.TLSEnabled {
		var cert tls.Certificate
		if cfg.TLSCert != "" && cfg.TLSKey != "" {
			cert, err = tls.LoadX509KeyPair(cfg.TLSCert, cfg.TLSKey)
			if err != nil {
				return nil, fmt.Errorf("failed to load TLS certificate and key: %w", err)
			}
		} else {
			log.Println("[Server] OBJECTRA_TLS_ENABLED=true but no cert/key files provided. Generating self-signed certificate in memory...")
			cert, err = GenerateSelfSignedCert()
			if err != nil {
				return nil, fmt.Errorf("failed to generate self-signed certificate: %w", err)
			}
		}

		tlsCfg := &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}

		s3Server.TLSConfig = tlsCfg
		consoleServer.TLSConfig = tlsCfg
	}

	return &Server{
		s3Server:      s3Server,
		consoleServer: consoleServer,
		engine:        engine,
		cleanupStop:   make(chan struct{}),
	}, nil
}

// Start launches both servers concurrently and starts background workers.
func (s *Server) Start() error {
	errCh := make(chan error, 2)

	go func() {
		if s.s3Server.TLSConfig != nil {
			log.Printf("[S3 API] Listening securely (HTTPS) on %s", s.s3Server.Addr)
			if err := s.s3Server.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
				errCh <- fmt.Errorf("S3 API server (HTTPS): %w", err)
			}
		} else {
			log.Printf("[S3 API] Listening on %s", s.s3Server.Addr)
			if err := s.s3Server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				errCh <- fmt.Errorf("S3 API server: %w", err)
			}
		}
	}()

	go func() {
		if s.consoleServer.TLSConfig != nil {
			log.Printf("[Console] Listening securely (HTTPS) on %s", s.consoleServer.Addr)
			if err := s.consoleServer.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
				errCh <- fmt.Errorf("Console server (HTTPS): %w", err)
			}
		} else {
			log.Printf("[Console] Listening on %s", s.consoleServer.Addr)
			if err := s.consoleServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				errCh <- fmt.Errorf("Console server: %w", err)
			}
		}
	}()

	// Start background multipart cleanup task
	go func() {
		log.Println("[Server] Starting background multipart cleanup worker...")
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()

		// Run once on startup
		if err := s.engine.CleanExpiredMultipartUploads(24 * time.Hour); err != nil {
			log.Printf("[Server] Error in initial multipart cleanup: %v", err)
		}

		for {
			select {
			case <-ticker.C:
				if err := s.engine.CleanExpiredMultipartUploads(24 * time.Hour); err != nil {
					log.Printf("[Server] Error cleaning expired multipart uploads: %v", err)
				}
			case <-s.cleanupStop:
				log.Println("[Server] Background multipart cleanup worker stopped.")
				return
			}
		}
	}()

	return <-errCh
}

// Shutdown gracefully shuts down both servers and stops background workers.
func (s *Server) Shutdown(ctx context.Context) error {
	log.Println("[Server] Shutting down...")

	// Stop background workers
	close(s.cleanupStop)

	var errs []error

	if err := s.s3Server.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Errorf("S3 API server shutdown error: %w", err))
	}

	if err := s.consoleServer.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Errorf("Console server shutdown error: %w", err))
	}

	if err := s.engine.Close(); err != nil {
		errs = append(errs, fmt.Errorf("Storage engine close error: %w", err))
	}

	return errors.Join(errs...)
}
