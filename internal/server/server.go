// Package server manages the HTTP servers for the S3 API and web console.
package server

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
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

	// Set temporary directory for auth hashing
	auth.TempDir = filepath.Join(cfg.DataDir, "tmp")
	if err := os.MkdirAll(auth.TempDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create temporary hashing directory: %w", err)
	}

	// Setup persistent JWT secret for Console session tracking
	jwtSecretEnv := os.Getenv("OBJECTRA_JWT_SECRET")
	var jwtSecret []byte
	if jwtSecretEnv != "" {
		jwtSecret = []byte(jwtSecretEnv)
	} else {
		storedSecret, err := engine.GetSystemValue("jwt_secret")
		if err == nil && storedSecret != "" {
			jwtSecret, _ = hex.DecodeString(storedSecret)
		}
		if len(jwtSecret) == 0 {
			jwtSecret = make([]byte, 32)
			if _, err := rand.Read(jwtSecret); err != nil {
				return nil, fmt.Errorf("failed to generate random JWT secret: %w", err)
			}
			_ = engine.PutSystemValue("jwt_secret", hex.EncodeToString(jwtSecret))
		}
	}
	console.SetJWTSecret(jwtSecret)

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
	consoleHandler := console.NewHandler(engine, creds, cfg.S3Port, cfg.Region, cfg.LoginRateLimit, cfg.APIRateLimit)
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
			slog.Warn("[Server] OBJECTRA_TLS_ENABLED=true but no cert/key files provided. Generating self-signed certificate in memory...")
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
	s3Listener, err := net.Listen("tcp", s.s3Server.Addr)
	if err != nil {
		return fmt.Errorf("failed to bind S3 API port %s: %w", s.s3Server.Addr, err)
	}

	consoleListener, err := net.Listen("tcp", s.consoleServer.Addr)
	if err != nil {
		s3Listener.Close()
		return fmt.Errorf("failed to bind Console port %s: %w", s.consoleServer.Addr, err)
	}

	go func() {
		if s.s3Server.TLSConfig != nil {
			slog.Info("[S3 API] Listening securely (HTTPS)", "addr", s.s3Server.Addr)
			if err := s.s3Server.ServeTLS(s3Listener, "", ""); err != nil && err != http.ErrServerClosed {
				slog.Error("[S3 API] Server error", "error", err)
			}
		} else {
			slog.Info("[S3 API] Listening", "addr", s.s3Server.Addr)
			if err := s.s3Server.Serve(s3Listener); err != nil && err != http.ErrServerClosed {
				slog.Error("[S3 API] Server error", "error", err)
			}
		}
	}()

	go func() {
		if s.consoleServer.TLSConfig != nil {
			slog.Info("[Console] Listening securely (HTTPS)", "addr", s.consoleServer.Addr)
			if err := s.consoleServer.ServeTLS(consoleListener, "", ""); err != nil && err != http.ErrServerClosed {
				slog.Error("[Console] Server error", "error", err)
			}
		} else {
			slog.Info("[Console] Listening", "addr", s.consoleServer.Addr)
			if err := s.consoleServer.Serve(consoleListener); err != nil && err != http.ErrServerClosed {
				slog.Error("[Console] Server error", "error", err)
			}
		}
	}()

	// Start background multipart cleanup task
	go func() {
		slog.Info("[Server] Starting background multipart cleanup worker")
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()

		if err := s.engine.CleanExpiredMultipartUploads(24 * time.Hour); err != nil {
			slog.Error("[Server] Error in initial multipart cleanup", "error", err)
		}
		if err := s.engine.CleanExpiredObjects(); err != nil {
			slog.Error("[Server] Error in initial lifecycle cleanup", "error", err)
		}

		for {
			select {
			case <-ticker.C:
				if err := s.engine.CleanExpiredMultipartUploads(24 * time.Hour); err != nil {
					slog.Error("[Server] Error cleaning expired multipart uploads", "error", err)
				}
				if err := s.engine.CleanExpiredObjects(); err != nil {
					slog.Error("[Server] Error cleaning expired objects", "error", err)
				}
			case <-s.cleanupStop:
				slog.Info("[Server] Background cleanup worker stopped")
				return
			}
		}
	}()

	return nil
}

// Shutdown gracefully shuts down both servers and stops background workers.
func (s *Server) Shutdown(ctx context.Context) error {
	slog.Info("[Server] Shutting down")

	// Stop background workers
	close(s.cleanupStop)

	var errs []error

	if err := s.s3Server.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Errorf("s3 API server shutdown error: %w", err))
	}

	if r, ok := s.s3Server.Handler.(*s3api.Router); ok {
		r.Close()
	}

	if err := s.consoleServer.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Errorf("console server shutdown error: %w", err))
	}

	if err := s.engine.Close(); err != nil {
		errs = append(errs, fmt.Errorf("storage engine close error: %w", err))
	}

	return errors.Join(errs...)
}
