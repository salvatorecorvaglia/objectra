package server

import (
	"context"
	"net"
	"strconv"
	"testing"

	"github.com/salvatorecorvaglia/stiva/internal/config"
)

func TestServerStartSuccessAndShutdown(t *testing.T) {
	tempDir := t.TempDir()
	cfg := &config.Config{
		AccessKey:   "testkey",
		SecretKey:   "testsecret",
		DataDir:     tempDir,
		S3Port:      0, // OS will assign an ephemeral port
		ConsolePort: 0, // OS will assign an ephemeral port
		Region:      "us-east-1",
	}

	srv, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	if err := srv.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}

	// Clean up and shutdown
	if err := srv.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown returned error: %v", err)
	}
}

func TestServerStartPortConflict(t *testing.T) {
	tempDir := t.TempDir()

	// Bind a port manually to simulate conflict on the wildcard interface
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Failed to bind test listener: %v", err)
	}
	defer l.Close()

	_, portStr, err := net.SplitHostPort(l.Addr().String())
	if err != nil {
		t.Fatalf("Failed to parse listener port: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("Failed to convert port: %v", err)
	}

	// Create config with conflicting S3Port
	cfg := &config.Config{
		AccessKey:   "testkey",
		SecretKey:   "testsecret",
		DataDir:     tempDir,
		S3Port:      port,
		ConsolePort: 0,
		Region:      "us-east-1",
	}

	srv, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// Try starting the server - it should fail synchronously
	err = srv.Start()
	if err == nil {
		t.Error("Expected error when starting server with conflicting port, got nil")
		srv.Shutdown(context.Background())
	}
}
