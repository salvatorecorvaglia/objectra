// Objectra — Self-hosted S3-compatible Object Storage
//
// Objectra is a high-performance, S3-compatible object storage server
// with a built-in web console for bucket and object management.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/salvatorecorvaglia/objectra/internal/config"
	"github.com/salvatorecorvaglia/objectra/internal/server"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	// Parse version flags
	for _, arg := range os.Args[1:] {
		if arg == "-v" || arg == "--version" || arg == "-version" || arg == "version" {
			fmt.Printf("Objectra version %s (commit %s, built at %s)\n", version, commit, date)
			os.Exit(0)
		}
	}

	cfg := config.Load()

	// Set up structured logging
	var level slog.Level
	switch strings.ToLower(cfg.LogLevel) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	var handler slog.Handler
	opts := &slog.HandlerOptions{Level: level}
	if strings.ToLower(cfg.LogFormat) == "json" {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}
	slog.SetDefault(slog.New(handler))

	if cfg.AccessKey == "objectra" && cfg.SecretKey == "objectra123" {
		slog.Warn("WARNING: Running Objectra with default credentials! Please set OBJECTRA_ACCESS_KEY and OBJECTRA_SECRET_KEY in production.")
	}

	printBanner(cfg)

	srv, err := server.New(cfg)
	if err != nil {
		slog.Error("Failed to start Objectra", "error", err)
		os.Exit(1)
	}

	if err := srv.Start(); err != nil {
		slog.Error("Server error", "error", err)
		os.Exit(1)
	}

	// Handle graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	<-sigCh
	slog.Info("Shutdown signal received")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("Shutdown error", "error", err)
		os.Exit(1)
	}

	slog.Info("[Server] Objectra stopped")
}

func printBanner(cfg *config.Config) {
	banner := fmt.Sprintf(`
   ____  __     _           __            
  / __ \/ /_   (_)__  _____/ /__________ _
 / / / / __ \ / / _ \/ ___/ __/ ___/ __  /
/ /_/ / /_/ // /  __/ /__/ /_/ /  / /_/ / 
\____/_.___// /\___/\___/\__/_/   \__,_/  
         /___/                             

  S3-Compatible Object Storage Server (%s)
`, version)
	fmt.Print(banner)
	scheme := "http"
	if cfg.TLSEnabled {
		scheme = "https"
	}
	fmt.Println("─────────────────────────────────────────")
	fmt.Printf("  S3 API:     %s://0.0.0.0:%d\n", scheme, cfg.S3Port)
	fmt.Printf("  Console:    %s://0.0.0.0:%d\n", scheme, cfg.ConsolePort)
	fmt.Printf("  Data Dir:   %s\n", cfg.DataDir)
	fmt.Printf("  Access Key: %s\n", cfg.AccessKey)
	fmt.Printf("  Region:     %s\n", cfg.Region)
	fmt.Println("─────────────────────────────────────────")
	fmt.Println()
}
