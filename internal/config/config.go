// Package config provides configuration management for Objectra.
// All settings are read from environment variables with sensible defaults.
package config

import (
	"os"
	"strconv"
)

// Config holds all runtime configuration for the Objectra server.
type Config struct {
	// AccessKey is the S3 access key used for authentication.
	AccessKey string
	// SecretKey is the S3 secret key used for authentication.
	SecretKey string
	// DataDir is the root directory for storing buckets and objects.
	DataDir string
	// S3Port is the port the S3 API server listens on.
	S3Port int
	// ConsolePort is the port the web console listens on.
	ConsolePort int
	// Region is the S3 region reported by the server.
	Region string
	// Domain is the base domain used for virtual host S3 bucket routing.
	Domain string
	// TLSEnabled specifies whether to serve S3 and Console APIs over HTTPS/TLS.
	TLSEnabled bool
	// TLSCert is the path to the SSL certificate file.
	TLSCert string
	// TLSKey is the path to the SSL private key file.
	TLSKey string
	// LoginRateLimit specifies the maximum requests per minute per IP for the login endpoint.
	LoginRateLimit int
	// APIRateLimit specifies the maximum requests per minute per IP for other console endpoints.
	APIRateLimit int
	// LogLevel is the log level (debug, info, warn, error).
	LogLevel string
	// LogFormat is the log format (text, json).
	LogFormat string
	// SyncEndpoint is the replication destination endpoint.
	SyncEndpoint string
	// SyncBucket is the replication destination bucket.
	SyncBucket string
	// SyncAccessKey is the replication access key.
	SyncAccessKey string
	// SyncSecretKey is the replication secret key.
	SyncSecretKey string
	// SyncRegion is the replication region.
	SyncRegion string
	// WebhookURL is the webhook notifications URL destination.
	WebhookURL string
}

// Load reads configuration from environment variables, falling back to defaults.
func Load() *Config {
	return &Config{
		AccessKey:      envOrDefault("OBJECTRA_ACCESS_KEY", "objectra"),
		SecretKey:      envOrDefault("OBJECTRA_SECRET_KEY", "objectra123"),
		DataDir:        envOrDefault("OBJECTRA_DATA_DIR", "/data"),
		S3Port:         envIntOrDefault("OBJECTRA_S3_PORT", 9000),
		ConsolePort:    envIntOrDefault("OBJECTRA_CONSOLE_PORT", 9001),
		Region:         envOrDefault("OBJECTRA_REGION", "us-east-1"),
		Domain:         envOrDefault("OBJECTRA_DOMAIN", ""),
		TLSEnabled:     os.Getenv("OBJECTRA_TLS_ENABLED") == "true",
		TLSCert:        envOrDefault("OBJECTRA_TLS_CERT", ""),
		TLSKey:         envOrDefault("OBJECTRA_TLS_KEY", ""),
		LoginRateLimit: envIntOrDefault("OBJECTRA_LOGIN_RATE_LIMIT", 5),
		APIRateLimit:   envIntOrDefault("OBJECTRA_API_RATE_LIMIT", 60),
		LogLevel:       envOrDefault("OBJECTRA_LOG_LEVEL", "info"),
		LogFormat:      envOrDefault("OBJECTRA_LOG_FORMAT", "text"),
		SyncEndpoint:   envOrDefault("OBJECTRA_SYNC_ENDPOINT", ""),
		SyncBucket:     envOrDefault("OBJECTRA_SYNC_BUCKET", ""),
		SyncAccessKey:  envOrDefault("OBJECTRA_SYNC_ACCESS_KEY", ""),
		SyncSecretKey:  envOrDefault("OBJECTRA_SYNC_SECRET_KEY", ""),
		SyncRegion:     envOrDefault("OBJECTRA_SYNC_REGION", "us-east-1"),
		WebhookURL:     envOrDefault("OBJECTRA_WEBHOOK_URL", ""),
	}
}

func envOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func envIntOrDefault(key string, defaultVal int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return defaultVal
}
