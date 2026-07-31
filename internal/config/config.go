// Package config provides configuration management for Stiva.
// All settings are read from environment variables with sensible defaults.
package config

import (
	"os"
	"strconv"
)

// Config holds all runtime configuration for the Stiva server.
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
	// S3Endpoint is the public S3 endpoint URL for presigned links.
	S3Endpoint string
	// TrustProxy specifies whether to trust proxy headers like X-Forwarded-For.
	TrustProxy bool
}

// Load reads configuration from environment variables, falling back to defaults.
func Load() *Config {
	return &Config{
		AccessKey:      envOrDefault("STIVA_ACCESS_KEY", "stiva"),
		SecretKey:      envOrDefault("STIVA_SECRET_KEY", "stiva123"),
		DataDir:        envOrDefault("STIVA_DATA_DIR", "/data"),
		S3Port:         envIntOrDefault("STIVA_S3_PORT", 9000),
		ConsolePort:    envIntOrDefault("STIVA_CONSOLE_PORT", 9001),
		Region:         envOrDefault("STIVA_REGION", "us-east-1"),
		Domain:         envOrDefault("STIVA_DOMAIN", ""),
		TLSEnabled:     os.Getenv("STIVA_TLS_ENABLED") == "true",
		TLSCert:        envOrDefault("STIVA_TLS_CERT", ""),
		TLSKey:         envOrDefault("STIVA_TLS_KEY", ""),
		LoginRateLimit: envIntOrDefault("STIVA_LOGIN_RATE_LIMIT", 5),
		APIRateLimit:   envIntOrDefault("STIVA_API_RATE_LIMIT", 60),
		LogLevel:       envOrDefault("STIVA_LOG_LEVEL", "info"),
		LogFormat:      envOrDefault("STIVA_LOG_FORMAT", "text"),
		SyncEndpoint:   envOrDefault("STIVA_SYNC_ENDPOINT", ""),
		SyncBucket:     envOrDefault("STIVA_SYNC_BUCKET", ""),
		SyncAccessKey:  envOrDefault("STIVA_SYNC_ACCESS_KEY", ""),
		SyncSecretKey:  envOrDefault("STIVA_SYNC_SECRET_KEY", ""),
		SyncRegion:     envOrDefault("STIVA_SYNC_REGION", "us-east-1"),
		WebhookURL:     envOrDefault("STIVA_WEBHOOK_URL", ""),
		S3Endpoint:     envOrDefault("STIVA_S3_ENDPOINT", ""),
		TrustProxy:     os.Getenv("STIVA_TRUST_PROXY") == "true",
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
