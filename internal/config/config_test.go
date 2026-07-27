package config_test

import (
	"os"
	"testing"

	"github.com/salvatorecorvaglia/objectra/internal/config"
)

func TestConfigLoadDefaults(t *testing.T) {
	// Clear any overrides
	os.Unsetenv("OBJECTRA_ACCESS_KEY")
	os.Unsetenv("OBJECTRA_SECRET_KEY")
	os.Unsetenv("OBJECTRA_S3_PORT")
	os.Unsetenv("OBJECTRA_TLS_ENABLED")

	cfg := config.Load()

	if cfg.AccessKey != "objectra" {
		t.Errorf("expected default access key 'objectra', got '%s'", cfg.AccessKey)
	}
	if cfg.SecretKey != "objectra123" {
		t.Errorf("expected default secret key 'objectra123', got '%s'", cfg.SecretKey)
	}
	if cfg.S3Port != 9000 {
		t.Errorf("expected default S3 port 9000, got %d", cfg.S3Port)
	}
	if cfg.ConsolePort != 9001 {
		t.Errorf("expected default console port 9001, got %d", cfg.ConsolePort)
	}
	if cfg.TLSEnabled {
		t.Errorf("expected TLS to be disabled by default")
	}
}

func TestConfigEnvOverrides(t *testing.T) {
	t.Setenv("OBJECTRA_ACCESS_KEY", "customaccess")
	t.Setenv("OBJECTRA_SECRET_KEY", "customsecret")
	t.Setenv("OBJECTRA_S3_PORT", "9900")
	t.Setenv("OBJECTRA_CONSOLE_PORT", "9901")
	t.Setenv("OBJECTRA_TLS_ENABLED", "true")
	t.Setenv("OBJECTRA_LOG_LEVEL", "debug")

	cfg := config.Load()

	if cfg.AccessKey != "customaccess" {
		t.Errorf("expected access key 'customaccess', got '%s'", cfg.AccessKey)
	}
	if cfg.SecretKey != "customsecret" {
		t.Errorf("expected secret key 'customsecret', got '%s'", cfg.SecretKey)
	}
	if cfg.S3Port != 9900 {
		t.Errorf("expected S3 port 9900, got %d", cfg.S3Port)
	}
	if cfg.ConsolePort != 9901 {
		t.Errorf("expected console port 9901, got %d", cfg.ConsolePort)
	}
	if !cfg.TLSEnabled {
		t.Errorf("expected TLS to be enabled")
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("expected log level 'debug', got '%s'", cfg.LogLevel)
	}
}
