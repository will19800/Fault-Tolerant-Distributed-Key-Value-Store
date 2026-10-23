package config_test

import (
	"flag"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/api"
	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/config"
)

func TestDefaultServer(t *testing.T) {
	cfg := config.DefaultServer()

	if cfg.Address != config.DefaultAddress {
		t.Fatalf("Address = %q, want %q", cfg.Address, config.DefaultAddress)
	}
	if cfg.DataDirectory != config.DefaultDataDirectory {
		t.Fatalf("DataDirectory = %q, want %q", cfg.DataDirectory, config.DefaultDataDirectory)
	}
	if cfg.MaxValueBytes != api.DefaultMaxValueBytes {
		t.Fatalf("MaxValueBytes = %d, want %d", cfg.MaxValueBytes, api.DefaultMaxValueBytes)
	}
	if cfg.ShutdownTimeout != config.DefaultShutdownTimeout {
		t.Fatalf("ShutdownTimeout = %s, want %s", cfg.ShutdownTimeout, config.DefaultShutdownTimeout)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default configuration is invalid: %v", err)
	}
}

func TestParseServer(t *testing.T) {
	cfg, err := config.ParseServer([]string{
		"-addr", "localhost:9090",
		"-data-dir", "/tmp/kv-data",
		"-max-value-bytes", "2048",
		"-shutdown-timeout", "750ms",
	}, io.Discard)
	if err != nil {
		t.Fatalf("ParseServer: %v", err)
	}

	if cfg.Address != "localhost:9090" {
		t.Fatalf("Address = %q, want localhost:9090", cfg.Address)
	}
	if cfg.DataDirectory != "/tmp/kv-data" {
		t.Fatalf("DataDirectory = %q, want /tmp/kv-data", cfg.DataDirectory)
	}
	if cfg.MaxValueBytes != 2048 {
		t.Fatalf("MaxValueBytes = %d, want 2048", cfg.MaxValueBytes)
	}
	if cfg.ShutdownTimeout != 750*time.Millisecond {
		t.Fatalf("ShutdownTimeout = %s, want 750ms", cfg.ShutdownTimeout)
	}
}

func TestParseServerHelp(t *testing.T) {
	var output strings.Builder
	_, err := config.ParseServer([]string{"-help"}, &output)
	if err != flag.ErrHelp {
		t.Fatalf("ParseServer help error = %v, want %v", err, flag.ErrHelp)
	}
	if !strings.Contains(output.String(), "-addr") {
		t.Fatalf("help output does not describe -addr: %q", output.String())
	}
}

func TestServerValidation(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*config.Server)
		wantError string
	}{
		{
			name:      "missing port",
			configure: func(cfg *config.Server) { cfg.Address = "localhost" },
			wantError: "expected host:port",
		},
		{
			name:      "URL instead of address",
			configure: func(cfg *config.Server) { cfg.Address = "http://localhost:8080" },
			wantError: "expected host:port",
		},
		{
			name:      "zero port",
			configure: func(cfg *config.Server) { cfg.Address = "localhost:0" },
			wantError: "port must be between 1 and 65535",
		},
		{
			name:      "port too large",
			configure: func(cfg *config.Server) { cfg.Address = "localhost:65536" },
			wantError: "port must be between 1 and 65535",
		},
		{
			name:      "empty data directory",
			configure: func(cfg *config.Server) { cfg.DataDirectory = "" },
			wantError: "data directory must not be empty",
		},
		{
			name:      "zero value limit",
			configure: func(cfg *config.Server) { cfg.MaxValueBytes = 0 },
			wantError: "maximum value size must be positive",
		},
		{
			name:      "zero shutdown timeout",
			configure: func(cfg *config.Server) { cfg.ShutdownTimeout = 0 },
			wantError: "shutdown timeout must be positive",
		},
		{
			name:      "value limit exceeds WAL limit",
			configure: func(cfg *config.Server) { cfg.MaxValueBytes = 64<<20 + 1 },
			wantError: "maximum value size must not exceed WAL limit",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := config.DefaultServer()
			test.configure(&cfg)
			err := cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("Validate error = %v, want containing %q", err, test.wantError)
			}
		})
	}
}

func TestParseServerRejectsPositionalArguments(t *testing.T) {
	_, err := config.ParseServer([]string{"unexpected"}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "unexpected positional arguments") {
		t.Fatalf("ParseServer error = %v, want unexpected positional arguments", err)
	}
}
