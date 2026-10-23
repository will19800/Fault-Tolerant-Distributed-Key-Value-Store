package config

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/api"
	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/wal"
)

const (
	DefaultAddress         = "127.0.0.1:8080"
	DefaultDataDirectory   = "data"
	DefaultShutdownTimeout = 5 * time.Second
)

// Server contains configuration for a single-node key-value server.
type Server struct {
	Address         string
	DataDirectory   string
	MaxValueBytes   int64
	ShutdownTimeout time.Duration
}

// DefaultServer returns the default single-node server configuration.
func DefaultServer() Server {
	return Server{
		Address:         DefaultAddress,
		DataDirectory:   DefaultDataDirectory,
		MaxValueBytes:   api.DefaultMaxValueBytes,
		ShutdownTimeout: DefaultShutdownTimeout,
	}
}

// ParseServer parses command-line flags and validates the resulting server
// configuration. Flag usage and parse errors are written to output.
func ParseServer(args []string, output io.Writer) (Server, error) {
	if output == nil {
		output = io.Discard
	}

	cfg := DefaultServer()
	flags := flag.NewFlagSet("kvserver", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.StringVar(&cfg.Address, "addr", cfg.Address, "HTTP listen address in host:port form")
	flags.StringVar(&cfg.DataDirectory, "data-dir", cfg.DataDirectory, "directory for durable server data")
	flags.Int64Var(&cfg.MaxValueBytes, "max-value-bytes", cfg.MaxValueBytes, "maximum PUT request body size")
	flags.DurationVar(&cfg.ShutdownTimeout, "shutdown-timeout", cfg.ShutdownTimeout, "graceful shutdown timeout")

	if err := flags.Parse(args); err != nil {
		return Server{}, err
	}
	if flags.NArg() != 0 {
		return Server{}, fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}
	if err := cfg.Validate(); err != nil {
		return Server{}, err
	}
	return cfg, nil
}

// Validate reports whether the server configuration can be used safely.
func (c Server) Validate() error {
	if err := validateAddress(c.Address); err != nil {
		return err
	}
	if c.DataDirectory == "" {
		return errors.New("data directory must not be empty")
	}
	if c.MaxValueBytes <= 0 {
		return fmt.Errorf("maximum value size must be positive: %d", c.MaxValueBytes)
	}
	if c.MaxValueBytes > wal.MaxValueBytes {
		return fmt.Errorf("maximum value size must not exceed WAL limit %d: %d", wal.MaxValueBytes, c.MaxValueBytes)
	}
	if c.ShutdownTimeout <= 0 {
		return fmt.Errorf("shutdown timeout must be positive: %s", c.ShutdownTimeout)
	}
	return nil
}

func validateAddress(address string) error {
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("invalid listen address %q: expected host:port: %w", address, err)
	}

	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return fmt.Errorf("invalid listen address %q: port must be between 1 and 65535", address)
	}
	return nil
}
