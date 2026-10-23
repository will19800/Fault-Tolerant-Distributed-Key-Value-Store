// Command kvserver runs a key-value store node.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/api"
	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/config"
	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/database"
	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/wal"
)

const readHeaderTimeout = 5 * time.Second

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stderr); err != nil {
		newLogger(os.Stderr).Error("server failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, output io.Writer) (runErr error) {
	cfg, err := config.ParseServer(args, output)
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("parse configuration: %w", err)
	}

	if err := os.MkdirAll(cfg.DataDirectory, 0o700); err != nil {
		return fmt.Errorf("create data directory %q: %w", cfg.DataDirectory, err)
	}
	store, recovery, err := database.Open(filepath.Join(cfg.DataDirectory, "store.wal"), wal.SyncEveryAppend)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer func() {
		runErr = errors.Join(runErr, store.Close())
	}()
	newLogger(output).Info("database recovered", "records", recovery.Records, "truncated_bytes", recovery.TruncatedBytes)

	handler, err := api.NewHandler(store, cfg.MaxValueBytes)
	if err != nil {
		return fmt.Errorf("create API handler: %w", err)
	}

	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.Address, err)
	}

	server := &http.Server{
		Handler:           handler.Routes(),
		ReadHeaderTimeout: readHeaderTimeout,
	}
	return serve(ctx, server, listener, cfg.ShutdownTimeout, newLogger(output))
}

func serve(ctx context.Context, server *http.Server, listener net.Listener, shutdownTimeout time.Duration, logger *slog.Logger) error {
	serveErrors := make(chan error, 1)
	go func() {
		serveErrors <- server.Serve(listener)
	}()

	logger.Info("server started", "address", listener.Addr().String())

	select {
	case err := <-serveErrors:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve HTTP: %w", err)
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		_ = server.Close()
		return fmt.Errorf("graceful shutdown: %w", err)
	}

	if err := <-serveErrors; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve HTTP during shutdown: %w", err)
	}
	logger.Info("server stopped")
	return nil
}

func newLogger(output io.Writer) *slog.Logger {
	if output == nil {
		output = io.Discard
	}
	return slog.New(slog.NewJSONHandler(output, nil))
}
