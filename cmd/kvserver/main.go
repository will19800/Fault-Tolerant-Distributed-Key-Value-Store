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
	"sort"
	"syscall"
	"time"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/api"
	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/config"
	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/database"
	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/transport"
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
	if cfg.Clustered() {
		return runCluster(ctx, cfg, output)
	}
	return runStandalone(ctx, cfg, output)
}

func runStandalone(ctx context.Context, cfg config.Server, output io.Writer) (runErr error) {
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

func runCluster(ctx context.Context, cfg config.Server, output io.Writer) (runErr error) {
	if err := os.MkdirAll(cfg.DataDirectory, 0o700); err != nil {
		return fmt.Errorf("create data directory %q: %w", cfg.DataDirectory, err)
	}
	hardState, err := raft.NewFileHardStateStore(filepath.Join(cfg.DataDirectory, "raft-hard-state.bin"))
	if err != nil {
		return err
	}
	logStore, err := raft.NewFileLogStore(filepath.Join(cfg.DataDirectory, "raft-log.bin"))
	if err != nil {
		return err
	}
	peerTransport, err := transport.NewHTTPTransport(cfg.Peers, cfg.RPCTimeout)
	if err != nil {
		return fmt.Errorf("create peer transport: %w", err)
	}
	members := make([]raft.NodeID, 0, len(cfg.Peers))
	for id := range cfg.Peers {
		members = append(members, id)
	}
	sort.Slice(members, func(i, j int) bool { return members[i] < members[j] })
	stateMachine := database.NewReplicatedStateMachine()
	node, err := raft.NewNode(raft.NodeConfig{
		ID: cfg.NodeID, Members: members, Transport: peerTransport,
		ElectionTimeout: cfg.ElectionTimeout, HeartbeatInterval: cfg.HeartbeatInterval,
		Persistence: hardState, LogStore: logStore, StateMachine: stateMachine,
	})
	if err != nil {
		return fmt.Errorf("create Raft node: %w", err)
	}
	defer func() { runErr = errors.Join(runErr, node.Close()) }()

	store, err := database.NewReplicatedStore(node, stateMachine)
	if err != nil {
		return fmt.Errorf("create replicated store: %w", err)
	}
	clientHandler, err := api.NewHandler(store, cfg.MaxValueBytes)
	if err != nil {
		return fmt.Errorf("create API handler: %w", err)
	}
	peerHandler, err := transport.NewHTTPHandler(node, transport.DefaultMaxRPCBytes)
	if err != nil {
		return fmt.Errorf("create peer handler: %w", err)
	}
	clientListener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return fmt.Errorf("listen on client address %s: %w", cfg.Address, err)
	}
	peerListener, err := net.Listen("tcp", cfg.PeerAddress)
	if err != nil {
		_ = clientListener.Close()
		return fmt.Errorf("listen on peer address %s: %w", cfg.PeerAddress, err)
	}
	clientServer := &http.Server{Handler: clientHandler.Routes(), ReadHeaderTimeout: readHeaderTimeout}
	peerServer := &http.Server{Handler: peerHandler.Routes(), ReadHeaderTimeout: readHeaderTimeout}
	logger := newLogger(output)
	logger.Info("cluster node starting", "node_id", cfg.NodeID, "client_address", cfg.Address, "peer_address", cfg.PeerAddress)
	serveCtx, cancelServe := context.WithCancel(ctx)
	nodeStopped := make(chan error, 1)
	go func() {
		<-serveCtx.Done()
		nodeStopped <- node.Close()
	}()
	serveErr := servePair(serveCtx, clientServer, clientListener, peerServer, peerListener, cfg.ShutdownTimeout, logger)
	cancelServe()
	return errors.Join(serveErr, <-nodeStopped)
}

func servePair(ctx context.Context, firstServer *http.Server, firstListener net.Listener, secondServer *http.Server, secondListener net.Listener, shutdownTimeout time.Duration, logger *slog.Logger) error {
	type serveResult struct {
		name string
		err  error
	}
	results := make(chan serveResult, 2)
	go func() { results <- serveResult{name: "client", err: firstServer.Serve(firstListener)} }()
	go func() { results <- serveResult{name: "peer", err: secondServer.Serve(secondListener)} }()
	logger.Info("cluster HTTP servers started", "client_address", firstListener.Addr().String(), "peer_address", secondListener.Addr().String())

	var early *serveResult
	select {
	case result := <-results:
		early = &result
	case <-ctx.Done():
	}
	peerCloseErr := secondServer.Close()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	clientShutdownErr := firstServer.Shutdown(shutdownCtx)
	cancel()
	if clientShutdownErr != nil {
		_ = firstServer.Close()
	}

	remaining := 2
	if early != nil {
		remaining--
	}
	var runErr error
	if early != nil && !errors.Is(early.err, http.ErrServerClosed) {
		runErr = fmt.Errorf("%s HTTP server: %w", early.name, early.err)
	}
	for range remaining {
		result := <-results
		if result.err != nil && !errors.Is(result.err, http.ErrServerClosed) {
			runErr = errors.Join(runErr, fmt.Errorf("%s HTTP server: %w", result.name, result.err))
		}
	}
	logger.Info("cluster HTTP servers stopped")
	return errors.Join(runErr, peerCloseErr, clientShutdownErr)
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
