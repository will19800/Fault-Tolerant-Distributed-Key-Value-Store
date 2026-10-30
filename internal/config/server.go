package config

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/api"
	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/wal"
)

const (
	DefaultAddress         = "127.0.0.1:8080"
	DefaultDataDirectory   = "data"
	DefaultShutdownTimeout = 5 * time.Second
	DefaultRPCTimeout      = 250 * time.Millisecond
)

// Server contains standalone or clustered key-value server configuration.
type Server struct {
	Address           string
	DataDirectory     string
	MaxValueBytes     int64
	ShutdownTimeout   time.Duration
	NodeID            raft.NodeID
	PeerAddress       string
	Peers             map[raft.NodeID]string
	RPCTimeout        time.Duration
	ElectionTimeout   time.Duration
	HeartbeatInterval time.Duration
}

// DefaultServer returns the default standalone server configuration.
func DefaultServer() Server {
	return Server{
		Address:         DefaultAddress,
		DataDirectory:   DefaultDataDirectory,
		MaxValueBytes:   api.DefaultMaxValueBytes,
		ShutdownTimeout: DefaultShutdownTimeout,
		RPCTimeout:      DefaultRPCTimeout, ElectionTimeout: raft.DefaultElectionTimeout,
		HeartbeatInterval: raft.DefaultHeartbeatInterval,
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
	flags.Var((*nodeIDValue)(&cfg.NodeID), "node-id", "Raft node ID (enables cluster mode)")
	flags.StringVar(&cfg.PeerAddress, "peer-addr", cfg.PeerAddress, "Raft peer listen address in host:port form")
	var peers string
	flags.StringVar(&peers, "peers", "", "comma-separated Raft members as node-id=host:port")
	flags.DurationVar(&cfg.RPCTimeout, "rpc-timeout", cfg.RPCTimeout, "Raft peer RPC timeout")
	flags.DurationVar(&cfg.ElectionTimeout, "election-timeout", cfg.ElectionTimeout, "minimum Raft election timeout")
	flags.DurationVar(&cfg.HeartbeatInterval, "heartbeat-interval", cfg.HeartbeatInterval, "Raft heartbeat interval")

	if err := flags.Parse(args); err != nil {
		return Server{}, err
	}
	if flags.NArg() != 0 {
		return Server{}, fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}
	if peers != "" {
		parsedPeers, parseErr := parsePeers(peers)
		if parseErr != nil {
			return Server{}, parseErr
		}
		cfg.Peers = parsedPeers
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
	clusterFields := c.NodeID != "" || c.PeerAddress != "" || len(c.Peers) != 0
	if !clusterFields {
		return nil
	}
	if err := c.NodeID.Validate(); err != nil {
		return fmt.Errorf("node ID: %w", err)
	}
	if err := validateAddress(c.PeerAddress); err != nil {
		return fmt.Errorf("peer address: %w", err)
	}
	if c.PeerAddress == c.Address {
		return errors.New("client and peer addresses must differ")
	}
	if len(c.Peers) != 3 && len(c.Peers) != 5 {
		return fmt.Errorf("Raft peers must contain exactly 3 or 5 members, got %d", len(c.Peers))
	}
	if _, exists := c.Peers[c.NodeID]; !exists {
		return fmt.Errorf("local node %q is missing from peers", c.NodeID)
	}
	seenAddresses := make(map[string]raft.NodeID, len(c.Peers))
	for id, address := range c.Peers {
		if err := id.Validate(); err != nil {
			return fmt.Errorf("peer ID: %w", err)
		}
		if err := validateAddress(address); err != nil {
			return fmt.Errorf("peer %q address: %w", id, err)
		}
		if previous, exists := seenAddresses[address]; exists {
			return fmt.Errorf("peers %q and %q share address %q", previous, id, address)
		}
		seenAddresses[address] = id
	}
	if c.Peers[c.NodeID] != c.PeerAddress {
		return fmt.Errorf("local peer address %q does not match peers entry %q", c.PeerAddress, c.Peers[c.NodeID])
	}
	if c.RPCTimeout <= 0 {
		return fmt.Errorf("RPC timeout must be positive: %s", c.RPCTimeout)
	}
	if c.ElectionTimeout <= 0 {
		return fmt.Errorf("election timeout must be positive: %s", c.ElectionTimeout)
	}
	if c.HeartbeatInterval <= 0 || c.HeartbeatInterval >= c.ElectionTimeout {
		return errors.New("heartbeat interval must be positive and below election timeout")
	}
	return nil
}

// Clustered reports whether the server is configured as a Raft member.
func (c Server) Clustered() bool { return c.NodeID != "" }

func parsePeers(value string) (map[raft.NodeID]string, error) {
	peers := make(map[raft.NodeID]string)
	for _, item := range strings.Split(value, ",") {
		idText, address, found := strings.Cut(strings.TrimSpace(item), "=")
		id := raft.NodeID(strings.TrimSpace(idText))
		address = strings.TrimSpace(address)
		if !found || id == "" || address == "" {
			return nil, fmt.Errorf("invalid peer %q: expected node-id=host:port", item)
		}
		if _, exists := peers[id]; exists {
			return nil, fmt.Errorf("duplicate peer %q", id)
		}
		peers[id] = address
	}
	return peers, nil
}

type nodeIDValue raft.NodeID

func (v *nodeIDValue) String() string { return string(*v) }
func (v *nodeIDValue) Set(value string) error {
	*v = nodeIDValue(value)
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
