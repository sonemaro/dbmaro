// raft.go
package queue

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/sonemaro/dbmaro/pkg/wal/logger"
)

// RaftConfig holds Raft-specific configuration
type RaftConfig struct {
	NodeID             string
	BindAddr           string
	DataDir            string
	RetainLogs         int
	SnapshotInterval   time.Duration
	SnapshotThreshold  uint64
	LeaderLeaseTimeout time.Duration
	HeartbeatTimeout   time.Duration
	ElectionTimeout    time.Duration
	CommitTimeout      time.Duration
	MaxAppendEntries   int
}

// RaftNode represents a Raft node in the cluster
type RaftNode struct {
	node      *raft.Raft
	fsm       *QueueFSM
	logger    *logger.Manager
	transport *raft.NetworkTransport
	config    *RaftConfig
}

// raftLogger adapts our logger to hclog.Logger interface
type raftLogger struct {
	logger *logger.Manager
}

func newRaftLogger(logger *logger.Manager) hclog.Logger {
	return &raftLogger{
		logger: logger,
	}
}

// Implement all required hclog.Logger methods
func (r *raftLogger) Log(level hclog.Level, msg string, args ...interface{}) {
	switch level {
	case hclog.Debug:
		r.logger.Debug(msg, args...)
	case hclog.Info:
		r.logger.Info(msg, args...)
	case hclog.Warn:
		r.logger.Warn(msg, args...)
	case hclog.Error:
		r.logger.Error(msg, args...)
	}
}

func (r *raftLogger) Trace(msg string, args ...interface{}) {}
func (r *raftLogger) Debug(msg string, args ...interface{}) { r.logger.Debug(msg, args...) }
func (r *raftLogger) Info(msg string, args ...interface{})  { r.logger.Info(msg, args...) }
func (r *raftLogger) Warn(msg string, args ...interface{})  { r.logger.Warn(msg, args...) }
func (r *raftLogger) Error(msg string, args ...interface{}) { r.logger.Error(msg, args...) }
func (r *raftLogger) IsTrace() bool                         { return false }
func (r *raftLogger) IsDebug() bool                         { return true }
func (r *raftLogger) IsInfo() bool                          { return true }
func (r *raftLogger) IsWarn() bool                          { return true }
func (r *raftLogger) IsError() bool                         { return true }
func (r *raftLogger) ImpliedArgs() []interface{}            { return nil }
func (r *raftLogger) With(args ...interface{}) hclog.Logger { return r }
func (r *raftLogger) Name() string                          { return "raft" }
func (r *raftLogger) Named(name string) hclog.Logger        { return r }
func (r *raftLogger) ResetNamed(name string) hclog.Logger   { return r }
func (r *raftLogger) SetLevel(level hclog.Level)            {}
func (r *raftLogger) GetLevel() hclog.Level                 { return hclog.Debug }
func (r *raftLogger) StandardLogger(opts *hclog.StandardLoggerOptions) *log.Logger {
	return log.New(os.Stderr, "", log.LstdFlags)
}
func (r *raftLogger) StandardWriter(opts *hclog.StandardLoggerOptions) io.Writer {
	return os.Stderr
}

// Convert queue Config to RaftConfig
func configToRaftConfig(config *Config) *RaftConfig {
	return &RaftConfig{
		NodeID:             config.RaftAddress,
		BindAddr:           config.RaftAddress,
		DataDir:            config.RaftDir,
		RetainLogs:         1000,
		SnapshotInterval:   30 * time.Minute,
		SnapshotThreshold:  8192,
		LeaderLeaseTimeout: 500 * time.Millisecond,
		HeartbeatTimeout:   1000 * time.Millisecond,
		ElectionTimeout:    1000 * time.Millisecond,
		CommitTimeout:      50 * time.Millisecond,
		MaxAppendEntries:   64,
	}
}

// NewRaftNode creates a new Raft node
func NewRaftNode(config *Config, queue *PartitionedQueue, logger *logger.Manager) (*RaftNode, error) {
	raftConfig := configToRaftConfig(config)

	// Create FSM
	fsm := &QueueFSM{
		queue:  queue,
		logger: logger,
	}

	// Create Raft configuration
	conf := raft.DefaultConfig()
	conf.LocalID = raft.ServerID(raftConfig.NodeID)
	conf.SnapshotInterval = raftConfig.SnapshotInterval
	conf.SnapshotThreshold = raftConfig.SnapshotThreshold
	conf.LeaderLeaseTimeout = raftConfig.LeaderLeaseTimeout
	conf.HeartbeatTimeout = raftConfig.HeartbeatTimeout
	conf.ElectionTimeout = raftConfig.ElectionTimeout
	conf.CommitTimeout = raftConfig.CommitTimeout
	conf.MaxAppendEntries = raftConfig.MaxAppendEntries
	conf.Logger = newRaftLogger(logger)

	// Setup transport
	addr, err := net.ResolveTCPAddr("tcp", raftConfig.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve bind address: %w", err)
	}

	transport, err := raft.NewTCPTransport(raftConfig.BindAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create transport: %w", err)
	}

	// Create WAL store
	logStore, err := NewWALStore(filepath.Join(raftConfig.DataDir, "raft.wal"))
	if err != nil {
		return nil, fmt.Errorf("failed to create log store: %w", err)
	}

	// Create snapshot store
	snapshotDir := filepath.Join(raftConfig.DataDir, "snapshots")
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create snapshot directory: %w", err)
	}

	snapshotStore, err := raft.NewFileSnapshotStore(snapshotDir, raftConfig.RetainLogs, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot store: %w", err)
	}

	// Initialize Raft
	r, err := raft.NewRaft(conf, fsm, logStore, logStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft: %w", err)
	}

	node := &RaftNode{
		node:      r,
		fsm:       fsm,
		logger:    logger,
		transport: transport,
	}

	// Handle bootstrapping if configured
	if config.Bootstrap {
		logger.Info("bootstrapping new cluster")

		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:       raft.ServerID(config.RaftAddress),
					Address:  raft.ServerAddress(config.RaftAddress),
					Suffrage: raft.Voter,
				},
			},
		}

		// Check if the store is empty
		hasState, err := logStore.HasExistingState()
		if err != nil {
			logger.Error("failed to check existing state", "error", err)
			return nil, fmt.Errorf("failed to check existing state: %w", err)
		}

		if !hasState {
			f := r.BootstrapCluster(configuration)
			if err := f.Error(); err != nil {
				logger.Error("failed to bootstrap cluster", "error", err)
				return nil, fmt.Errorf("failed to bootstrap cluster: %w", err)
			}
			logger.Info("cluster bootstrapped successfully")
		} else {
			logger.Info("skipping bootstrap, cluster already initialized")
		}
	}

	return node, nil
}

// DefaultRaftConfig returns default Raft configuration
func DefaultRaftConfig() *RaftConfig {
	return &RaftConfig{
		RetainLogs:         1000,
		SnapshotInterval:   30 * time.Minute,
		SnapshotThreshold:  8192,
		LeaderLeaseTimeout: 500 * time.Millisecond,
		HeartbeatTimeout:   1000 * time.Millisecond,
		ElectionTimeout:    1000 * time.Millisecond,
		CommitTimeout:      50 * time.Millisecond,
		MaxAppendEntries:   64,
	}
}

// AddPeer adds a new peer to the Raft cluster
func (rn *RaftNode) AddPeer(id, addr string) error {
	rn.logger.Info("adding peer to cluster",
		"node_id", id,
		"address", addr)

	if rn.node.State() != raft.Leader {
		return ErrNotLeader
	}

	// Check if server already exists
	configFuture := rn.node.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return fmt.Errorf("failed to get configuration: %w", err)
	}

	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(addr) {
			return nil // already a member
		}
	}

	// Add as non-voter first
	future := rn.node.AddNonvoter(raft.ServerID(addr), raft.ServerAddress(addr), 0, 0)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to add non-voter: %w", err)
	}

	// Wait for replication to stabilize
	time.Sleep(5 * time.Second)

	// Check if we're still leader
	if rn.node.State() != raft.Leader {
		return ErrNotLeader
	}

	// Promote to voter
	future = rn.node.AddVoter(raft.ServerID(addr), raft.ServerAddress(addr), 0, 0)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to promote to voter: %w", err)
	}

	// Wait for promotion to complete
	time.Sleep(2 * time.Second)

	return nil
}

// RemovePeer removes a peer from the Raft cluster
func (rn *RaftNode) RemovePeer(id string) error {
	rn.logger.Info("removing peer from cluster", "node_id", id)

	if rn.node.State() != raft.Leader {
		return ErrNotLeader
	}

	future := rn.node.RemoveServer(raft.ServerID(id), 0, 0)
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to remove server: %w", err)
	}

	return nil
}

// Shutdown gracefully shuts down the Raft node
func (rn *RaftNode) Shutdown() error {
	rn.logger.Info("shutting down raft node")

	future := rn.node.Shutdown()
	if err := future.Error(); err != nil {
		return fmt.Errorf("failed to shutdown raft: %w", err)
	}

	return nil
}
