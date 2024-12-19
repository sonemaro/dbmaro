package queue

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/sonemaro/dbmaro/pkg/wal"
	"github.com/sonemaro/dbmaro/pkg/wal/logger"
	"github.com/sonemaro/dbmaro/pkg/wal/options"
)

var (
	ErrNotLeader             = errors.New("not the leader")
	ErrQueueFull             = errors.New("queue is full")
	ErrTooManyConsumerGroups = errors.New("too many consumer groups")
	ErrConsumerGroupExists   = errors.New("consumer group already exists")
	ErrConsumerGroupNotFound = errors.New("consumer group not found")
	ErrTooManyGroupMembers   = errors.New("too many members in consumer group")
	ErrInvalidPartition      = errors.New("invalid partition")
)

type PartitionedQueue struct {
	config     *Config
	partitions []*Partition
	wal        *wal.WAL
	batch      *BatchManager
	raft       *RaftNode
	logger     *logger.Manager

	consumerGroups map[string]*ConsumerGroup

	mu sync.RWMutex
}

func NewPartitionedQueue(config *Config, log *logger.Manager) (*PartitionedQueue, error) {
	log.WithFields(logger.Fields{
		"partitions": config.MaxPartitions,
		"batch_size": config.BatchSize,
	}).Info("initializing partitioned queue")

	// TODO read from config
	opts := options.Options{
		InitialMapSize: 32 * 1024 * 1024,       // 32MB
		MaxMapSize:     2 * 1024 * 1024 * 1024, // 2GB
		BufferSize:     256 * 1024,             // 256KB
		PoolConfig:     options.DefaultPoolConfig(),
	}

	w, err := wal.Open(config.WalDir, opts)
	if err != nil {
		log.Error("failed to open WAL",
			"error", err,
			"path", config.WalDir)

		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}

	q := &PartitionedQueue{
		config:         config,
		partitions:     make([]*Partition, config.MaxPartitions),
		wal:            w,
		consumerGroups: make(map[string]*ConsumerGroup),
		logger:         log,
	}

	// Initialize partitions
	for i := 0; i < config.MaxPartitions; i++ {
		q.partitions[i] = NewPartition(i)
	}

	q.batch = NewBatchManager(config, log)

	// Initialize raft if configured
	if config.RaftAddress != "" {
		q.raft, err = NewRaftNode(config, q, log)
		if err != nil {
			log.WithFields(logger.Fields{
				"error":   err,
				"address": config.RaftAddress,
			}).Error("failed to initialize raft")

			return nil, fmt.Errorf("failed to initialize raft: %w", err)
		}
	}

	return q, nil
}

// Enqueue adds messages to batch
func (q *PartitionedQueue) Enqueue(messages []*Message) error {
	if q.raft != nil && q.raft.node.State() != raft.Leader {
		return ErrNotLeader
	}

	for _, msg := range messages {
		select {
		case q.batch.messages <- msg:
			q.logger.Debug("message queued for batch",
				"partition", msg.Partition,
				"id", msg.ID)
		default:
			return ErrQueueFull
		}
	}

	return nil
}

// StartHTTPServer starts the HTTP server for cluster management
func (q *PartitionedQueue) StartHTTPServer(address string) error {
	mux := http.NewServeMux()

	// Handler for join requests
	mux.HandleFunc("/cluster/join", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Check if Raft is initialized
		if q.raft == nil {
			msg := "Raft not initialized"
			q.logger.Error(msg)
			http.Error(w, msg, http.StatusServiceUnavailable)
			return
		}

		// Check if node is leader
		state := q.raft.node.State()
		if state != raft.Leader {
			msg := fmt.Sprintf("Not the leader. Current state: %v", state)
			q.logger.Error(msg)
			http.Error(w, msg, http.StatusServiceUnavailable)
			return
		}

		address := r.FormValue("address")
		if address == "" {
			http.Error(w, "Address required", http.StatusBadRequest)
			return
		}

		// Verify the joining node is reachable
		conn, err := net.DialTimeout("tcp", address, 5*time.Second)
		if err != nil {
			msg := fmt.Sprintf("Node at %s is not reachable: %v", address, err)
			q.logger.Error(msg)
			http.Error(w, msg, http.StatusServiceUnavailable)
			return
		}
		conn.Close()

		// Generate a node ID
		nodeID := address

		q.logger.Info("received join request",
			"node_id", nodeID,
			"address", address,
			"raft_state", state)

		// Try to add peer with retries
		maxRetries := 3
		var lastErr error
		for i := 0; i < maxRetries; i++ {
			if i > 0 {
				time.Sleep(time.Second * 2)
			}

			if err := q.raft.AddPeer(nodeID, address); err != nil {
				lastErr = err
				q.logger.Error("failed to add peer, retrying",
					"attempt", i+1,
					"node_id", nodeID,
					"address", address,
					"error", err)
				continue
			}

			q.logger.Info("new node joined cluster",
				"node_id", nodeID,
				"address", address)
			w.WriteHeader(http.StatusOK)
			return
		}

		errMsg := fmt.Sprintf("Failed to add peer after %d attempts: %v", maxRetries, lastErr)
		q.logger.Error(errMsg,
			"node_id", nodeID,
			"address", address)
		http.Error(w, errMsg, http.StatusInternalServerError)
	})

	// Add health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		state := "follower"
		if q.raft != nil && q.raft.node.State() == raft.Leader {
			state = "leader"
		}
		fmt.Fprintf(w, `{"status":"ok","state":"%s"}`, state)
	})

	server := &http.Server{
		Addr:    address,
		Handler: mux,
	}

	go func() {
		q.logger.Info("starting HTTP server", "address", address)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			q.logger.Error("http server failed", "error", err)
		}
	}()

	return nil
}

func (q *PartitionedQueue) IsLeader() bool {
	if q.raft == nil {
		return false
	}
	return q.raft.node.State() == raft.Leader
}

func (q *PartitionedQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.logger.Info("shutting down queue")

	// Close WAL
	if q.wal != nil {
		if err := q.wal.Close(); err != nil {
			q.logger.Error("failed to close WAL", "error", err)
		}
	}

	// Shutdown Raft if enabled
	if q.raft != nil {
		if err := q.raft.Shutdown(); err != nil {
			q.logger.Error("failed to shutdown raft", "error", err)
		}
	}

	return nil
}
