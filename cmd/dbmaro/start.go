// cmd/dbmaro/start.go
package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sonemaro/dbmaro/pkg/queue"
	"github.com/sonemaro/dbmaro/pkg/wal/logger"
	"github.com/spf13/cobra"
)

var (
	walDir         string
	raftDir        string
	raftAddress    string
	httpAddress    string
	raftPeers      []string
	maxPartitions  int
	maxQueueSize   int64
	batchSize      int
	batchTimeoutMs int
	bootstrap      bool
	leaderTimeout  int
)

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start a DBMaro node",
	Run:   runStart,
}

func init() {
	startCmd.Flags().StringVar(&walDir, "wal-dir", "", "WAL directory path")
	startCmd.Flags().StringVar(&raftDir, "raft-dir", "", "Raft directory path")
	startCmd.Flags().StringVar(&raftAddress, "raft-address", "", "Raft node address")
	startCmd.Flags().StringSliceVar(&raftPeers, "raft-peers", []string{}, "Raft peer addresses")
	startCmd.Flags().IntVar(&maxPartitions, "max-partitions", 16, "Maximum number of partitions")
	startCmd.Flags().Int64Var(&maxQueueSize, "max-queue-size", 1000000, "Maximum queue size")
	startCmd.Flags().IntVar(&batchSize, "batch-size", 1000, "Batch size for writes")
	startCmd.Flags().IntVar(&batchTimeoutMs, "batch-timeout", 100, "Batch timeout in milliseconds")
	startCmd.Flags().StringVar(&httpAddress, "http-address", ":8000", "HTTP server address")
	startCmd.Flags().BoolVar(&bootstrap, "bootstrap", false, "Bootstrap as first node")
	startCmd.Flags().IntVar(&leaderTimeout, "leader-timeout", 30, "Seconds to wait for leadership")

	startCmd.MarkFlagRequired("wal-dir")
	startCmd.MarkFlagRequired("raft-dir")
	startCmd.MarkFlagRequired("raft-address")
}

func runStart(cmd *cobra.Command, args []string) {
	log := logger.NewManager(logLevel == "debug").SetLevel(parseLogLevel(logLevel))

	config := &queue.Config{
		WalDir:         walDir,
		RaftDir:        raftDir,
		RaftAddress:    raftAddress,
		RaftPeers:      raftPeers,
		MaxPartitions:  maxPartitions,
		MaxQueueSize:   maxQueueSize,
		BatchSize:      batchSize,
		BatchTimeoutMs: batchTimeoutMs,
		LogLevel:       logLevel,
		Bootstrap:      bootstrap,
	}

	q, err := queue.NewPartitionedQueue(config, log)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start queue: %v\n", err)
		os.Exit(1)
	}

	// wait for leadership if bootstrapping
	if bootstrap {
		log.Info("waiting for leadership", "timeout", leaderTimeout)
		deadline := time.Now().Add(time.Duration(leaderTimeout) * time.Second)
		for time.Now().Before(deadline) {
			if q.IsLeader() {
				log.Info("became leader")

				break
			}

			time.Sleep(time.Second)
		}

		if !q.IsLeader() {
			log.Error("failed to become leader within timeout")
			os.Exit(1)
		}
	}

	if err := q.StartHTTPServer(httpAddress); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start HTTP server: %v\n", err)
		os.Exit(1)
	}

	log.WithFields(logger.Fields{
		"raft_address": raftAddress,
		"http_address": httpAddress,
		"is_leader":    q.IsLeader(),
	}).Info("Server started")

	// handle shutdown gracefully
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	log.Info("Shutting down...")
	q.Close()
}
