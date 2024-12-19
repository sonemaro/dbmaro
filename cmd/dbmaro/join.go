package main

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sonemaro/dbmaro/pkg/queue"
	"github.com/sonemaro/dbmaro/pkg/wal/logger"
	"github.com/spf13/cobra"
)

var (
	leaderAddress   string
	nodeAddress     string
	joinWalDir      string
	joinRaftDir     string
	joinHttpAddress string
)

var joinCmd = &cobra.Command{
	Use:   "join",
	Short: "Join an existing cluster",
	Run:   runJoin,
}

func init() {
	joinCmd.Flags().StringVar(&leaderAddress, "leader", "", "Leader node address")
	joinCmd.Flags().StringVar(&nodeAddress, "address", "", "This node's Raft address")
	joinCmd.Flags().StringVar(&walDir, "wal-dir", "", "WAL directory path")
	joinCmd.Flags().StringVar(&raftDir, "raft-dir", "", "Raft directory path")
	joinCmd.Flags().StringVar(&httpAddress, "http-address", "", "HTTP server address")

	joinCmd.MarkFlagRequired("leader")
	joinCmd.MarkFlagRequired("address")
	joinCmd.MarkFlagRequired("wal-dir")
	joinCmd.MarkFlagRequired("raft-dir")
	joinCmd.MarkFlagRequired("http-address")
}

func runJoin(cmd *cobra.Command, args []string) {
	// TODO read values from config
	config := &queue.Config{
		WalDir:         walDir,
		RaftDir:        raftDir,
		RaftAddress:    nodeAddress,
		RaftPeers:      []string{leaderAddress},
		MaxPartitions:  16,
		MaxQueueSize:   1000000,
		BatchSize:      1000,
		BatchTimeoutMs: 100,
		LogLevel:       logLevel,
		Bootstrap:      false, // don't bootstrap when joining
	}

	// TODO read the verbosity and log level from config
	log := logger.NewManager(logLevel == "debug").SetLevel(parseLogLevel(logLevel))

	// Create directories if they don't exist
	err := os.MkdirAll(raftDir, 0755)
	if err != nil {
		log.Error("failed to create the raft directory:", err)
		os.Exit(1)
	}

	q, err := queue.NewPartitionedQueue(config, log)
	if err != nil {
		log.Error("failed to start queue:", err)
		os.Exit(1)
	}

	// start HTTP server
	if err := q.StartHTTPServer(httpAddress); err != nil {
		log.Error("failed to start HTTP server: ", err)
		os.Exit(1)
	}

	log.WithFields(logger.Fields{
		"raft_address":   nodeAddress,
		"http_address":   httpAddress,
		"leader_address": leaderAddress,
	}).Info("server started")

	// wait a bit for Raft server to start
	// TODO read from config
	time.Sleep(2 * time.Second)

	// try to join the cluster
	// TODO read from config
	maxRetries := 10
	retryInterval := time.Second * 3

	// TODO read from config
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	var lastErr error
	for i := 0; i < maxRetries; i++ {
		if i > 0 {
			log.WithFields(logger.Fields{
				"attemp": i + 1,
				"max":    maxRetries,
			}).Info("retrying join attempt")

			time.Sleep(retryInterval)
		}

		joinURL := fmt.Sprintf("http://%s/cluster/join", leaderAddress)
		values := url.Values{}
		values.Set("address", nodeAddress)

		resp, err := client.PostForm(joinURL, values)
		if err != nil {
			lastErr = fmt.Errorf("failed to connect to leader: %w", err)
			continue
		}

		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == http.StatusServiceUnavailable {
			lastErr = fmt.Errorf("leader not ready: %s", string(body))
			continue
		}

		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("failed to join cluster, status: %d, message: %s",
				resp.StatusCode, string(body))

			if resp.StatusCode == http.StatusBadRequest {
				break
			}

			continue
		}

		log.Info("successfully joined cluster", "leader", leaderAddress)

		// handle shutdown gracefully
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

		<-sigChan
		log.Info("shutting down...")
		q.Close()
		return
	}

	log.WithFields(logger.Fields{
		"max_retries": maxRetries,
		"error":       lastErr,
	}).Error("failed to join cluster")

	q.Close()
	os.Exit(1)
}
