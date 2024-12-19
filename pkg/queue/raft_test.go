// raft_test.go
package queue

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/sonemaro/dbmaro/pkg/wal/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var basePort = 15000 // Starting from a higher port number to avoid conflicts

func bootstrapRaft(node *PartitionedQueue) error {
	// Bootstrap as a single node cluster initially
	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(node.config.RaftAddress),
				Address: raft.ServerAddress(node.config.RaftAddress),
			},
		},
	}

	return node.raft.node.BootstrapCluster(configuration).Error()
}

func setupTestNode(t *testing.T, nodeID string, testID int) (*PartitionedQueue, func()) {
	tmpDir, err := os.MkdirTemp("", fmt.Sprintf("raft-test-%s-*", nodeID))
	require.NoError(t, err)

	walDir := filepath.Join(tmpDir, "wal")
	raftDir := filepath.Join(tmpDir, "raft")
	err = os.MkdirAll(walDir, 0755)
	require.NoError(t, err)
	err = os.MkdirAll(raftDir, 0755)
	require.NoError(t, err)

	port := basePort + (testID * 10) + func() int {
		id := 0
		fmt.Sscanf(nodeID, "%d", &id)
		return id
	}()

	addr := fmt.Sprintf("127.0.0.1:%d", port)

	config := &Config{
		WalDir:             filepath.Join(walDir, "queue.wal"),
		RaftDir:            raftDir,
		RaftAddress:        addr,
		BatchSize:          100,
		BatchTimeoutMs:     50,
		MaxQueueSize:       1000,
		MaxPartitions:      4,
		MaxConsumerGroups:  2,
		MaxMembersPerGroup: 2,
		LogLevel:           "debug",
	}

	log := logger.NewManager(true).SetLevel(logger.DEBUG)

	queue, err := NewPartitionedQueue(config, log)
	require.NoError(t, err)

	// Bootstrap only if this is the first node
	if nodeID == "1" {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					Suffrage: raft.Voter,
					ID:       raft.ServerID(addr),
					Address:  raft.ServerAddress(addr),
				},
			},
		}
		f := queue.raft.node.BootstrapCluster(configuration)
		require.NoError(t, f.Error())

		// Wait for leadership
		time.Sleep(3 * time.Second)
		require.Eventually(t, func() bool {
			return queue.raft.node.State() == raft.Leader
		}, 5*time.Second, 100*time.Millisecond, "First node should become leader")
	}

	cleanup := func() {
		if queue != nil {
			if queue.raft != nil {
				_ = queue.raft.Shutdown()
				time.Sleep(500 * time.Millisecond)
			}
			if queue.wal != nil {
				queue.wal.Close()
			}
		}
		os.RemoveAll(tmpDir)
		time.Sleep(500 * time.Millisecond)
	}

	return queue, cleanup
}

func TestThreeNodeCluster(t *testing.T) {
	testID := 1

	// Create and bootstrap first node
	node1, cleanup1 := setupTestNode(t, "1", testID)
	defer cleanup1()

	// Wait for the first node to become leader
	time.Sleep(1 * time.Second)
	require.Equal(t, "Leader", node1.raft.node.State().String(), "First node should become leader")

	// Create other nodes (don't bootstrap them)
	node2, cleanup2 := setupTestNode(t, "2", testID)
	defer cleanup2()

	node3, cleanup3 := setupTestNode(t, "3", testID)
	defer cleanup3()

	t.Run("Join cluster", func(t *testing.T) {
		// Add node2 and node3 to the cluster
		err := node1.raft.AddPeer("2", node2.config.RaftAddress)
		assert.NoError(t, err, "Should be able to add node2")

		err = node1.raft.AddPeer("3", node3.config.RaftAddress)
		assert.NoError(t, err, "Should be able to add node3")

		// Wait for cluster to stabilize
		time.Sleep(2 * time.Second)

		// Verify only one leader
		leaderCount := 0
		for _, node := range []*PartitionedQueue{node1, node2, node3} {
			if node.raft.node.State().String() == "Leader" {
				leaderCount++
			}
		}
		assert.Equal(t, 1, leaderCount, "Should have exactly one leader")
	})

	t.Run("Message replication", func(t *testing.T) {
		// Find the leader
		var leader *PartitionedQueue
		for _, node := range []*PartitionedQueue{node1, node2, node3} {
			if node.raft.node.State().String() == "Leader" {
				leader = node
				break
			}
		}
		require.NotNil(t, leader, "Should have a leader")

		// Enqueue messages through leader
		messages := []*Message{
			{
				ID:        1,
				Partition: 0,
				Payload:   []byte("replicated message"),
				Timestamp: time.Now(),
			},
		}

		err := leader.Enqueue(messages)
		assert.NoError(t, err, "Should be able to enqueue messages")

		// Wait for replication
		time.Sleep(1 * time.Second)
	})
}

func TestLeaderFailover(t *testing.T) {
	testID := 2

	// Create and bootstrap first node
	node1, cleanup1 := setupTestNode(t, "1", testID)
	defer cleanup1()

	// Create other nodes
	node2, cleanup2 := setupTestNode(t, "2", testID)
	defer cleanup2()

	node3, cleanup3 := setupTestNode(t, "3", testID)
	defer cleanup3()

	// Add nodes to cluster and wait for stability
	err := node1.raft.AddPeer("2", node2.config.RaftAddress)
	require.NoError(t, err)
	time.Sleep(3 * time.Second)

	err = node1.raft.AddPeer("3", node3.config.RaftAddress)
	require.NoError(t, err)
	time.Sleep(3 * time.Second)

	// Verify cluster configuration
	future := node1.raft.node.GetConfiguration()
	require.NoError(t, future.Error())
	cfg := future.Configuration()
	require.Len(t, cfg.Servers, 3, "Should have three servers in configuration")

	t.Run("Leader failover", func(t *testing.T) {
		// Find current leader and followers
		var leader *PartitionedQueue
		var followers []*PartitionedQueue
		for _, node := range []*PartitionedQueue{node1, node2, node3} {
			if node.raft.node.State() == raft.Leader {
				leader = node
			} else {
				followers = append(followers, node)
			}
		}
		require.NotNil(t, leader, "Should have a leader")
		require.Len(t, followers, 2, "Should have two followers")

		// Wait for cluster to stabilize
		time.Sleep(3 * time.Second)

		// Shutdown leader gracefully
		err := leader.raft.node.Shutdown()
		assert.NoError(t, err.Error(), "Leader shutdown should succeed")

		// Wait for new leader election
		time.Sleep(10 * time.Second)

		// Verify new leader is elected
		newLeaderFound := false
		for _, node := range followers {
			if node.raft.node.State() == raft.Leader {
				newLeaderFound = true
				break
			}
		}
		assert.True(t, newLeaderFound, "New leader should be elected")
	})
}
