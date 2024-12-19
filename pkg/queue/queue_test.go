// queue_test.go
package queue

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sonemaro/dbmaro/pkg/wal/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTest(t *testing.T) (*PartitionedQueue, func()) {
	tmpDir, err := os.MkdirTemp("", "queue-test-*")
	require.NoError(t, err)

	walPath := filepath.Join(tmpDir, "queue.wal")
	raftDir := filepath.Join(tmpDir, "raft")

	err = os.MkdirAll(raftDir, 0755)
	require.NoError(t, err)

	config := &Config{
		WalDir:             walPath,
		RaftDir:            raftDir,
		BatchSize:          100,
		BatchTimeoutMs:     50,
		MaxQueueSize:       1000,
		MaxPartitions:      4,
		MaxConsumerGroups:  2, // Reduced for testing
		MaxMembersPerGroup: 2,
		LogLevel:           "debug",
	}

	logger := logger.NewManager(true).SetLevel(logger.DEBUG)

	queue, err := NewPartitionedQueue(config, logger)
	require.NoError(t, err)

	cleanup := func() {
		if queue != nil && queue.wal != nil {
			queue.wal.Close()
		}
		os.RemoveAll(tmpDir)
	}

	return queue, cleanup
}

func TestQueueBasicOperations(t *testing.T) {
	queue, cleanup := setupTest(t)
	defer cleanup()

	t.Run("Enqueue single message batch", func(t *testing.T) {
		messages := []*Message{
			{
				ID:        1,
				Partition: 0,
				Payload:   []byte("test message 1"),
				Timestamp: time.Now(),
			},
		}

		err := queue.Enqueue(messages)
		assert.NoError(t, err)
	})

	t.Run("Enqueue multiple message batch", func(t *testing.T) {
		messages := make([]*Message, 10)
		for i := range messages {
			messages[i] = &Message{
				ID:        uint64(i + 100),
				Partition: i % queue.config.MaxPartitions,
				Payload:   []byte(fmt.Sprintf("test message %d", i)),
				Timestamp: time.Now(),
			}
		}

		err := queue.Enqueue(messages)
		assert.NoError(t, err)
	})

	t.Run("Queue full error", func(t *testing.T) {
		messages := make([]*Message, queue.config.MaxQueueSize+1)
		for i := range messages {
			messages[i] = &Message{
				ID:        uint64(i + 1000),
				Partition: 0,
				Payload:   []byte("overflow message"),
				Timestamp: time.Now(),
			}
		}

		err := queue.Enqueue(messages)
		assert.ErrorIs(t, err, ErrQueueFull)
	})
}

func TestConsumerGroups(t *testing.T) {
	queue, cleanup := setupTest(t)
	defer cleanup()

	t.Run("Create consumer group", func(t *testing.T) {
		group, err := queue.CreateConsumerGroup("test-group-1")
		assert.NoError(t, err)
		assert.NotNil(t, group)
		assert.Equal(t, "test-group-1", group.ID)
	})

	t.Run("Create duplicate consumer group", func(t *testing.T) {
		_, err := queue.CreateConsumerGroup("test-group-1")
		assert.ErrorIs(t, err, ErrConsumerGroupExists)
	})

	t.Run("Exceed max consumer groups", func(t *testing.T) {
		// First group already created above
		// Create one more (reaching max of 2)
		_, err := queue.CreateConsumerGroup("group-2")
		assert.NoError(t, err)

		// Try to create one more (exceeding max)
		_, err = queue.CreateConsumerGroup("group-3")
		assert.ErrorIs(t, err, ErrTooManyConsumerGroups)
	})

	t.Run("Add group member", func(t *testing.T) {
		// Use existing group
		err := queue.AddGroupMember("test-group-1", "member-1")
		assert.NoError(t, err)

		group, exists := queue.consumerGroups["test-group-1"]
		assert.True(t, exists)
		assert.Contains(t, group.Members, "member-1")
	})

	t.Run("Add member to non-existent group", func(t *testing.T) {
		err := queue.AddGroupMember("non-existent-group", "member-1")
		assert.ErrorIs(t, err, ErrConsumerGroupNotFound)
	})

	t.Run("Exceed max group members", func(t *testing.T) {
		groupID := "test-group-1"

		// Add members until max
		for i := 0; i < queue.config.MaxMembersPerGroup; i++ {
			memberID := fmt.Sprintf("member-%d", i)
			err := queue.AddGroupMember(groupID, memberID)
			if err != nil && err != ErrTooManyGroupMembers {
				assert.NoError(t, err)
			}
		}

		// Try to add one more
		err := queue.AddGroupMember(groupID, "one-too-many")
		assert.ErrorIs(t, err, ErrTooManyGroupMembers)
	})
}

func TestBatchProcessing(t *testing.T) {
	queue, cleanup := setupTest(t)
	defer cleanup()

	t.Run("Batch timeout", func(t *testing.T) {
		messages := []*Message{
			{
				ID:        1,
				Partition: 0,
				Payload:   []byte("test message 1"),
				Timestamp: time.Now(),
			},
		}

		err := queue.Enqueue(messages)
		assert.NoError(t, err)

		// Wait for batch timeout
		time.Sleep(time.Duration(queue.config.BatchTimeoutMs*2) * time.Millisecond)
	})

	t.Run("Batch size trigger", func(t *testing.T) {
		messages := make([]*Message, queue.config.BatchSize)
		for i := range messages {
			messages[i] = &Message{
				ID:        uint64(i + 1000),
				Partition: 0,
				Payload:   []byte(fmt.Sprintf("batch message %d", i)),
				Timestamp: time.Now(),
			}
		}

		err := queue.Enqueue(messages)
		assert.NoError(t, err)

		// Wait a bit for batch processing
		time.Sleep(50 * time.Millisecond)
	})
}

func TestQueueRecovery(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "queue-recovery-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	walPath := filepath.Join(tmpDir, "queue.wal") // Changed to file path
	raftDir := filepath.Join(tmpDir, "raft")

	config := &Config{
		WalDir:             walPath,
		RaftDir:            raftDir,
		BatchSize:          100,
		BatchTimeoutMs:     50,
		MaxQueueSize:       1000,
		MaxPartitions:      4,
		MaxConsumerGroups:  2,
		MaxMembersPerGroup: 2,
		LogLevel:           "debug",
	}

	logger := logger.NewManager(true).
		SetLevel(logger.DEBUG)

	t.Run("Recover after clean shutdown", func(t *testing.T) {
		// Create first queue instance
		queue1, err := NewPartitionedQueue(config, logger)
		require.NoError(t, err)

		// Enqueue some messages
		messages := make([]*Message, 10)
		for i := range messages {
			messages[i] = &Message{
				ID:        uint64(i + 1),
				Partition: 0,
				Payload:   []byte(fmt.Sprintf("recovery test message %d", i)),
				Timestamp: time.Now(),
				Status:    0,
			}
		}

		err = queue1.Enqueue(messages)
		assert.NoError(t, err)

		// Wait for batch processing
		time.Sleep(time.Duration(config.BatchTimeoutMs*2) * time.Millisecond)

		// Create second queue instance (simulating recovery)
		queue2, err := NewPartitionedQueue(config, logger)
		require.NoError(t, err)

		fmt.Println(queue2)

		// Verify state is recovered
		// Note: Add verification logic based on your implementation
	})
}
