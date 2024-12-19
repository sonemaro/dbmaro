// fsm.go
package queue

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/sonemaro/dbmaro/pkg/wal/logger"
)

// Command types for Raft operations
const (
	CmdEnqueue byte = iota
	CmdCreateGroup
	CmdAddMember
	CmdRemoveMember
	CmdDeleteGroup
	CmdAcknowledgeMessage
)

// Command represents a Raft log entry
type Command struct {
	Type    byte
	Payload []byte
}

// QueueFSM implements raft.FSM interface
type QueueFSM struct {
	queue  *PartitionedQueue
	logger *logger.Manager
	mu     sync.RWMutex
}

// Apply implements the raft.FSM interface
func (f *QueueFSM) Apply(log *raft.Log) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		f.logger.Error("failed to unmarshal command",
			"error", err,
			"index", log.Index,
			"term", log.Term)
		return nil
	}

	f.logger.Debug("applying command",
		"type", cmd.Type,
		"index", log.Index,
		"term", log.Term)

	switch cmd.Type {
	case CmdEnqueue:
		return f.applyEnqueue(cmd.Payload)
	case CmdCreateGroup:
		return f.applyCreateGroup(cmd.Payload)
	case CmdAddMember:
		return f.applyAddMember(cmd.Payload)
	case CmdRemoveMember:
		return f.applyRemoveMember(cmd.Payload)
	case CmdDeleteGroup:
		return f.applyDeleteGroup(cmd.Payload)
	case CmdAcknowledgeMessage:
		return f.applyAcknowledge(cmd.Payload)
	default:
		f.logger.Error("unknown command type",
			"type", cmd.Type,
			"index", log.Index)
		return fmt.Errorf("unknown command type: %d", cmd.Type)
	}
}

func (f *QueueFSM) applyEnqueue(payload []byte) interface{} {
	var messages []*Message
	if err := json.Unmarshal(payload, &messages); err != nil {
		f.logger.Error("failed to unmarshal messages", "error", err)
		return err
	}

	for _, msg := range messages {
		partition := f.queue.partitions[msg.Partition]
		partition.mu.Lock()
		partition.messages[msg.ID] = msg
		partition.mu.Unlock()
	}

	f.logger.Debug("applied enqueue command",
		"message_count", len(messages))
	return nil
}

func (f *QueueFSM) applyCreateGroup(payload []byte) interface{} {
	var groupID string
	if err := json.Unmarshal(payload, &groupID); err != nil {
		f.logger.Error("failed to unmarshal group ID", "error", err)
		return err
	}

	if len(f.queue.consumerGroups) >= f.queue.config.MaxConsumerGroups {
		return ErrTooManyConsumerGroups
	}

	if _, exists := f.queue.consumerGroups[groupID]; exists {
		return ErrConsumerGroupExists
	}

	group := &ConsumerGroup{
		ID:      groupID,
		Members: make(map[string]*Consumer),
		offsets: make(map[int]uint64),
	}

	f.queue.consumerGroups[groupID] = group
	f.logger.Info("created consumer group", "group_id", groupID)
	return nil
}

func (f *QueueFSM) applyAddMember(payload []byte) interface{} {
	var req struct {
		GroupID  string
		MemberID string
	}
	if err := json.Unmarshal(payload, &req); err != nil {
		f.logger.Error("failed to unmarshal member request", "error", err)
		return err
	}

	group, exists := f.queue.consumerGroups[req.GroupID]
	if !exists {
		return ErrConsumerGroupNotFound
	}

	if len(group.Members) >= f.queue.config.MaxMembersPerGroup {
		return ErrTooManyGroupMembers
	}

	group.Members[req.MemberID] = &Consumer{
		ID: req.MemberID,
	}

	f.logger.Info("added group member",
		"group_id", req.GroupID,
		"member_id", req.MemberID)
	return nil
}

func (f *QueueFSM) applyRemoveMember(payload []byte) interface{} {
	var req struct {
		GroupID  string
		MemberID string
	}
	if err := json.Unmarshal(payload, &req); err != nil {
		f.logger.Error("failed to unmarshal member request", "error", err)
		return err
	}

	group, exists := f.queue.consumerGroups[req.GroupID]
	if !exists {
		return ErrConsumerGroupNotFound
	}

	delete(group.Members, req.MemberID)
	f.logger.Info("removed group member",
		"group_id", req.GroupID,
		"member_id", req.MemberID)
	return nil
}

func (f *QueueFSM) applyDeleteGroup(payload []byte) interface{} {
	var groupID string
	if err := json.Unmarshal(payload, &groupID); err != nil {
		f.logger.Error("failed to unmarshal group ID", "error", err)
		return err
	}

	if _, exists := f.queue.consumerGroups[groupID]; !exists {
		return ErrConsumerGroupNotFound
	}

	delete(f.queue.consumerGroups, groupID)
	f.logger.Info("deleted consumer group", "group_id", groupID)

	return nil
}

func (f *QueueFSM) applyAcknowledge(payload []byte) interface{} {
	var req struct {
		GroupID   string
		Partition int
		MessageID uint64
	}
	if err := json.Unmarshal(payload, &req); err != nil {
		f.logger.Error("failed to unmarshal ack request", "error", err)
		return err
	}

	group, exists := f.queue.consumerGroups[req.GroupID]
	if !exists {
		return ErrConsumerGroupNotFound
	}

	group.offsets[req.Partition] = req.MessageID
	f.logger.WithFields(logger.Fields{
		"group_id":   req.GroupID,
		"partition":  req.Partition,
		"message_id": req.MessageID,
	}).Debug("acknowledged message")

	return nil
}

// QueueSnapshot implements the raft.FSMSnapshot interface
type QueueSnapshot struct {
	queue  *PartitionedQueue
	logger *logger.Manager
}

// Persist implements the raft.FSMSnapshot interface
func (s *QueueSnapshot) Persist(sink raft.SnapshotSink) error {
	s.logger.Info("starting snapshot persistence")

	// create snapshot data structure
	snapshot := struct {
		Messages       map[int]map[uint64]*Message
		ConsumerGroups map[string]*ConsumerGroup
	}{
		Messages:       make(map[int]map[uint64]*Message),
		ConsumerGroups: s.queue.consumerGroups,
	}

	// copy messages from all partitions
	for i, partition := range s.queue.partitions {
		partition.mu.RLock()
		messages := make(map[uint64]*Message)
		for id, msg := range partition.messages {
			messages[id] = msg
		}

		snapshot.Messages[i] = messages
		partition.mu.RUnlock()
	}

	// encode snapshot
	data, err := json.Marshal(snapshot)
	if err != nil {
		s.logger.Error("failed to marshal snapshot", "error", err)
		sink.Cancel()

		return fmt.Errorf("failed to marshal snapshot: %w", err)
	}

	// Write to sink
	if _, err := sink.Write(data); err != nil {
		s.logger.Error("failed to write snapshot", "error", err)
		sink.Cancel()

		return fmt.Errorf("failed to write snapshot: %w", err)
	}

	if err := sink.Close(); err != nil {
		s.logger.Error("failed to close snapshot", "error", err)

		return fmt.Errorf("failed to close snapshot: %w", err)
	}

	s.logger.Info("snapshot persistence completed")
	return nil
}

// Release implements the raft.FSMSnapshot interface
func (s *QueueSnapshot) Release() {
	s.logger.Debug("releasing snapshot resources")
}

// Snapshot implements the raft.FSM interface
func (f *QueueFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	f.logger.Info("creating FSM snapshot")

	return &QueueSnapshot{
		queue:  f.queue,
		logger: f.logger,
	}, nil
}

// Restore implements the raft.FSM interface
func (f *QueueFSM) Restore(rc io.ReadCloser) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.logger.Info("starting FSM restore")

	// read snapshot data
	var snapshot struct {
		Messages       map[int]map[uint64]*Message
		ConsumerGroups map[string]*ConsumerGroup
	}

	decoder := json.NewDecoder(rc)
	if err := decoder.Decode(&snapshot); err != nil {
		return fmt.Errorf("failed to decode snapshot: %w", err)
	}

	// restore messages to partitions
	for partitionID, messages := range snapshot.Messages {
		if partitionID >= len(f.queue.partitions) {
			f.logger.WithFields(logger.Fields{
				"partition_id": partitionID,
			}).Error("invalid partition ID in snapshot")

			continue
		}

		partition := f.queue.partitions[partitionID]
		partition.mu.Lock()
		partition.messages = messages
		partition.mu.Unlock()
	}

	// restore consumer groups
	f.queue.consumerGroups = snapshot.ConsumerGroups

	f.logger.WithFields(logger.Fields{
		"partitions":      len(snapshot.Messages),
		"consumer_groups": len(snapshot.ConsumerGroups),
	}).Info("FSM restore completed")

	return nil
}
