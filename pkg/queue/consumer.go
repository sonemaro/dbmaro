package queue

import (
	"sync"

	"github.com/sonemaro/dbmaro/pkg/wal/logger"
)

type Consumer struct {
	ID        string
	Partition int
	Offset    uint64
}

type ConsumerGroup struct {
	ID      string
	Members map[string]*Consumer

	partitions []int
	offsets    map[int]uint64

	mu sync.RWMutex
}

func (q *PartitionedQueue) CreateConsumerGroup(groupID string) (*ConsumerGroup, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.consumerGroups) >= q.config.MaxConsumerGroups {
		q.logger.WithFields(logger.Fields{
			"current": len(q.consumerGroups),
			"max":     q.config.MaxConsumerGroups,
		}).Warn("too many consumer groups")

		return nil, ErrTooManyConsumerGroups
	}

	if _, exists := q.consumerGroups[groupID]; exists {
		q.logger.WithFields(logger.Fields{
			"group_id": groupID,
		}).Warn("consumer group already exists")

		return nil, ErrConsumerGroupExists
	}

	group := &ConsumerGroup{
		ID:      groupID,
		Members: make(map[string]*Consumer),
		offsets: make(map[int]uint64),
	}

	q.consumerGroups[groupID] = group

	q.logger.WithFields(logger.Fields{
		"group_id": groupID,
	}).Info("created consumer group")

	return group, nil
}

func (q *PartitionedQueue) AddGroupMember(groupID, memberID string) error {
	group, exists := q.consumerGroups[groupID]
	if !exists {
		return ErrConsumerGroupNotFound
	}

	group.mu.Lock()
	defer group.mu.Unlock()

	if len(group.Members) >= q.config.MaxMembersPerGroup {
		q.logger.WithFields(logger.Fields{
			"current": len(group.Members),
			"max":     q.config.MaxMembersPerGroup,
		}).Warn("too many group members")

		return ErrTooManyGroupMembers
	}

	group.Members[memberID] = &Consumer{
		ID: memberID,
	}

	q.logger.WithFields(logger.Fields{
		"group_id":  groupID,
		"member_id": memberID,
	}).Info("added group member")

	return nil
}
