package queue

import "sync"

type Partition struct {
	ID            int
	currentOffset uint64
	messages      map[uint64]*Message
	mu            sync.RWMutex
}

func NewPartition(id int) *Partition {
	return &Partition{
		ID:       id,
		messages: make(map[uint64]*Message),
	}
}
