package queue

import (
	"time"
)

type MessageStatus int

type Message struct {
	ID        uint64
	Partition int
	Payload   []byte
	Timestamp time.Time
	Status    MessageStatus
}

type MessageBatch struct {
	Messages []*Message
	BatchID  uint64
}
