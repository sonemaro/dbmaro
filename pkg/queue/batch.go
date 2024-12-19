package queue

import (
	"sync"
	"time"

	"github.com/sonemaro/dbmaro/pkg/wal/logger"
)

type BatchManager struct {
	batch     *MessageBatch
	batchSize int
	timeout   time.Duration
	timer     *time.Timer
	messages  chan *Message
	flush     chan struct{}
	logger    *logger.Manager

	mu sync.Mutex
}

func NewBatchManager(config *Config, logger *logger.Manager) *BatchManager {
	bm := &BatchManager{
		batchSize: config.BatchSize,
		timeout:   time.Duration(config.BatchTimeoutMs) * time.Millisecond,
		messages:  make(chan *Message, config.BatchSize),
		flush:     make(chan struct{}),
		logger:    logger,
	}

	go bm.processBatches()
	return bm
}

func (bm *BatchManager) processBatches() {
	bm.timer = time.NewTimer(bm.timeout)

	for {
		select {
		case msg := <-bm.messages:
			bm.mu.Lock()
			if bm.batch == nil {
				bm.batch = &MessageBatch{
					Messages: make([]*Message, 0, bm.batchSize),
				}
			}
			bm.batch.Messages = append(bm.batch.Messages, msg)

			if len(bm.batch.Messages) >= bm.batchSize {
				bm.logger.Debug("batch full, flushing", "size", len(bm.batch.Messages))
				bm.flush <- struct{}{}
			}
			bm.mu.Unlock()

		case <-bm.timer.C:
			bm.mu.Lock()
			if bm.batch != nil && len(bm.batch.Messages) > 0 {
				bm.logger.Debug("batch timeout, flushing", "size", len(bm.batch.Messages))
				bm.flush <- struct{}{}
			}
			bm.timer.Reset(bm.timeout)
			bm.mu.Unlock()
		}
	}
}
