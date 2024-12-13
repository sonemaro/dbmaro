package wal

/*
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║                         WAL Manager v1.0                                     ║
║                         ===============                                      ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝

WAL Manager is designed to handle multiple Write-Ahead Logs efficiently, optimizing
for batch operations and large data volumes. It provides intelligent routing and
load balancing across multiple WAL instances.

Core Features:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  • Multiple WAL management
  • Batch operation optimization
  • Size-based routing
  • Load balancing
  • Metrics collection

Usage Example:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // Initialize manager
    manager, err := NewWALManager("/data/wal", 2, opts)
    if err != nil {
        log.Fatal(err)
    }
    defer manager.Close()

    // Batch write
    entries := []*entry.Entry{...}
    err = manager.WriteBatch(entries)

Configuration:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  WAL Count: Number of WAL instances to manage
  Threshold: Size threshold for routing decisions
  Options: Standard WAL options applied to each instance

Performance Considerations:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  • Batch operations are more efficient than single writes
  • Larger payloads generally provide better throughput
  • Multiple WALs can improve performance for concurrent writes
  • Size-based routing helps balance load across WALs

Metrics:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  BatchWrites: Total number of batch operations
  BytesWritten: Total bytes written
  AvgBatchSize: Average size of batches
  MaxThroughput: Peak write throughput

Error Handling:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  • WAL initialization errors
  • Write operation failures
  • Resource cleanup errors

Best Practices:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  1. Group related writes into batches
  2. Consider payload sizes when batching
  3. Monitor metrics for performance optimization
  4. Properly handle cleanup with Close()

Notes:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  - Thread-safe for concurrent operations
  - Automatic load balancing across WALs
  - Metrics are atomically updated
  - Clean shutdown important for data integrity

╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║  "The art of aggregation is knowing when to combine and when to separate"    ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
*/

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/sonemaro/dbmaro/pkg/wal/entry"
	"github.com/sonemaro/dbmaro/pkg/wal/options"
)

const (
	// Batch size thresholds for testing
	smallBatchThreshold  = 256 * 1024  // 256KB
	mediumBatchThreshold = 512 * 1024  // 512KB
	largeBatchThreshold  = 1024 * 1024 // 1MB
)

type WALManager struct {
	wals           []*WAL
	numWALs        int
	metrics        *ManagerMetrics
	mu             sync.RWMutex
	walSizes       []atomic.Int64
	currentWAL     atomic.Int32
	batchThreshold int64
}

type ManagerMetrics struct {
	BatchWrites   atomic.Int64
	BytesWritten  atomic.Int64
	AvgBatchSize  atomic.Int64
	MaxThroughput atomic.Int64
}

func NewWALManager(baseDir string, numWALs int, opts options.Options) (*WALManager, error) {
	if numWALs <= 0 {
		numWALs = 1
	}

	m := &WALManager{
		wals:           make([]*WAL, numWALs),
		numWALs:        numWALs,
		metrics:        &ManagerMetrics{},
		walSizes:       make([]atomic.Int64, numWALs),
		batchThreshold: mediumBatchThreshold, // Default threshold
	}

	// Initialize WALs
	for i := 0; i < numWALs; i++ {
		walPath := fmt.Sprintf("%s/wal-%d", baseDir, i)
		wal, err := Open(walPath, opts)
		if err != nil {
			m.Close()
			return nil, fmt.Errorf("failed to open WAL %d: %w", i, err)
		}
		m.wals[i] = wal
	}

	return m, nil
}

func totalSize(entries []*entry.Entry) int64 {
	var size int64
	for _, e := range entries {
		size += e.Size()
	}
	return size
}

func (m *WALManager) selectOptimalWAL(batchSize int64) int {
	// Optimized for 2 WALs
	if m.numWALs == 2 {
		if batchSize < m.batchThreshold {
			return int(m.currentWAL.Add(1) & 1)
		}
		// For large batches, use WAL with less data
		if m.walSizes[0].Load() <= m.walSizes[1].Load() {
			return 0
		}
		return 1
	}

	// For other configurations
	if batchSize < m.batchThreshold {
		return int(m.currentWAL.Add(1)) % m.numWALs
	}

	// Find WAL with least data
	minSize := m.walSizes[0].Load()
	optimalWAL := 0

	for i := 1; i < m.numWALs; i++ {
		size := m.walSizes[i].Load()
		if size < minSize {
			minSize = size
			optimalWAL = i
		}
	}

	return optimalWAL
}

func (m *WALManager) WriteBatch(entries []*entry.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	batchSize := totalSize(entries)
	walIdx := m.selectOptimalWAL(batchSize)

	err := m.wals[walIdx].WriteBatch(entries)
	if err != nil {
		return fmt.Errorf("WAL %d batch write failed: %w", walIdx, err)
	}

	// Update metrics and WAL size
	m.metrics.BatchWrites.Add(1)
	m.metrics.BytesWritten.Add(batchSize)
	m.walSizes[walIdx].Add(batchSize)

	return nil
}

func (m *WALManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var lastErr error
	for i, wal := range m.wals {
		if wal != nil {
			if err := wal.Close(); err != nil {
				lastErr = fmt.Errorf("WAL %d close failed: %w", i, err)
			}
		}
	}
	return lastErr
}

func (m *WALManager) Metrics() *ManagerMetrics {
	return m.metrics
}

// SetBatchThreshold allows tuning the batch size threshold
func (m *WALManager) SetBatchThreshold(threshold int64) {
	m.batchThreshold = threshold
}
