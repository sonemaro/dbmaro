package wal

import (
	"crypto/rand"
	"fmt"
	"os"
	"testing"

	"github.com/sonemaro/dbmaro/pkg/wal/entry"
	"github.com/sonemaro/dbmaro/pkg/wal/options"
)

func BenchmarkWALManager(b *testing.B) {
	payloadSizes := []int{
		4 * 1024,   // 4KB
		8 * 1024,   // 8KB
		16 * 1024,  // 16KB
		32 * 1024,  // 32KB
		64 * 1024,  // 64KB
		128 * 1024, // 128KB
	}
	batchSizes := []int{50, 100, 200} // Test different batch sizes
	walCounts := []int{1, 2, 4}       // Focus on most promising WAL counts
	thresholds := []int64{
		256 * 1024, // 256KB (best so far)
		512 * 1024, // 512KB (for verification)
	}

	for _, threshold := range thresholds {
		for _, numWALs := range walCounts {
			for _, payloadSize := range payloadSizes {
				for _, batchSize := range batchSizes {
					name := fmt.Sprintf(
						"WALs=%d/Payload=%dKB/Batch=%d/Threshold=%dKB",
						numWALs,
						payloadSize/1024,
						batchSize,
						threshold/1024,
					)

					b.Run(name, func(b *testing.B) {
						benchmarkManagerBatch(b, numWALs, payloadSize, batchSize, threshold)
					})
				}
			}
		}
	}
}

func benchmarkManagerBatch(b *testing.B, numWALs, payloadSize, batchSize int, threshold int64) {
	dir := b.TempDir()

	opts := options.DefaultOptions()
	opts.SyncStrategy = options.SyncBatch
	opts.InitialMapSize = 32 * 1024 * 1024

	manager, err := NewWALManager(dir, numWALs, opts)
	if err != nil {
		b.Fatalf("Failed to create WAL manager: %v", err)
	}
	manager.SetBatchThreshold(threshold)

	defer func() {
		manager.Close()
		os.RemoveAll(dir)
	}()

	// Pre-generate test data
	entries := make([]*entry.Entry, batchSize)
	for i := 0; i < batchSize; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := make([]byte, payloadSize)
		rand.Read(value)
		entries[i] = entry.NewEntry(key, value)
	}

	b.ResetTimer()
	b.SetBytes(int64(payloadSize * batchSize))

	for i := 0; i < b.N; i++ {
		if err := manager.WriteBatch(entries); err != nil {
			b.Fatalf("WriteBatch failed: %v", err)
		}
	}
}
