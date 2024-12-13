// wal_bench_test.go
package wal

import (
	"crypto/rand"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/sonemaro/dbmaro/pkg/wal/entry"
	"github.com/sonemaro/dbmaro/pkg/wal/options"
)

// Benchmark configurations
const (
	benchMaxTime = 3  // seconds per benchmark
	benchMaxSize = 10 // max size multiplier for payloads
)

func benchmarkConcurrentWAL(b *testing.B, numWriters int) {
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.wal")

	opts := options.DefaultOptions()
	opts.SyncStrategy = options.SyncBatch
	opts.InitialMapSize = 32 * 1024 * 1024

	wal, err := Open(path, opts)
	if err != nil {
		b.Fatalf("Failed to open WAL: %v", err)
	}
	defer func() {
		wal.Close()
		os.Remove(path)
	}()

	// Pre-generate batch of entries
	batchSize := 10
	payloadSize := 1024
	entries := make([]*entry.Entry, batchSize)
	for i := 0; i < batchSize; i++ {
		key := []byte(fmt.Sprintf("concurrent-key-%d", i))
		value := make([]byte, payloadSize)
		rand.Read(value)
		entries[i] = entry.NewEntry(key, value)
	}

	b.ResetTimer()
	b.SetBytes(int64(payloadSize * batchSize))
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := wal.WriteBatch(entries); err != nil {
				b.Errorf("WriteBatch failed: %v", err)
				return
			}
		}
	})
}

// Test configurations
var (
	poolConfigs = []struct {
		name string
		cfg  options.PoolConfig
	}{
		{
			"DefaultPool",
			options.DefaultPoolConfig(),
		},
		{
			"LargePool",
			options.PoolConfig{
				SmallBufferSize:       16 * 1024,
				MediumBufferSize:      256 * 1024,
				LargeBufferSize:       4 * 1024 * 1024,
				SmallPoolSize:         2048,
				MediumPoolSize:        256,
				LargePoolSize:         64,
				SmallBufferThreshold:  8 * 1024,
				MediumBufferThreshold: 128 * 1024,
			},
		},
	}

	checksumConfigs = []struct {
		name string
		cfg  options.ChecksumConfig
	}{
		{
			"IEEE",
			options.ChecksumConfig{
				Algorithm:       crc32.IEEE,
				PoolSize:        128,
				DirectThreshold: 4 * 1024,
			},
		},
		{
			"Castagnoli",
			options.ChecksumConfig{
				Algorithm:       crc32.Castagnoli,
				PoolSize:        128,
				DirectThreshold: 4 * 1024,
			},
		},
	}
)

func BenchmarkWAL(b *testing.B) {
	payloadSizes := []int{128, 1024, 10_240}
	batchSizes := []int{1, 10, 100}
	syncStrategies := []struct {
		name     string
		strategy options.SyncStrategy
	}{
		{"NoSync", options.SyncNever},
		{"SyncBatch", options.SyncBatch},
	}

	for _, poolCfg := range poolConfigs {
		for _, checksumCfg := range checksumConfigs {
			for _, size := range payloadSizes {
				for _, batch := range batchSizes {
					for _, sync := range syncStrategies {
						name := fmt.Sprintf(
							"Pool=%s/Checksum=%s/Size=%d/Batch=%d/%s",
							poolCfg.name,
							checksumCfg.name,
							size,
							batch,
							sync.name,
						)
						b.Run(name, func(b *testing.B) {
							benchmarkWALOperations(b, size, batch, sync.strategy, poolCfg.cfg, checksumCfg.cfg)
						})
					}
				}
			}
		}
	}
}

func benchmarkWALOperations(b *testing.B, payloadSize, batchSize int,
	syncStrategy options.SyncStrategy,
	poolCfg options.PoolConfig,
	checksumCfg options.ChecksumConfig) {

	dir := b.TempDir()
	path := filepath.Join(dir, "bench.wal")

	opts := options.DefaultOptions()
	opts.SyncStrategy = syncStrategy
	opts.InitialMapSize = 32 * 1024 * 1024
	opts.PoolConfig = poolCfg
	opts.ChecksumConfig = checksumCfg

	wal, err := Open(path, opts)
	if err != nil {
		b.Fatalf("Failed to open WAL: %v", err)
	}
	defer func() {
		wal.Close()
		os.Remove(path)
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
		if batchSize == 1 {
			if err := wal.Write(entries[0]); err != nil {
				b.Fatalf("Write failed: %v", err)
			}
		} else {
			if err := wal.WriteBatch(entries); err != nil {
				b.Fatalf("WriteBatch failed: %v", err)
			}
		}

		if syncStrategy == options.SyncBatch {
			if err := wal.Sync(); err != nil {
				b.Fatalf("Sync failed: %v", err)
			}
		}
	}
}

// Add memory statistics
// func (w *WAL) getPoolStats() string {
// 	return fmt.Sprintf(
// 		"SmallPool: %d, MediumPool: %d, LargePool: %d",
// 		w.bufferPool.Stats().SmallCount,
// 		w.bufferPool.Stats().MediumCount,
// 		w.bufferPool.Stats().LargeCount,
// 	)
// }

// Optional: Add detailed benchmarks for specific scenarios
func BenchmarkWALDetailed(b *testing.B) {
	// Test specific configurations
	cfg := options.PoolConfig{
		SmallBufferSize:       4 * 1024,
		MediumBufferSize:      64 * 1024,
		LargeBufferSize:       1 * 1024 * 1024,
		SmallPoolSize:         1024,
		MediumPoolSize:        128,
		LargePoolSize:         32,
		SmallBufferThreshold:  4 * 1024,
		MediumBufferThreshold: 64 * 1024,
	}

	b.Run("OptimizedConfig", func(b *testing.B) {
		opts := options.DefaultOptions()
		opts.PoolConfig = cfg
		opts.ChecksumConfig = options.ChecksumConfig{
			Algorithm:       crc32.IEEE,
			PoolSize:        128,
			DirectThreshold: 4 * 1024,
		}
		// ... rest of benchmark
	})
}
