// wal_read_bench_test.go

package wal

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/sonemaro/dbmaro/pkg/wal/entry"
	"github.com/sonemaro/dbmaro/pkg/wal/options"
)

// Test data configuration
const (
	benchDataSize   = 100 * 1024 * 1024 // 100MB of test data
	maxEntrySize    = 10 * 1024         // 10KB max entry size
	sequentialReads = 1000              // Number of sequential reads
	randomReads     = 1000              // Number of random reads
	batchReadSize   = 100               // Size of read batches
)

// prepareBenchData creates test data for benchmarking
func prepareBenchData(b *testing.B, wal *WAL, config benchConfig) []int64 {
	b.Helper()
	offsets := make([]int64, 0, config.numEntries)

	// Create entries with specified size
	for i := 0; i < config.numEntries; i++ {
		data := make([]byte, config.entrySize)
		rand.Read(data)
		e := entry.NewEntry([]byte(fmt.Sprintf("key-%d", i)), data)

		if err := wal.Write(e); err != nil {
			b.Fatalf("Failed to write test data: %v", err)
		}
		offsets = append(offsets, wal.offset.Load())
	}

	return offsets
}

type benchConfig struct {
	entrySize  int
	numEntries int
	batchSize  int
}

func BenchmarkWALRead(b *testing.B) {
	configs := []benchConfig{
		{entrySize: 128, numEntries: 1000, batchSize: 1},     // Small entries
		{entrySize: 1024, numEntries: 1000, batchSize: 1},    // Medium entries
		{entrySize: 10240, numEntries: 1000, batchSize: 1},   // Large entries
		{entrySize: 128, numEntries: 1000, batchSize: 100},   // Small batch
		{entrySize: 1024, numEntries: 1000, batchSize: 100},  // Medium batch
		{entrySize: 10240, numEntries: 1000, batchSize: 100}, // Large batch
	}

	for _, cfg := range configs {
		// Sequential read benchmark
		b.Run(fmt.Sprintf("SequentialRead/Size=%d/Batch=%d", cfg.entrySize, cfg.batchSize), func(b *testing.B) {
			benchmarkSequentialRead(b, cfg)
		})

		// Random read benchmark
		b.Run(fmt.Sprintf("RandomRead/Size=%d/Batch=%d", cfg.entrySize, cfg.batchSize), func(b *testing.B) {
			benchmarkRandomRead(b, cfg)
		})

		// Range read benchmark
		b.Run(fmt.Sprintf("RangeRead/Size=%d/Batch=%d", cfg.entrySize, cfg.batchSize), func(b *testing.B) {
			benchmarkRangeRead(b, cfg)
		})
	}
}

func benchmarkSequentialRead(b *testing.B, cfg benchConfig) {
	// Setup WAL with test data
	dir := b.TempDir()
	wal, err := Open(dir+"/bench.wal", options.DefaultOptions())
	if err != nil {
		b.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	offsets := prepareBenchData(b, wal, cfg)
	fmt.Println(offsets)

	// Reset timer for actual benchmark
	b.ResetTimer()
	b.SetBytes(int64(cfg.entrySize))

	for i := 0; i < b.N; i++ {
		it := wal.Iterator()
		count := 0
		for {
			_, err := it.Next()
			if err != nil {
				b.Fatalf("Iterator error: %v", err)
			}
			count++
			if count >= cfg.numEntries {
				break
			}
		}
	}
}

func benchmarkRandomRead(b *testing.B, cfg benchConfig) {
	dir := b.TempDir()
	wal, err := Open(dir+"/bench.wal", options.DefaultOptions())
	if err != nil {
		b.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	offsets := prepareBenchData(b, wal, cfg)

	b.ResetTimer()
	b.SetBytes(int64(cfg.entrySize))

	for i := 0; i < b.N; i++ {
		// Use Iterator instead of direct Read
		it := wal.Iterator()
		// Seek to random offset
		idx := rand.Intn(len(offsets))
		for j := 0; j < idx; j++ {
			_, err := it.Next()
			if err != nil {
				b.Fatalf("Iterator error: %v", err)
			}
		}
		entry, err := it.Next()
		if err != nil {
			b.Fatalf("Read error: %v", err)
		}
		if entry == nil {
			b.Fatal("Unexpected nil entry")
		}
	}
}

func benchmarkRangeRead(b *testing.B, cfg benchConfig) {
	dir := b.TempDir()
	wal, err := Open(dir+"/bench.wal", options.DefaultOptions())
	if err != nil {
		b.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	offsets := prepareBenchData(b, wal, cfg)
	fmt.Println(offsets)

	b.ResetTimer()
	b.SetBytes(int64(cfg.entrySize * cfg.batchSize))

	for i := 0; i < b.N; i++ {
		// Use Iterator to read range
		it := wal.Iterator()
		entries := make([]*entry.Entry, 0, cfg.batchSize)

		// Read batch size number of entries
		for j := 0; j < cfg.batchSize; j++ {
			entry, err := it.Next()
			if err != nil {
				b.Fatalf("Range read error: %v", err)
			}
			if entry == nil {
				break
			}
			entries = append(entries, entry)
		}

		if len(entries) < cfg.batchSize {
			b.Fatalf("Insufficient entries read: got %d, want %d",
				len(entries), cfg.batchSize)
		}
	}
}

// Optional: Benchmark with different read patterns
func BenchmarkWALReadPatterns(b *testing.B) {
	patterns := []struct {
		name     string
		pattern  func([]int64) int64
		readSize int
	}{
		{"Sequential", func(offsets []int64) int64 { return offsets[0] }, 1024},
		{"Random", func(offsets []int64) int64 {
			return offsets[rand.Intn(len(offsets))]
		}, 1024},
		{"HotSpot", func(offsets []int64) int64 {
			// 80% reads from 20% of data
			if rand.Float32() < 0.8 {
				return offsets[rand.Intn(len(offsets)/5)]
			}
			return offsets[rand.Intn(len(offsets))]
		}, 1024},
	}

	for _, p := range patterns {
		b.Run(p.name, func(b *testing.B) {
			// Setup and run pattern-specific benchmark
			// ... implementation ...
		})
	}
}

// Optional: Benchmark concurrent reads
func BenchmarkWALConcurrentRead(b *testing.B) {
	concurrencyLevels := []int{2, 4, 8, 16}

	for _, level := range concurrencyLevels {
		b.Run(fmt.Sprintf("Readers=%d", level), func(b *testing.B) {
			// Setup concurrent read benchmark
			// ... implementation ...
		})
	}
}
