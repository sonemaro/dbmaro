// recovery_test.go
package wal

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/sonemaro/dbmaro/pkg/wal/entry"
	"github.com/sonemaro/dbmaro/pkg/wal/options"
)

func TestWALRecovery(t *testing.T) {
	testCases := []struct {
		name          string
		numEntries    int
		batchSize     int
		payloadSize   int
		crashPosition float64
		corrupt       bool
		expectedLoss  int // expected number of entries that might be lost
	}{
		{
			name:          "clean_shutdown",
			numEntries:    1000,
			batchSize:     100,
			payloadSize:   1024,
			crashPosition: 1.0,
			corrupt:       false,
			expectedLoss:  0,
		},
		{
			name:          "crash_mid_batch",
			numEntries:    1000,
			batchSize:     100,
			payloadSize:   1024,
			crashPosition: 0.5,
			corrupt:       false,
			expectedLoss:  50, // might lose up to half a batch
		},
		{
			name:          "crash_with_corruption",
			numEntries:    1000,
			batchSize:     100,
			payloadSize:   1024,
			crashPosition: 0.75,
			corrupt:       true,
			expectedLoss:  100, // might lose up to a batch
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			walPath := filepath.Join(dir, "test.wal")

			// Write data and simulate crash
			writtenEntries := simulateCrash(t, walPath, tc.numEntries, tc.batchSize,
				tc.payloadSize, tc.crashPosition, tc.corrupt)

			// Recover and verify
			recoveredEntries := recoverAndRead(t, walPath)

			// Verify recovery
			verifyRecovery(t, writtenEntries, recoveredEntries, tc.expectedLoss)
		})
	}
}

func TestWALBasicRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	// Create and write to WAL
	wal, err := Open(path, options.DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}

	// Write a few entries
	entries := generateBatch(0, 10, 1024)
	if err := wal.WriteBatch(entries); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Force sync and close properly
	if err := wal.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	wal.Close()

	// Reopen and verify
	wal, err = Open(path, options.DefaultOptions())
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer wal.Close()

	recovered, err := wal.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if len(recovered) != len(entries) {
		t.Errorf("Expected %d entries, got %d", len(entries), len(recovered))
	}
}

func generateTestEntries(count, payloadSize int) []*entry.Entry {
	entries := make([]*entry.Entry, count)
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := make([]byte, payloadSize)
		// Fill with deterministic but unique data
		for j := range value {
			value[j] = byte((i * j) % 256)
		}
		entries[i] = entry.NewEntry(key, value)
	}
	return entries
}

func simulateCrash(t *testing.T, path string, numEntries, batchSize,
	payloadSize int, crashPoint float64, corrupt bool) []*entry.Entry {
	t.Logf("Using WAL file: %s", path)

	opts := options.DefaultOptions()
	opts.SyncStrategy = options.SyncBatch

	wal, err := Open(path, opts)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}

	var writtenEntries []*entry.Entry
	crashAfter := int(float64(numEntries) * crashPoint)

	t.Logf("Starting writes: entries=%d, batch=%d, size=%d",
		crashAfter, batchSize, payloadSize)

	// Write entries until crash point
	for i := 0; i < crashAfter; i += batchSize {
		batch := generateBatch(i, min(batchSize, crashAfter-i), payloadSize)
		if err := wal.WriteBatch(batch); err != nil {
			t.Fatalf("Write failed at batch %d: %v", i/batchSize, err)
		}
		writtenEntries = append(writtenEntries, batch...)

		fileSize := wal.fm.Size()
		walOffset := wal.offset.Load()
		t.Logf("Batch %d written: file=%d, offset=%d",
			i/batchSize, fileSize, walOffset)
	}

	if info, err := os.Stat(path); err != nil {
		t.Fatalf("Failed to stat WAL file: %v", err)
	} else {
		t.Logf("WAL file size before crash: %d", info.Size())
	}

	// Force sync
	if err := wal.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	finalSize := wal.fm.Size()
	finalOffset := wal.offset.Load()
	t.Logf("Before crash: entries=%d, size=%d, offset=%d",
		len(writtenEntries), finalSize, finalOffset)

	// Simulate crash by not closing
	if corrupt {
		corruptWALFile(t, path)
		t.Log("Corrupted WAL file")
	}

	// Clear any file handles
	wal = nil
	runtime.GC() // Force cleanup

	if info, err := os.Stat(path); err != nil {
		t.Fatalf("File missing after crash: %v", err)
	} else {
		t.Logf("WAL file exists after crash, size: %d", info.Size())
	}

	return writtenEntries
}

func generateBatch(start, count, payloadSize int) []*entry.Entry {
	batch := make([]*entry.Entry, count)
	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("key-%d", start+i))
		value := make([]byte, payloadSize)
		// Deterministic but unique data
		for j := range value {
			value[j] = byte((start + i + j) % 256)
		}
		batch[i] = entry.NewEntry(key, value)
	}
	return batch
}

func recoverAndRead(t *testing.T, path string) []*entry.Entry {
	// Verify file before recovery
	if info, err := os.Stat(path); err != nil {
		t.Fatalf("File missing before recovery: %v", err)
	} else {
		t.Logf("WAL file size before recovery: %d", info.Size())
	}

	opts := options.DefaultOptions()

	// Open WAL (triggers recovery)
	wal, err := Open(path, opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer wal.Close()

	// Verify file after recovery
	if info, err := os.Stat(path); err != nil {
		t.Fatalf("File missing after recovery: %v", err)
	} else {
		t.Logf("WAL file size after recovery: %d", info.Size())
	}

	// Read all entries
	entries, err := wal.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	return entries
}

func verifyRecovery(t *testing.T, written, recovered []*entry.Entry, expectedLoss int) {
	t.Logf("Written: %d, Recovered: %d, Expected loss: ≤%d",
		len(written), len(recovered), expectedLoss)

	actualLoss := len(written) - len(recovered)
	if actualLoss > expectedLoss {
		t.Errorf("Lost too many entries. Lost: %d, Expected maximum loss: %d",
			actualLoss, expectedLoss)
	}

	// Verify recovered entries match written ones
	for i, rec := range recovered {
		if !bytes.Equal(rec.Key, written[i].Key) {
			t.Errorf("Entry %d: key mismatch", i)
		}
		if !bytes.Equal(rec.Value, written[i].Value) {
			t.Errorf("Entry %d: value mismatch", i)
		}
	}
}

func corruptWALFile(t *testing.T, path string) {
	// Read file
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("Failed to read WAL file: %v", err)
	}

	// Corrupt last few bytes
	if len(data) > 100 {
		for i := len(data) - 100; i < len(data); i++ {
			data[i] = byte(i % 256)
		}
	}

	// Write back corrupted data
	if err := os.WriteFile(path, data, 0666); err != nil {
		t.Fatalf("Failed to write corrupted data: %v", err)
	}
}

func performRecovery(t *testing.T, path string) ([]*entry.Entry, error) {
	opts := options.DefaultOptions()

	// Open WAL (should trigger recovery)
	wal, err := Open(path, opts)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}
	defer wal.Close()

	// Verify recovery completed
	_, err = wal.Recover()
	if err != nil {
		t.Fatal(err)
	}

	// if state.EntriesFound != expectedEntries {
	// 	t.Errorf("Expected %d entries, got %d", expectedEntries, state.EntriesFound)
	// }
	// if state.Truncated {
	// 	t.Logf("WAL was truncated from %d to %d bytes", originalSize, state.ValidOffset)
	// }

	// Read all entries
	entries, err := wal.Read()
	if err != nil {
		return nil, fmt.Errorf("read entries: %w", err)
	}

	return entries, nil
}

func verifyRecoveredData(t *testing.T, written, recovered []*entry.Entry) {
	t.Logf("Written entries: %d, Recovered entries: %d", len(written), len(recovered))

	// First verify counts match
	if len(recovered) > len(written) {
		t.Errorf("Recovered more entries than written: %d > %d", len(recovered), len(written))
		return
	}

	// Verify each recovered entry matches written
	for i, rec := range recovered {
		if i >= len(written) {
			t.Errorf("Extra entry recovered at position %d", i)
			continue
		}

		wrt := written[i]

		// Compare keys
		if !bytes.Equal(rec.Key, wrt.Key) {
			t.Errorf("Entry %d: key mismatch\nwant: %x\ngot:  %x", i, wrt.Key, rec.Key)
		}

		// Compare values
		if !bytes.Equal(rec.Value, wrt.Value) {
			t.Errorf("Entry %d: value mismatch\nwant: %x\ngot:  %x", i, wrt.Value, rec.Value)
		}

		// Verify timestamp is reasonable
		if rec.Timestamp > time.Now().UnixNano() || rec.Timestamp < time.Now().Add(-1*time.Hour).UnixNano() {
			t.Errorf("Entry %d: suspicious timestamp: %v", i, rec.Timestamp)
		}
	}
}

// Additional helper test for WAL Manager recovery
func TestWALManagerRecovery(t *testing.T) {
	dir := t.TempDir()
	numWALs := 2
	entriesPerWAL := 500
	batchSize := 100
	payloadSize := 1024

	// Create and populate WAL Manager
	opts := options.DefaultOptions()
	manager, err := NewWALManager(dir, numWALs, opts)
	if err != nil {
		t.Fatalf("Failed to create WAL manager: %v", err)
	}

	// Generate and write test data
	allEntries := generateTestEntries(entriesPerWAL*numWALs, payloadSize)
	for i := 0; i < len(allEntries); i += batchSize {
		end := i + batchSize
		if end > len(allEntries) {
			end = len(allEntries)
		}
		if err := manager.WriteBatch(allEntries[i:end]); err != nil {
			t.Fatalf("Write failed at batch %d: %v", i/batchSize, err)
		}
	}

	// Simulate crash by not calling Close()
	manager = nil

	// Attempt recovery
	newManager, err := NewWALManager(dir, numWALs, opts)
	if err != nil {
		t.Fatalf("Failed to recover WAL manager: %v", err)
	}
	defer newManager.Close()

	// Verify each WAL recovered correctly
	for i := 0; i < numWALs; i++ {
		walPath := filepath.Join(dir, fmt.Sprintf("wal-%d", i))
		entries, err := performRecovery(t, walPath)
		if err != nil {
			t.Errorf("WAL %d recovery failed: %v", i, err)
			continue
		}
		t.Logf("WAL %d recovered %d entries", i, len(entries))
	}
}

// Add this test first
func TestWALBasicReadWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	// Open WAL
	wal, err := Open(path, options.DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}

	// Write some entries
	entries := generateTestEntries(10, 1024)
	if err := wal.WriteBatch(entries); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Sync and close properly
	if err := wal.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	wal.Close()

	// Reopen and read
	wal, err = Open(path, options.DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	defer wal.Close()

	recovered, err := wal.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	t.Logf("Written: %d, Read: %d", len(entries), len(recovered))
}
