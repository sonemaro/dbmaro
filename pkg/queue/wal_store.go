// wal_store.go
package queue

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/sonemaro/dbmaro/pkg/wal"
	"github.com/sonemaro/dbmaro/pkg/wal/entry"
	"github.com/sonemaro/dbmaro/pkg/wal/logger"
	"github.com/sonemaro/dbmaro/pkg/wal/options"
)

const (
	// Key prefixes for different types of data
	prefixLog    = byte(1)
	prefixStable = byte(2)

	// Default batch size for WAL operations
	defaultBatchSize = 100

	// Stable store keys
	keyCurrentTerm  = "CurrentTerm"
	keyLastVoteTerm = "LastVoteTerm"
	keyLastVoteCand = "LastVoteCandidate"
)

type WALStore struct {
	wal    *wal.WAL
	logger *logger.Manager
	mu     sync.RWMutex

	// Cache for stable store values
	cache map[string][]byte
}

func NewWALStore(dir string) (*WALStore, error) {
	logger := logger.NewManager(false).SetLevel(logger.INFO)

	opts := options.Options{
		InitialMapSize: 32 * 1024 * 1024,       // 32MB
		MaxMapSize:     2 * 1024 * 1024 * 1024, // 2GB
		BufferSize:     256 * 1024,             // 256KB
		PoolConfig:     options.DefaultPoolConfig(),
	}

	w, err := wal.Open(dir, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}

	store := &WALStore{
		wal:    w,
		logger: logger,
		cache:  make(map[string][]byte),
	}

	// Initialize with default values if empty
	if err := store.initializeIfEmpty(); err != nil {
		return nil, fmt.Errorf("failed to initialize store: %w", err)
	}

	// Recover existing data
	if _, err := store.recover(); err != nil {
		return nil, fmt.Errorf("failed to recover WAL: %w", err)
	}

	return store, nil
}

func (w *WALStore) initializeIfEmpty() error {
	// Check if store is empty
	first, err := w.FirstIndex()
	if err != nil {
		return err
	}

	if first == 0 {
		// Initialize with default values
		if err := w.SetUint64([]byte(keyCurrentTerm), 0); err != nil {
			return err
		}
		if err := w.SetUint64([]byte(keyLastVoteTerm), 0); err != nil {
			return err
		}
		if err := w.Set([]byte(keyLastVoteCand), []byte("")); err != nil {
			return err
		}
	}

	return nil
}

// recover rebuilds the cache from WAL
func (w *WALStore) recover() (*wal.RecoveryState, error) {
	w.logger.Info("starting WAL recovery")

	state, err := w.wal.Recover()
	if err != nil {
		return nil, err
	}

	// Rebuild cache from recovered entries
	iter := w.wal.Iterator()
	for {
		e, err := iter.Next()
		if err != nil {
			return nil, fmt.Errorf("iterator error: %w", err)
		}
		if e == nil {
			break
		}

		// Only process stable store entries
		if len(e.Key) > 0 && e.Key[0] == prefixStable {
			w.cache[string(e.Key[1:])] = e.Value
		}
	}

	return state, nil
}

// encodeLogKey creates a key for log entries
func encodeLogKey(index uint64) []byte {
	key := make([]byte, 9)
	key[0] = prefixLog
	binary.BigEndian.PutUint64(key[1:], index)
	return key
}

// encodeStableKey creates a key for stable store entries
func encodeStableKey(key []byte) []byte {
	newKey := make([]byte, len(key)+1)
	newKey[0] = prefixStable
	copy(newKey[1:], key)
	return newKey
}

// Implement raft.LogStore interface
func (w *WALStore) FirstIndex() (uint64, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	iter := w.wal.Iterator()
	e, err := iter.Next()
	if err != nil {
		return 0, err
	}
	if e == nil || len(e.Key) == 0 || e.Key[0] != prefixLog {
		return 0, nil
	}

	return binary.BigEndian.Uint64(e.Key[1:]), nil
}

func (w *WALStore) LastIndex() (uint64, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	var lastIndex uint64
	iter := w.wal.Iterator()
	for {
		e, err := iter.Next()
		if err != nil {
			return 0, err
		}
		if e == nil {
			break
		}
		if len(e.Key) > 0 && e.Key[0] == prefixLog {
			lastIndex = binary.BigEndian.Uint64(e.Key[1:])
		}
	}

	return lastIndex, nil
}

func (w *WALStore) GetLog(index uint64, log *raft.Log) error {
	w.mu.RLock()
	defer w.mu.RUnlock()

	key := encodeLogKey(index)

	iter := w.wal.Iterator()
	for {
		e, err := iter.Next()
		if err != nil {
			return err
		}
		if e == nil {
			return raft.ErrLogNotFound
		}
		if string(e.Key) == string(key) {
			return decodeLog(e.Value, log)
		}
	}
}

func (w *WALStore) StoreLogs(logs []*raft.Log) error {
	if len(logs) == 0 {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// Create batch of entries
	entries := make([]*entry.Entry, len(logs))
	for i, log := range logs {
		key := encodeLogKey(log.Index)
		data := encodeLog(log)
		entries[i] = entry.NewEntry(key, data)
	}

	// Write batch to WAL
	return w.wal.WriteBatch(entries)
}

func (w *WALStore) StoreLog(log *raft.Log) error {
	return w.StoreLogs([]*raft.Log{log})
}

func (w *WALStore) DeleteRange(min, max uint64) error {
	// Your WAL doesn't support direct deletion
	// This is a limitation we have to live with
	// Raft will handle this by compacting the log
	return nil
}

// Implement raft.StableStore interface
func (w *WALStore) Set(key []byte, val []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	stableKey := encodeStableKey(key)
	e := entry.NewEntry(stableKey, val)

	if err := w.wal.Write(e); err != nil {
		return err
	}

	// Update cache
	w.cache[string(key)] = val
	return nil
}

func (w *WALStore) Get(key []byte) ([]byte, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	// Check cache first
	if val, ok := w.cache[string(key)]; ok {
		return val, nil
	}

	return nil, fmt.Errorf("key not found")
}

func (w *WALStore) SetUint64(key []byte, val uint64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, val)
	return w.Set(key, buf)
}

func (w *WALStore) GetUint64(key []byte) (uint64, error) {
	buf, err := w.Get(key)
	if err != nil {
		return 0, err
	}
	if len(buf) != 8 {
		return 0, fmt.Errorf("invalid uint64 value")
	}
	return binary.BigEndian.Uint64(buf), nil
}

func (w *WALStore) HasExistingState() (bool, error) {
	// Check if we have any existing state
	firstIndex, err := w.FirstIndex()
	if err != nil {
		return false, err
	}
	lastIndex, err := w.LastIndex()
	if err != nil {
		return false, err
	}

	return firstIndex > 0 || lastIndex > 0, nil
}

// Helper functions for log encoding/decoding
func encodeLog(log *raft.Log) []byte {
	size := 8 + 8 + 4 + 1 + len(log.Data)
	buf := make([]byte, size)
	offset := 0

	binary.BigEndian.PutUint64(buf[offset:], log.Index)
	offset += 8

	binary.BigEndian.PutUint64(buf[offset:], log.Term)
	offset += 8

	binary.BigEndian.PutUint32(buf[offset:], uint32(log.Type))
	offset += 4

	buf[offset] = byte(len(log.Data))
	offset++

	copy(buf[offset:], log.Data)

	return buf
}

func decodeLog(buf []byte, log *raft.Log) error {
	if len(buf) < 21 { // 8 + 8 + 4 + 1
		return fmt.Errorf("invalid log data size")
	}

	offset := 0

	log.Index = binary.BigEndian.Uint64(buf[offset:])
	offset += 8

	log.Term = binary.BigEndian.Uint64(buf[offset:])
	offset += 8

	log.Type = raft.LogType(binary.BigEndian.Uint32(buf[offset:]))
	offset += 4

	dataLen := int(buf[offset])
	offset++

	if offset+dataLen > len(buf) {
		return fmt.Errorf("invalid log data length")
	}

	log.Data = make([]byte, dataLen)
	copy(log.Data, buf[offset:offset+dataLen])

	return nil
}
