package wal

/*
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║                       High-Performance WAL Design                            ║
║                       ======================                                 ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝

Overview:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
This Write-Ahead Log (WAL) implementation is designed for high-throughput
sequential writes with focus on large batch operations. It's optimized for
modern hardware (NVMe, high RAM) and provides strong durability guarantees.

Key Features:
• High-performance batch writes (~117 MB/s for 100x10KB batches)
• Memory-mapped I/O with smart buffering
• Optimized for large sequential writes
• Checksum verification for data integrity
• Crash recovery support

File Format:
┌──────────────┬──────────────┬──────────────┬──────────────┬──────────────┐
│    Header    │    Entry     │   Payload    │   Checksum   │    Entry     │
│    (8B)      │    (8B)      │    (var)     │    (4B)      │    ...      │
└──────────────┴──────────────┴──────────────┴──────────────┴──────────────┘

Header Format (8 bytes):
┌──────────────┬──────────────┬──────────────┬──────────────┐
│    Magic     │   Version    │    Flags     │  Reserved    │
│    (2B)      │    (1B)      │    (1B)      │    (4B)      │
└──────────────┴──────────────┴──────────────┴──────────────┘

Entry Format:
┌──────────────┬──────────────┬──────────────┬──────────────┐
│  Timestamp   │   DataSize   │    Data      │   Checksum   │
│    (8B)      │    (4B)      │    (var)     │    (4B)      │
└──────────────┴──────────────┴──────────────┴──────────────┘

Performance Characteristics:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Write Performance:
• Single small writes (<4KB):    ~0.08 MB/s
• Batch writes (100x128B):       ~2.75 MB/s
• Large batch writes (100x10KB): ~117 MB/s

Memory Usage:
• Small writes:  2-12 allocs/op
• Large batches: ~104 allocs/op
• Buffer pools:  Configurable sizes

Usage Examples:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Basic Usage:
    wal, err := Open("data.wal", DefaultOptions())
    if err != nil {
        return err
    }
    defer wal.Close()

    // Single write
    entry := NewEntry(key, value)
    if err := wal.Write(entry); err != nil {
        return err
    }

    // Batch write
    entries := make([]*Entry, batchSize)
    // ... fill entries
    if err := wal.WriteBatch(entries); err != nil {
        return err
    }

Configuration:
    opts := Options{
        SyncStrategy: SyncBatch,
        InitialMapSize: 32 * 1024 * 1024,  // 32MB
        MaxMapSize: 2 * 1024 * 1024 * 1024, // 2GB
        BufferSize: 256 * 1024,            // 256KB
    }

Recovery:
    wal, err := Open("data.wal", opts)
    if err != nil {
        return err
    }
    if err := wal.Recover(); err != nil {
        return err
    }

Best Practices:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1. Write Optimization:
   • Batch similar-sized entries together
   • Use appropriate buffer sizes for your data
   • Consider sync strategy based on durability needs

2. Memory Management:
   • Configure initial size based on expected load
   • Monitor growth patterns
   • Use appropriate buffer pools

3. Durability:
   • Choose sync strategy carefully
   • Implement proper recovery procedures
   • Regular checksum verification

Limitations:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
• Maximum entry size: 4GB (uint32 size field)
• Maximum file size: System dependent
• Single writer, multiple readers

Error Handling:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Common errors:
• ErrInvalidData: Data corruption detected
• ErrChecksumMismatch: Checksum verification failed
• ErrWALClosed: Operations on closed WAL
• ErrInvalidSize: Entry size validation failed

╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║  "Performance comes from simplicity; reliability comes from careful design"  ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
*/

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sonemaro/dbmaro/pkg/wal/buffer"
	"github.com/sonemaro/dbmaro/pkg/wal/checksum"
	"github.com/sonemaro/dbmaro/pkg/wal/entry"
	"github.com/sonemaro/dbmaro/pkg/wal/file"
	"github.com/sonemaro/dbmaro/pkg/wal/logger"
	"github.com/sonemaro/dbmaro/pkg/wal/options"
)

const (
	walMagicNumber  = 0x57414C2D76310001 // "WAL-v1\x00\x01"
	headerSize      = 8
	entryHeaderSize = 8 // Magic(2) + Version(1) + Flags(1) + Size(4)
	minEntrySize    = entryHeaderSize + checksum.Size

	// Recovery Result
	RecoveryComplete  RecoveryResult = iota // Normal completion, all data valid
	RecoveryTruncated                       // Completed with truncation (normal)
	RecoveryError                           // Actual error conditions
)

var (
	// WAL Errors
	ErrInvalidMagic     = errors.New("invalid magic number")
	ErrInvalidHeader    = errors.New("invalid header")
	ErrChecksumMismatch = errors.New("checksum mismatch")
	ErrWALClosed        = errors.New("wal is closed")
	ErrCorruptedData    = errors.New("corrupted data")

	// For small batches/entries
	smallBufPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 0, 32*1024) // 32KB initial
		},
	}

	// For larger batches
	largeBufPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 0, 1024*1024) // 1MB initial
		},
	}

	// For entry metadata
	entryDataPool = sync.Pool{
		New: func() interface{} {
			return make([]entryData, 0, 128) // Reasonable default
		},
	}
)

type RecoveryResult int

type entryData struct {
	data     []byte
	checksum uint32
	size     int
}

// Metrics for monitoring WAL operations
type Metrics struct {
	ReadOps      uint64
	WriteOps     uint64
	BytesRead    uint64
	BytesWritten uint64
	Errors       uint64
	Corruptions  uint64
}

func NewMetrics() *Metrics {
	return &Metrics{}
}

// WalBoundary tracks valid data boundaries
type WalBoundary struct {
	ValidDataOffset int64
	LastWriteTime   int64
	LastValidMagic  uint16
	Checksum        uint32
}

type Iterator struct {
	wal       *WAL
	offset    int64
	endOffset int64
}

type WAL struct {
	mu         sync.RWMutex
	bufferPool *buffer.BufferPool
	fm         *file.FileManager
	reader     *file.ChunkReader
	parser     *entry.Parser
	checksum   *checksum.Manager
	logger     *logger.Manager
	offset     atomic.Int64
	closed     atomic.Bool
	metrics    *Metrics
	boundary   atomic.Value // stores *WalBoundary
}

type RecoveryState struct {
	ValidOffset    int64
	EntriesFound   int
	LastValidEntry *entry.Entry
	Truncated      bool
	ChunksRead     int
	TotalBytesRead int64
}

func Open(path string, opts options.Options) (*WAL, error) {
	// Initialize file manager with explicit sync
	pageSize := int64(4096)
	fm, err := file.NewFileManager(path, &file.Options{
		ReadOnly: false,
		InitSize: pageSize,
		MaxSize:  opts.MaxMapSize,
		Sync:     true, // Force sync for header operations
	})
	if err != nil {
		return nil, fmt.Errorf("file manager init: %w", err)
	}

	w := &WAL{
		bufferPool: buffer.NewBufferPool(opts.PoolConfig),
		fm:         fm,
		reader:     file.NewChunkReader(64 * 1024), // 64KB chunks
		parser:     entry.NewParser(),
		checksum:   checksum.NewManager(options.DefaultChecksumConfig()),
		logger:     logger.NewManager(false).SetLevel(logger.ERROR), // TODO read debug mode form config
		metrics:    NewMetrics(),
	}

	// initialize/verify WAL
	if err := w.init(); err != nil {
		fm.Close()

		return nil, err
	}

	if opts.InitialMapSize > pageSize {
		newSize := (opts.InitialMapSize + pageSize - 1) & ^(pageSize - 1)
		if err := fm.Truncate(newSize); err != nil {
			fm.Close()

			return nil, fmt.Errorf("resize: %w", err)
		}
	}

	state, err := w.Recover()
	if err != nil {
		fm.Close()

		return nil, fmt.Errorf("recovery failed: %w", err)
	}

	if state.Truncated {
		w.logger.Info("WAL recovered with truncation: entries=%d, valid_bytes=%d",
			state.EntriesFound, state.ValidOffset)
	} else {
		w.logger.Info("WAL recovered cleanly: entries=%d, bytes=%d",
			state.EntriesFound, state.ValidOffset)
	}

	return w, nil
}

func (w *WAL) init() error {
	stat, err := w.fm.Stat()
	if err != nil {
		return fmt.Errorf("stat file: %w", err)
	}

	size := stat.Size()
	w.logger.Debug("Initializing WAL, file size: %d", size)

	// Initialize boundary
	boundary := &WalBoundary{
		ValidDataOffset: headerSize,
		LastWriteTime:   time.Now().UnixNano(),
		LastValidMagic:  entry.MagicMarker,
		Checksum:        0,
	}
	w.boundary.Store(boundary)

	// For new or empty files
	if size == 0 || size < headerSize {
		// Write header
		header := make([]byte, headerSize)
		binary.BigEndian.PutUint64(header, walMagicNumber)

		if err := w.fm.Write(header, 0); err != nil {
			return fmt.Errorf("write header: %w", err)
		}

		if err := w.fm.Sync(); err != nil {
			return fmt.Errorf("sync header: %w", err)
		}

		w.offset.Store(headerSize)
		w.logger.Debug("Initialized new WAL with header")
		return nil
	}

	// Verify header for existing files
	header, err := w.fm.Read(0, headerSize)
	if err != nil {
		return fmt.Errorf("read header: %w", err)
	}

	if len(header) < headerSize {
		return fmt.Errorf("incomplete header: got %d bytes", len(header))
	}

	magic := binary.BigEndian.Uint64(header)
	if magic != walMagicNumber {
		// If magic number is zero, initialize as new file
		if magic == 0 {
			w.logger.Debug("Found zeroed header, initializing as new file")
			// Write header
			header := make([]byte, headerSize)
			binary.BigEndian.PutUint64(header, walMagicNumber)

			if err := w.fm.Write(header, 0); err != nil {
				return fmt.Errorf("write header: %w", err)
			}

			if err := w.fm.Sync(); err != nil {
				return fmt.Errorf("sync header: %w", err)
			}

			w.offset.Store(headerSize)
			return nil
		}
		return fmt.Errorf("%w: got %x, want %x", ErrInvalidMagic, magic, walMagicNumber)
	}

	w.offset.Store(size)
	w.logger.Debug("Verified existing WAL, size=%d", size)
	return nil
}

// func (w *WAL) init() error {
// 	stat, err := w.fm.Stat()
// 	if err != nil {
// 		return fmt.Errorf("stat file: %w", err)
// 	}

// 	size := stat.Size()
// 	if size == 0 {
// 		// New file, write header
// 		header := make([]byte, headerSize)
// 		binary.BigEndian.PutUint64(header, walMagicNumber)

// 		if err := w.fm.Write(header, 0); err != nil {
// 			return fmt.Errorf("write header: %w", err)
// 		}

// 		// Force sync after writing header
// 		if err := w.fm.Sync(); err != nil {
// 			return fmt.Errorf("sync header: %w", err)
// 		}

// 		w.offset.Store(headerSize)
// 		w.logger.Debug("Initialized new WAL with header")

// 		// Initialize boundary for new file
// 		w.boundary.Store(&WalBoundary{
// 			ValidDataOffset: headerSize,
// 			LastWriteTime:   time.Now().UnixNano(),
// 			LastValidMagic:  entry.MagicMarker,
// 			Checksum:        0,
// 		})

// 		return nil
// 	}

// 	// Existing file, verify header
// 	if size < headerSize {
// 		return fmt.Errorf("invalid file size: %d", size)
// 	}

// 	header, err := w.fm.Read(0, headerSize)
// 	if err != nil {
// 		return fmt.Errorf("read header: %w", err)
// 	}

// 	if len(header) < headerSize {
// 		return fmt.Errorf("incomplete header: got %d bytes", len(header))
// 	}

// 	magic := binary.BigEndian.Uint64(header)
// 	if magic != walMagicNumber {
// 		return fmt.Errorf("%w: got %x, want %x", ErrInvalidMagic, magic, walMagicNumber)
// 	}

// 	w.offset.Store(size)
// 	w.logger.Debug("Verified existing WAL, size=%d", size)

// 	// Initialize boundary for existing file
// 	w.boundary.Store(&WalBoundary{
// 		ValidDataOffset: headerSize,
// 		LastWriteTime:   time.Now().UnixNano(),
// 		LastValidMagic:  entry.MagicMarker,
// 		Checksum:        0,
// 	})

// 	return nil
// }

func (w *WAL) Write(e *entry.Entry) error {
	if w.closed.Load() {
		return ErrWALClosed
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// Pre-calculate sizes to avoid multiple allocations
	data, err := e.Marshal()
	if err != nil {
		return fmt.Errorf("marshal entry: %w", err)
	}

	totalSize := entry.HeaderSize + len(data) + checksum.Size
	writeOffset := w.offset.Load()

	// Single allocation for the entire entry
	buf := make([]byte, totalSize)

	// Write header directly into buffer
	header := entry.NewHeader(uint32(len(data)), 0)
	headerBytes := header.Marshal()
	copy(buf, headerBytes)

	// Write data
	copy(buf[entry.HeaderSize:], data)

	// Calculate and write checksum
	chk := w.checksum.Calculate(data)
	binary.BigEndian.PutUint32(buf[entry.HeaderSize+len(data):], chk)

	// Single write operation
	if err := w.fm.Write(buf, writeOffset); err != nil {
		return fmt.Errorf("write: %w", err)
	}

	w.offset.Add(int64(totalSize))
	return w.fm.Sync()
}

func (w *WAL) WriteBatch(entries []*entry.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// Get entryData slice from pool
	entryDataList := entryDataPool.Get().([]entryData)
	if cap(entryDataList) < len(entries) {
		entryDataList = make([]entryData, 0, len(entries))
	}
	entryDataList = entryDataList[:0]
	defer entryDataPool.Put(entryDataList)

	// Pre-calculate total size
	var totalSize int64 = 0
	for _, e := range entries {
		size := entry.HeaderSize + e.Size() + checksum.Size
		totalSize += size
	}

	// Get appropriate buffer from pool
	var buf []byte
	if totalSize <= 32*1024 {
		buf = smallBufPool.Get().([]byte)
		if int64(cap(buf)) < totalSize {
			buf = make([]byte, 0, totalSize)
		}
		buf = buf[:0]
		defer smallBufPool.Put(buf)
	} else {
		buf = largeBufPool.Get().([]byte)
		if int64(cap(buf)) < totalSize {
			buf = make([]byte, 0, totalSize)
		}
		buf = buf[:0]
		defer largeBufPool.Put(buf)
	}

	// Process entries
	for i, e := range entries {
		data, err := e.Marshal()
		if err != nil {
			return fmt.Errorf("marshal entry %d: %w", i, err)
		}

		chk := w.checksum.Calculate(data)
		size := entry.HeaderSize + len(data) + checksum.Size

		entryDataList = append(entryDataList, entryData{
			data:     data,
			checksum: chk,
			size:     size,
		})
	}

	// Assemble entries into buffer
	for _, ed := range entryDataList {
		// Write header
		header := entry.NewHeader(uint32(len(ed.data)), 0)
		headerBytes := header.Marshal()
		buf = append(buf, headerBytes...)

		// Write data
		buf = append(buf, ed.data...)

		// Write checksum
		checksumBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(checksumBytes, ed.checksum)
		buf = append(buf, checksumBytes...)
	}

	// Single write
	writeOffset := w.offset.Load()
	if err := w.fm.Write(buf, writeOffset); err != nil {
		return fmt.Errorf("write batch: %w", err)
	}

	w.offset.Add(int64(len(buf)))
	return w.fm.Sync()
}

func (w *WAL) Read() ([]*entry.Entry, error) {
	if w.closed.Load() {
		return nil, ErrWALClosed
	}

	w.mu.RLock()
	defer w.mu.RUnlock()

	entries := make([]*entry.Entry, 0, 1000)
	offset := int64(headerSize)
	endOffset := w.offset.Load()

	// Read in larger chunks for better performance
	const readChunkSize = 64 * 1024 // 64KB chunks
	var chunk []byte
	var chunkOffset int64

	for offset < endOffset {
		// Read new chunk if needed
		if chunk == nil || offset >= chunkOffset+int64(len(chunk)) {
			size := min(readChunkSize, int(endOffset-offset))
			var err error
			chunk, err = w.fm.Read(offset, int64(size))
			if err != nil {
				return entries, fmt.Errorf("read chunk: %w", err)
			}
			chunkOffset = offset
		}

		// Process entry from chunk
		localOffset := int(offset - chunkOffset)

		// Parse header
		header := &entry.Header{}
		if err := header.Unmarshal(chunk[localOffset : localOffset+entry.HeaderSize]); err != nil {
			return entries, fmt.Errorf("unmarshal header: %w", err)
		}

		dataSize := int(header.DataSize)
		totalSize := entry.HeaderSize + dataSize + checksum.Size

		// Verify we have enough data in chunk
		if localOffset+totalSize > len(chunk) {
			chunk = nil
			continue
		}

		// Extract and verify data
		data := chunk[localOffset+entry.HeaderSize : localOffset+entry.HeaderSize+dataSize]
		storedChecksum := binary.BigEndian.Uint32(chunk[localOffset+entry.HeaderSize+dataSize:])

		if w.checksum.Calculate(data) != storedChecksum {
			return entries, ErrChecksumMismatch
		}

		// Unmarshal entry
		e := &entry.Entry{}
		if err := e.Unmarshal(data); err != nil {
			return entries, fmt.Errorf("unmarshal entry: %w", err)
		}

		entries = append(entries, e)
		offset += int64(totalSize)
	}

	return entries, nil
}

func (w *WAL) getValidEndOffset() int64 {
	if boundary := w.boundary.Load(); boundary != nil {
		return boundary.(*WalBoundary).ValidDataOffset
	}
	return w.offset.Load()
}

func (w *WAL) Close() error {
	if !w.closed.CompareAndSwap(false, true) {
		return ErrWALClosed
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	return w.fm.Close()
}

func (w *WAL) Sync() error {
	if w.closed.Load() {
		return ErrWALClosed
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	return w.fm.Sync()
}

func (w *WAL) Iterator() *Iterator {
	return &Iterator{
		wal:       w,
		offset:    headerSize,
		endOffset: w.offset.Load(),
	}
}

func (it *Iterator) Next() (*entry.Entry, error) {
	if it.offset >= it.endOffset {
		return nil, nil
	}

	// Read header
	headerData, err := it.wal.fm.Read(it.offset, entry.HeaderSize)
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}

	header := &entry.Header{}
	if err := header.Unmarshal(headerData); err != nil {
		return nil, fmt.Errorf("unmarshal header: %w", err)
	}

	// Read data and checksum
	dataSize := int64(header.DataSize)
	entryData, err := it.wal.fm.Read(it.offset+entry.HeaderSize, dataSize+checksum.Size)
	if err != nil {
		return nil, fmt.Errorf("read data: %w", err)
	}

	// Verify checksum
	data, valid := it.wal.checksum.ExtractAndVerify(entryData)
	if !valid {
		return nil, ErrChecksumMismatch
	}

	// Unmarshal entry
	e := &entry.Entry{}
	if err := e.Unmarshal(data); err != nil {
		return nil, fmt.Errorf("unmarshal entry: %w", err)
	}

	it.offset += entry.HeaderSize + dataSize + checksum.Size
	return e, nil
}

func (w *WAL) Recover() (*RecoveryState, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	state := &RecoveryState{}

	fileSize := w.fm.Size()
	w.logger.Debug("starting WAL recovery: header=%d, fileSize=%d, currentOffset=%d",
		headerSize, fileSize, w.offset.Load())

	offset := int64(headerSize)
	state.ValidOffset = offset

	endOffset := fileSize

	w.logger.Debug("starting recovery: header=%d, end=%d", offset, endOffset)

	// Read in chunks like Read() method
	const readChunkSize = 64 * 1024 // 64KB chunks
	var chunk []byte
	var chunkOffset int64

	for offset < endOffset {
		// Read new chunk if needed
		if chunk == nil || offset >= chunkOffset+int64(len(chunk)) {
			size := min(readChunkSize, int(endOffset-offset))
			var err error
			chunk, err = w.fm.Read(offset, int64(size))
			if err != nil {
				w.logger.Error("failed to read chunk at offset %d: %v", offset, err)

				break // stop at read error
			}

			chunkOffset = offset

			state.ChunksRead++
			state.TotalBytesRead += int64(len(chunk))

			w.logger.Debug("read chunk: offset=%d, size=%d", offset, size)
		}

		localOffset := int(offset - chunkOffset)

		// try to parse header
		header := &entry.Header{}
		if localOffset+entry.HeaderSize > len(chunk) {
			w.logger.Debug("chunk too small for header, reading next chunk")
			chunk = nil

			continue
		}

		if err := header.Unmarshal(chunk[localOffset : localOffset+entry.HeaderSize]); err != nil {
			w.logger.Debug("found end of valid data at offset %d", state.ValidOffset)

			break // invalid header means corruption
		}

		w.logger.Debug("found header: magic=%x, size=%d", header.Magic, header.DataSize)

		// verify magic number
		if header.Magic != entry.MagicMarker {
			w.logger.Debug("found end of valid data at offset %d", state.ValidOffset)

			break // invalid magic number means corruption
		}

		dataSize := int(header.DataSize)
		totalSize := entry.HeaderSize + dataSize + checksum.Size

		// ensure we have the full entry in the chunk
		if localOffset+totalSize > len(chunk) {
			w.logger.Debug("entry spans chunks, reading next chunk")

			chunk = nil
			continue
		}

		// verify checksum
		data := chunk[localOffset+entry.HeaderSize : localOffset+entry.HeaderSize+dataSize]
		storedChecksum := binary.BigEndian.Uint32(chunk[localOffset+entry.HeaderSize+dataSize:])
		calculatedChecksum := w.checksum.Calculate(data)

		if calculatedChecksum != storedChecksum {
			w.logger.Debug("found end of valid data, checksum mismatch at offset %d", state.ValidOffset)

			break
		}

		// try to unmarshal entry to verify it's valid
		e := &entry.Entry{}
		if err := e.Unmarshal(data); err != nil {
			w.logger.Debug("found end of valid data, invalid entry at offset %d", state.ValidOffset)

			break // invalid entry data means corruption
		}

		state.ValidOffset = offset + int64(totalSize)
		state.EntriesFound++
		state.LastValidEntry = e

		// entry is valid, update validOffset
		offset += int64(totalSize)

		w.logger.Debug("valid entry found: offset=%d, size=%d", offset, totalSize)
		w.logger.Debug("processing entry at offset %d, chunk offset %d, local offset %d",
			offset, chunkOffset, localOffset)
	}

	// truncate to last valid entry if needed
	if state.ValidOffset < w.offset.Load() {
		w.logger.Info("truncating WAL from %d to %d bytes", w.offset.Load(), state.ValidOffset)
		state.Truncated = true

		if err := w.fm.Truncate(state.ValidOffset); err != nil {
			return state, fmt.Errorf("truncate during recovery: %w", err)
		}

		w.logger.Info("WAL truncated to valid offset: %d", state.ValidOffset)
	}

	w.offset.Store(state.ValidOffset)

	w.logger.Info("recovery completed, valid entries up to offset %d of %d",
		state.ValidOffset, fileSize)

	return state, nil
}
