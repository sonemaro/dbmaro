// file/manager.go
package file

import (
	"fmt"
	"io/fs"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sonemaro/dbmaro/pkg/wal/options"
	"golang.org/x/sys/unix"
)

const (
	defaultMapSize = 32 * 1024 * 1024 // 32MB
	maxMapSize     = 1 << 30          // 1GB
	pageSize       = 4096

	preAllocSize = 16 * 1024 * 1024 // 16MB pre-allocation chunks
	pageMask     = ^(pageSize - 1)  // For fast alignment
)

type FileManager struct {
	mu         sync.RWMutex
	file       *os.File
	mmap       []byte
	syncBuffer *syncBuffer
	path       string
	offset     atomic.Int64
	mapSize    int64
	readonly   bool

	writePos  int64 // Current write position without atomic
	lastSync  int64 // Last synced position
	growCount int32 // Track number of grows for adaptive sizing
}

type syncBuffer struct {
	mu        sync.Mutex
	pending   int64
	lastSync  time.Time
	threshold int64
	interval  time.Duration
}

type Options struct {
	ReadOnly     bool
	InitSize     int64
	MaxSize      int64
	SyncStrategy options.SyncStrategy
	Sync         bool
}

// For debugging/monitoring
type Stats struct {
	Path     string
	Size     int64
	Offset   int64
	ReadOnly bool
}

func NewFileManager(path string, opts *Options) (*FileManager, error) {
	if opts == nil {
		opts = &Options{
			ReadOnly: false,
			InitSize: 32 * 1024 * 1024,
			MaxSize:  1 << 30,
		}
	}

	flags := os.O_RDWR | os.O_CREATE
	if opts.ReadOnly {
		flags = os.O_RDONLY
	}

	file, err := os.OpenFile(path, flags, 0666)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}

	fm := &FileManager{
		file:     file,
		path:     path,
		readonly: opts.ReadOnly,
		mapSize:  opts.InitSize,
		syncBuffer: &syncBuffer{
			threshold: 1 << 20, // 1MB
			interval:  50 * time.Millisecond,
		},
	}

	// Initialize file size and mmap
	if err := fm.initializeFile(opts.InitSize); err != nil {
		file.Close()
		return nil, err
	}

	return fm, nil
}

// Optimize sync operations
func (fm *FileManager) syncOptimized() error {
	// Only sync changed pages
	dirtyStart := (fm.lastSync + pageSize - 1) & ^(pageSize - 1)
	dirtyEnd := (fm.writePos + pageSize - 1) & ^(pageSize - 1)

	if dirtyEnd > dirtyStart {
		err := unix.Msync(fm.mmap[dirtyStart:dirtyEnd], unix.MS_SYNC)
		if err == nil {
			fm.lastSync = fm.writePos
		}
		return err
	}
	return nil
}

func (fm *FileManager) Sync() error {
	currentPos := fm.writePos
	if currentPos == fm.lastSync {
		return nil // Nothing to sync
	}

	// Round up to page size
	syncSize := (currentPos + pageSize - 1) & pageMask

	err := unix.Msync(fm.mmap[:syncSize], unix.MS_SYNC)
	if err == nil {
		fm.lastSync = currentPos
	}

	return err
}

func (fm *FileManager) initializeFile(initSize int64) error {
	stat, err := fm.file.Stat()
	if err != nil {
		return fmt.Errorf("stat file: %w", err)
	}

	size := stat.Size()
	if size == 0 {
		// New file, set initial size
		if err := fm.file.Truncate(initSize); err != nil {
			return fmt.Errorf("truncate: %w", err)
		}
		size = initSize
	}

	// Initialize mmap
	if err := fm.mmapInit(size); err != nil {
		return fmt.Errorf("mmap init: %w", err)
	}

	// Set initial offset
	fm.offset.Store(0)
	fm.writePos = 0
	fm.lastSync = 0

	return nil
}

func (fm *FileManager) init(opts *Options) error {
	stat, err := fm.file.Stat()
	if err != nil {
		return fmt.Errorf("stat file: %w", err)
	}

	size := stat.Size()
	if size == 0 {
		// New file, set initial size
		if err := fm.file.Truncate(opts.InitSize); err != nil {
			return fmt.Errorf("truncate: %w", err)
		}
		size = opts.InitSize
	}

	// Initialize mmap
	if err := fm.mmapInit(size); err != nil {
		return fmt.Errorf("mmap init: %w", err)
	}

	return nil
}

func (fm *FileManager) mmapInit(size int64) error {
	// Round up to page size
	size = ((size + pageSize - 1) / pageSize) * pageSize

	prot := unix.PROT_READ
	if !fm.readonly {
		prot |= unix.PROT_WRITE
	}

	data, err := unix.Mmap(
		int(fm.file.Fd()),
		0,
		int(size),
		prot,
		unix.MAP_SHARED,
	)
	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}

	fm.mmap = data
	fm.mapSize = size

	return nil
}

func (fm *FileManager) Write(data []byte, offset int64) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	needed := offset + int64(len(data))

	// Fast path: check if we need to grow
	if needed > fm.mapSize {
		// Grow with pre-allocation
		newSize := ((needed + preAllocSize - 1) & ^(preAllocSize - 1))
		if err := fm.grow(newSize); err != nil {
			return fmt.Errorf("grow: %w", err)
		}
	}

	// Direct copy to mmap
	copy(fm.mmap[offset:], data)

	// Update offset without atomic in single-writer scenario
	if needed > fm.writePos {
		fm.writePos = needed
		fm.offset.Store(needed) // Atomic only for readers
	}

	return nil
}

func (fm *FileManager) Read(offset int64, size int64) ([]byte, error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	// Check if offset is beyond file size
	if offset >= fm.mapSize {
		return nil, nil
	}

	// Adjust size if it would read beyond mapped region
	if offset+size > fm.mapSize {
		size = fm.mapSize - offset
	}

	// Return empty slice if size is 0 or negative
	if size <= 0 {
		return make([]byte, 0), nil
	}

	// Copy data to avoid returning direct mmap slice
	data := make([]byte, size)
	copy(data, fm.mmap[offset:offset+size])

	return data, nil
}

// func (fm *FileManager) Read(offset int64, size int64) ([]byte, error) {
// 	fm.mu.RLock()

// 	// Fast path: check bounds
// 	if offset >= fm.writePos {
// 		fm.mu.RUnlock()
// 		return make([]byte, 0), nil
// 	}

// 	// Adjust size if needed
// 	if offset+size > fm.writePos {
// 		size = fm.writePos - offset
// 	}

// 	// Zero-copy read for small sizes
// 	if size <= 4096 {
// 		data := make([]byte, size)
// 		copy(data, fm.mmap[offset:offset+size])
// 		fm.mu.RUnlock()
// 		return data, nil
// 	}

// 	// For larger reads, use direct slice
// 	data := fm.mmap[offset : offset+size]
// 	result := make([]byte, size)
// 	copy(result, data)

// 	fm.mu.RUnlock()
// 	return result, nil
// }

func (fm *FileManager) grow(newSize int64) error {
	// Round up to page size
	newSize = (newSize + pageSize - 1) & pageMask

	// Adaptive growth: if we grow too frequently, increase pre-allocation
	if atomic.AddInt32(&fm.growCount, 1) > 3 {
		newSize += preAllocSize * 2
		atomic.StoreInt32(&fm.growCount, 0)
	}

	// Unmap existing
	if err := unix.Munmap(fm.mmap); err != nil {
		return fmt.Errorf("munmap: %w", err)
	}

	// Extend file
	if err := fm.file.Truncate(newSize); err != nil {
		return fmt.Errorf("truncate: %w", err)
	}

	// Optimized mmap initialization
	data, err := unix.Mmap(
		int(fm.file.Fd()),
		0,
		int(newSize),
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_SHARED|unix.MAP_POPULATE, // Pre-fault pages
	)
	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}

	fm.mmap = data
	fm.mapSize = newSize

	return nil
}

func (fm *FileManager) Close() error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	if fm.mmap != nil {
		if err := unix.Munmap(fm.mmap); err != nil {
			return fmt.Errorf("munmap: %w", err)
		}
		fm.mmap = nil
	}

	if err := fm.file.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}

	return nil
}

func (fm *FileManager) Size() int64 {
	// First try to get actual file size
	stat, err := fm.file.Stat()
	if err == nil {
		return stat.Size()
	}

	// Fallback to offset if stat fails
	return fm.offset.Load()
}

func (fm *FileManager) Stat() (fs.FileInfo, error) {
	return fm.file.Stat()
}

func (fm *FileManager) Offset() int64 {
	return fm.offset.Load()
}

func (fm *FileManager) Stats() Stats {
	return Stats{
		Path:     fm.path,
		Size:     fm.mapSize,
		Offset:   fm.offset.Load(),
		ReadOnly: fm.readonly,
	}
}

// file/manager.go
func (fm *FileManager) Truncate(size int64) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	if fm.readonly {
		return fmt.Errorf("file is readonly")
	}

	// Unmap existing
	if err := unix.Munmap(fm.mmap); err != nil {
		return fmt.Errorf("munmap: %w", err)
	}

	// Truncate file
	if err := fm.file.Truncate(size); err != nil {
		return fmt.Errorf("truncate: %w", err)
	}

	// Reinitialize mmap
	if err := fm.mmapInit(size); err != nil {
		return fmt.Errorf("mmap init: %w", err)
	}

	// Update offset if needed
	currentOffset := fm.offset.Load()
	if currentOffset > size {
		fm.offset.Store(size)
	}

	return nil
}
