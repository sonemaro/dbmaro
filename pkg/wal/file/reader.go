package file

import (
	"fmt"
	"sync"
)

const (
	DefaultChunkSize = 64 * 1024 // 64KB
)

type ChunkReader struct {
	bufferPool sync.Pool
	chunkSize  int64
}

func NewChunkReader(chunkSize int64) *ChunkReader {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}

	return &ChunkReader{
		bufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, chunkSize)
			},
		},
		chunkSize: chunkSize,
	}
}

func (r *ChunkReader) ReadChunk(fm *FileManager, offset, maxSize int64) ([]byte, error) {
	// Calculate actual read size
	remaining := maxSize - offset
	if remaining <= 0 {
		return nil, nil // EOF
	}

	readSize := min(remaining, r.chunkSize)

	buf := r.bufferPool.Get().([]byte)
	if cap(buf) < int(readSize) {
		buf = make([]byte, readSize)
	} else {
		buf = buf[:readSize]
	}

	data, err := fm.Read(offset, readSize)
	if err != nil {
		r.bufferPool.Put(buf)
		return nil, fmt.Errorf("read chunk: %w", err)
	}

	// If no data was read (EOF)
	if data == nil {
		r.bufferPool.Put(buf)
		return nil, nil
	}

	// Copy data to pooled buffer
	copy(buf, data)
	return buf[:len(data)], nil
}

func (r *ChunkReader) ReturnBuffer(buf []byte) {
	r.bufferPool.Put(&buf)
}
