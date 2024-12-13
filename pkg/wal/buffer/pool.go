package buffer

import (
	"sync"

	"github.com/sonemaro/dbmaro/pkg/wal/options"
)

type BufferPool struct {
	small  sync.Pool
	medium sync.Pool
	large  sync.Pool
	config options.PoolConfig
}

func NewBufferPool(config options.PoolConfig) *BufferPool {
	bp := &BufferPool{config: config}

	bp.small.New = func() interface{} {
		return make([]byte, 0, config.SmallBufferSize)
	}

	bp.medium.New = func() interface{} {
		return make([]byte, 0, config.MediumBufferSize)
	}

	bp.large.New = func() interface{} {
		return make([]byte, 0, config.LargeBufferSize)
	}

	return bp
}

func (bp *BufferPool) Get(size int64) []byte {
	switch {
	case size <= bp.config.SmallBufferThreshold:
		buf := bp.small.Get().([]byte)
		return buf[:0]
	case size <= bp.config.MediumBufferThreshold:
		buf := bp.medium.Get().([]byte)
		return buf[:0]
	default:
		if size <= bp.config.LargeBufferSize {
			buf := bp.large.Get().([]byte)
			return buf[:0]
		}
		return make([]byte, 0, size)
	}
}

func (bp *BufferPool) Put(buf []byte) {
	cap := int64(cap(buf))
	switch {
	case cap <= bp.config.SmallBufferThreshold:
		bp.small.Put(buf)
	case cap <= bp.config.MediumBufferThreshold:
		bp.medium.Put(buf)
	case cap <= bp.config.LargeBufferSize:
		bp.large.Put(buf)
	}
	// Larger buffers are dropped for GC
}
