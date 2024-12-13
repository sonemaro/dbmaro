package options

import (
	"errors"
	"time"
)

type GrowthPolicy string

const (
	GrowthDouble  GrowthPolicy = "double"
	GrowthGradual GrowthPolicy = "gradual"
	GrowthFixed   GrowthPolicy = "fixed"
)

type SyncStrategy int

const (
	SyncAlways SyncStrategy = iota
	SyncPeriodic
	SyncBatched
	SyncNever
	SyncBatch
)

// System limits
const (
	// Memory limits
	MinInitialMapSize = 32 * 1024 * 1024       // 32MB minimum
	MaxInitialMapSize = 256 * 1024 * 1024      // 256MB maximum
	MinMaxMapSize     = 256 * 1024 * 1024      // 256MB minimum
	MaxMaxMapSize     = 4 * 1024 * 1024 * 1024 // 4GB maximum

	// Buffer limits
	MinBufferSize = 64 * 1024        // 64KB minimum
	MaxBufferSize = 1 * 1024 * 1024  // 1MB maximum
	MinReadBuffer = 1 * 1024 * 1024  // 1MB minimum
	MaxReadBuffer = 32 * 1024 * 1024 // 32MB maximum

	// Segment limits
	MinSegmentSize = 64 * 1024 * 1024       // 64MB minimum
	MaxSegmentSize = 2 * 1024 * 1024 * 1024 // 2GB maximum

	// Growth limits
	MinGrowthFactor = 1.1  // 10% minimum growth
	MaxGrowthFactor = 3.0  // 300% maximum growth
	MinUtilization  = 0.5  // 50% minimum threshold
	MaxUtilization  = 0.95 // 95% maximum threshold

	// Sync limits
	MinSyncPeriod = 100 * time.Millisecond
	MaxSyncPeriod = 10 * time.Second
)

type Options struct {
	// Sync configuration
	SyncStrategy SyncStrategy
	SyncPeriod   time.Duration
	SegmentSize  int64

	// Memory management
	InitialMapSize       int64
	MaxMapSize           int64
	UtilizationThreshold float64
	GrowthPolicy         GrowthPolicy
	GrowthFactor         float64
	CustomGrowthSize     int64

	// Performance tuning
	BufferSize     int64
	ReadBufferSize int64
	MaxSegmentSize int64

	PoolConfig     PoolConfig
	ChecksumConfig ChecksumConfig
}

func DefaultOptions() Options {
	return Options{
		SyncStrategy: SyncBatched,
		SyncPeriod:   time.Second,
		SegmentSize:  1 * 1024 * 1024 * 1024, // 1GB

		InitialMapSize:       32 * 1024 * 1024,
		MaxMapSize:           2 * 1024 * 1024 * 1024,
		UtilizationThreshold: 0.75,
		GrowthPolicy:         GrowthGradual,
		GrowthFactor:         1.5,
		CustomGrowthSize:     64 * 1024 * 1024,

		BufferSize:     256 * 1024,
		ReadBufferSize: 10 * 1024 * 1024,
		MaxSegmentSize: 1 * 1024 * 1024 * 1024,
		PoolConfig:     DefaultPoolConfig(),
		ChecksumConfig: DefaultChecksumConfig(),
	}
}

var (
	ErrInvalidInitialSize  = errors.New("initial map size out of range")
	ErrInvalidMaxSize      = errors.New("max map size out of range")
	ErrInvalidBufferSize   = errors.New("buffer size out of range")
	ErrInvalidReadBuffer   = errors.New("read buffer size out of range")
	ErrInvalidSegmentSize  = errors.New("segment size out of range")
	ErrInvalidGrowthFactor = errors.New("growth factor out of range")
	ErrInvalidUtilization  = errors.New("utilization threshold out of range")
	ErrInvalidSyncPeriod   = errors.New("sync period out of range")
	ErrInvalidSizeRelation = errors.New("initial size must be less than max size")
)

// Validate checks and adjusts options to be within acceptable ranges
func (o *Options) Validate() error {
	// Validate and clamp memory sizes
	if o.InitialMapSize < MinInitialMapSize || o.InitialMapSize > MaxInitialMapSize {
		return ErrInvalidInitialSize
	}
	if o.MaxMapSize < MinMaxMapSize || o.MaxMapSize > MaxMaxMapSize {
		return ErrInvalidMaxSize
	}
	if o.InitialMapSize >= o.MaxMapSize {
		return ErrInvalidSizeRelation
	}

	// Validate and clamp buffer sizes
	if o.BufferSize < MinBufferSize || o.BufferSize > MaxBufferSize {
		return ErrInvalidBufferSize
	}
	if o.ReadBufferSize < MinReadBuffer || o.ReadBufferSize > MaxReadBuffer {
		return ErrInvalidReadBuffer
	}

	// Validate and clamp segment size
	if o.SegmentSize < MinSegmentSize || o.SegmentSize > MaxSegmentSize {
		return ErrInvalidSegmentSize
	}

	// Validate growth parameters
	if o.GrowthFactor < MinGrowthFactor || o.GrowthFactor > MaxGrowthFactor {
		return ErrInvalidGrowthFactor
	}
	if o.UtilizationThreshold < MinUtilization || o.UtilizationThreshold > MaxUtilization {
		return ErrInvalidUtilization
	}

	// Validate sync period
	if o.SyncStrategy == SyncPeriodic {
		if o.SyncPeriod < MinSyncPeriod || o.SyncPeriod > MaxSyncPeriod {
			return ErrInvalidSyncPeriod
		}
	}

	return nil
}
