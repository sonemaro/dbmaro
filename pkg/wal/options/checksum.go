package options

import "hash/crc32"

const (
	defaultPoolSize        = 128
	defaultDirectThreshold = 4 * 1024 // 4KB
)

type ChecksumConfig struct {
	// Checksum type
	Algorithm uint32 // crc32.IEEE or crc32.Castagnoli

	// Pool settings
	PoolSize int // Number of hash instances to pool

	// Size thresholds
	DirectThreshold int64 // Use direct calculation if size <= this
}

func DefaultChecksumConfig() ChecksumConfig {
	return ChecksumConfig{
		Algorithm:       crc32.IEEE,
		PoolSize:        defaultPoolSize,
		DirectThreshold: defaultDirectThreshold,
	}
}
