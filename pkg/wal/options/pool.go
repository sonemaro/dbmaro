package options

type PoolConfig struct {
	// Buffer pool sizes
	SmallBufferSize  int64 // For entries < 4KB
	MediumBufferSize int64 // For entries < 64KB
	LargeBufferSize  int64 // For larger entries

	// Pool initial capacities
	SmallPoolSize  int // Number of small buffers to pre-allocate
	MediumPoolSize int // Number of medium buffers
	LargePoolSize  int // Number of large buffers

	// Thresholds for switching between pools
	SmallBufferThreshold  int64 // Use small buffer if size <= this
	MediumBufferThreshold int64 // Use medium buffer if size <= this
}

func DefaultPoolConfig() PoolConfig {
	return PoolConfig{
		SmallBufferSize:  4 * 1024,
		MediumBufferSize: 64 * 1024,
		LargeBufferSize:  1 * 1024 * 1024,

		SmallPoolSize:  1024,
		MediumPoolSize: 128,
		LargePoolSize:  32,

		SmallBufferThreshold:  4 * 1024,
		MediumBufferThreshold: 64 * 1024,
	}
}
