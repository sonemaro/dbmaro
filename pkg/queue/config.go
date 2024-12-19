package queue

// Config holds all queue configuration
type Config struct {
	// WAL related
	WalDir         string
	BatchSize      int
	BatchTimeoutMs int

	// queue limits
	MaxQueueSize       int64
	MaxPartitions      int
	MaxConsumerGroups  int
	MaxMembersPerGroup int

	// raft related
	RaftDir     string
	RaftAddress string
	RaftPeers   []string
	Bootstrap   bool // whether to bootstrap as first node

	// logging
	LogLevel string
	LogFile  string
}

func DefaultConfig() *Config {
	return &Config{
		BatchSize:          1000,
		BatchTimeoutMs:     100,
		MaxQueueSize:       1000000,
		MaxPartitions:      16,
		MaxConsumerGroups:  100,
		MaxMembersPerGroup: 10,
		LogLevel:           "info",
	}
}
