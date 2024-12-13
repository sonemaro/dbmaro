package checksum

import (
	"encoding/binary"
	"hash"
	"hash/crc32"
	"sync"

	"github.com/sonemaro/dbmaro/pkg/wal/options"
)

const (
	Size = 4 // CRC32 size in bytes

)

type Manager struct {
	config options.ChecksumConfig
	table  *crc32.Table
	pool   sync.Pool
}

func NewManager(config options.ChecksumConfig) *Manager {
	m := &Manager{
		config: config,
		table:  crc32.MakeTable(config.Algorithm),
	}

	m.pool.New = func() interface{} {
		return crc32.New(m.table)
	}

	return m
}

func (m *Manager) Calculate(data []byte) uint32 {
	if len(data) <= int(m.config.DirectThreshold) {
		return crc32.Checksum(data, m.table)
	}

	h := m.pool.Get().(hash.Hash32)
	defer func() {
		h.Reset()
		m.pool.Put(h)
	}()

	h.Write(data)
	return h.Sum32()
}

func (m *Manager) Append(data []byte) []byte {
	result := make([]byte, len(data)+Size)
	copy(result, data)
	binary.BigEndian.PutUint32(result[len(data):], m.Calculate(data))

	return result
}

func (m *Manager) Verify(data []byte, checksum uint32) bool {
	return m.Calculate(data) == checksum
}

func (m *Manager) ExtractAndVerify(data []byte) ([]byte, bool) {
	if len(data) < Size {
		return nil, false
	}

	contentLen := len(data) - Size
	content := data[:contentLen]
	storedChecksum := binary.BigEndian.Uint32(data[contentLen:])

	return content, m.Verify(content, storedChecksum)
}
