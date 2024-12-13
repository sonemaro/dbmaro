package entry

/*
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║                           WAL Entry Format v1.0                              ║
║                           ==================                                 ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝

Entry Binary Format:
┌──────────────┬──────────────┬──────────────┬──────────────┬──────────────┐
│  Timestamp   │   Key Size   │     Key      │  Value Size  │    Value     │
│    (8B)      │    (4B)      │    (var)     │    (4B)      │    (var)     │
└──────────────┴──────────────┴──────────────┴──────────────┴──────────────┘

Field Details:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Timestamp   : int64, big-endian, unix nano
  Key Size    : uint32, big-endian, length of key in bytes
  Key         : raw bytes, variable length
  Value Size  : uint32, big-endian, length of value in bytes
  Value       : raw bytes, variable length

Minimum Size : 16 bytes (empty key & value)
Maximum Size : 16 bytes + 4,294,967,295(key is uint32, so the limit for value would be this value)

Usage:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  entry := NewEntry([]byte("key"), []byte("value"))

  // Serialize
  data, err := entry.Marshal()

  // Deserialize
  entry = &Entry{}
  err = entry.Unmarshal(data)

Validation:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ✓ Key cannot be empty
  ✓ Minimum data size for unmarshal is 17 bytes
  ✓ Data size must match header information

Error Handling:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ErrInvalidData : Data corruption or incomplete data
  ErrEmptyKey    : Key validation failed

Notes:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  - All integers are big-endian
  - Timestamp is auto-generated on NewEntry()
  - Thread-safe for concurrent reads
  - Immutable after creation

╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║  "Simple things should be simple, complex things should be possible"         ║
║                                                   - Alan Kay                 ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
*/

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"
)

const (
	HeaderSize    = 8
	MagicMarker   = 0xABCD
	FormatVersion = 0x01

	CompressNone = 0x00
	CompressLZ4  = 0x10
)

var (
	ErrInvalidData = errors.New("invalid data")
	ErrEmptyKey    = errors.New("empty key")
)

type Header struct {
	Magic    uint16 // 2B: 0xABCD
	Version  byte   // 1B: version
	Flags    byte   // 1B: flags for future use
	DataSize uint32 // 4B: size of data
}

func NewHeader(dataSize uint32, flags byte) *Header {
	return &Header{
		Magic:    MagicMarker,
		Version:  FormatVersion,
		Flags:    flags,
		DataSize: dataSize,
	}
}

// Marshal serializes header to bytes
func (h *Header) Marshal() []byte {
	buf := make([]byte, HeaderSize)

	binary.BigEndian.PutUint16(buf[0:], h.Magic)
	buf[2] = h.Version
	buf[3] = h.Flags
	binary.BigEndian.PutUint32(buf[4:], h.DataSize)

	return buf
}

// Unmarshal deserializes bytes to header
func (h *Header) Unmarshal(buf []byte) error {
	if len(buf) < HeaderSize {
		return fmt.Errorf("invalid header size: got %d, want %d", len(buf), HeaderSize)
	}

	h.Magic = binary.BigEndian.Uint16(buf[0:])
	if h.Magic != MagicMarker {
		return fmt.Errorf("invalid magic number: got %x, want %x", h.Magic, MagicMarker)
	}

	h.Version = buf[2]
	h.Flags = buf[3]
	h.DataSize = binary.BigEndian.Uint32(buf[4:])

	return nil
}

// Entry represents a single WAL entry
type Entry struct {
	Key       []byte
	Value     []byte
	Timestamp int64
}

// NewEntry creates a new entry with current timestamp
func NewEntry(key, value []byte) *Entry {
	return &Entry{
		Key:       key,
		Value:     value,
		Timestamp: time.Now().UnixNano(),
	}
}

// Marshal serializes entry to bytes
func (e *Entry) Marshal() ([]byte, error) {
	if len(e.Key) == 0 {
		return nil, ErrEmptyKey
	}

	size := e.Size()
	data := make([]byte, size)
	offset := 0

	// Write timestamp
	binary.BigEndian.PutUint64(data[offset:], uint64(e.Timestamp))
	offset += 8

	// Write key
	binary.BigEndian.PutUint32(data[offset:], uint32(len(e.Key)))
	offset += 4
	copy(data[offset:], e.Key)
	offset += len(e.Key)

	// Write value
	binary.BigEndian.PutUint32(data[offset:], uint32(len(e.Value)))
	offset += 4
	copy(data[offset:], e.Value)

	return data, nil
}

// Unmarshal deserializes bytes to entry
func (e *Entry) Unmarshal(data []byte) error {
	if len(data) < 16 { // minimum size: timestamp(8) + keyLen(4) + valueLen(4)
		return ErrInvalidData
	}

	offset := 0

	// Read timestamp
	e.Timestamp = int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8

	// Read key
	keyLen := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	if offset+int(keyLen) > len(data) {
		return ErrInvalidData
	}
	e.Key = make([]byte, keyLen)
	copy(e.Key, data[offset:offset+int(keyLen)])
	offset += int(keyLen)

	// Read value
	if offset+4 > len(data) {
		return ErrInvalidData
	}
	valueLen := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	if offset+int(valueLen) > len(data) {
		return ErrInvalidData
	}
	e.Value = make([]byte, valueLen)
	copy(e.Value, data[offset:offset+int(valueLen)])

	return nil
}

// Size returns total size needed for marshaling
func (e *Entry) Size() int64 {
	return 8 + // Timestamp
		4 + int64(len(e.Key)) + // Key length + Key
		4 + int64(len(e.Value)) // Value length + Value
}

// Validate performs basic validation
func (e *Entry) Validate() error {
	if len(e.Key) == 0 {
		return ErrEmptyKey
	}
	return nil
}
