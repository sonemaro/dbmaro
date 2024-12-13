package entry

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"
)

func TestHeader(t *testing.T) {
	tests := []struct {
		name        string
		dataSize    uint32
		compression byte
		wantErr     bool
	}{
		{"basic", 100, CompressNone, false},
		{"compressed", 200, CompressLZ4, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewHeader(tt.dataSize, tt.compression)
			data := h.Marshal()

			h2 := &Header{}
			err := h2.Unmarshal(data)
			if (err != nil) != tt.wantErr {
				t.Errorf("Unmarshal() error = %v, wantErr %v", err, tt.wantErr)
			}

			if h.Magic != h2.Magic || h.Version != h2.Version || h.Flags != h2.Flags || h.DataSize != h2.DataSize {
				t.Errorf("Headers don't match: got %+v, want %+v", h2, h)
			}
		})
	}
}

func TestHeaderValidation(t *testing.T) {
	h := &Header{}
	if err := h.Unmarshal(make([]byte, HeaderSize-1)); err == nil {
		t.Error("Expected error for small buffer")
	}

	invalidMagic := make([]byte, HeaderSize)
	if err := h.Unmarshal(invalidMagic); err == nil {
		t.Error("Expected error for invalid magic")
	}
}

func TestEntry(t *testing.T) {
	tests := []struct {
		name    string
		key     []byte
		value   []byte
		wantErr bool
	}{
		{"basic", []byte("key"), []byte("value"), false},
		{"empty_value", []byte("key"), []byte{}, false},
		{"empty_key", []byte{}, []byte("value"), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := NewEntry(tt.key, tt.value)
			data, err := e.Marshal()
			if (err != nil) != tt.wantErr {
				t.Errorf("Marshal() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			e2 := &Entry{}
			if err := e2.Unmarshal(data); err != nil {
				t.Errorf("Unmarshal() error = %v", err)
				return
			}

			if !bytes.Equal(e.Key, e2.Key) || !bytes.Equal(e.Value, e2.Value) {
				t.Errorf("Entries don't match: got %+v, want %+v", e2, e)
			}
		})
	}
}

func TestEntryValidation(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		wantErr bool
	}{
		{
			name:    "too_small",
			data:    make([]byte, 15),
			wantErr: true,
		},
		{
			name: "invalid_key_len",
			data: func() []byte {
				buf := make([]byte, 16)
				binary.BigEndian.PutUint32(buf[8:], 1000) // large key length
				return buf
			}(),
			wantErr: true,
		},
		{
			name: "invalid_value_len",
			data: func() []byte {
				buf := make([]byte, 20)
				binary.BigEndian.PutUint32(buf[8:], 4)     // key length
				binary.BigEndian.PutUint32(buf[16:], 1000) // large value length
				return buf
			}(),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &Entry{}
			err := e.Unmarshal(tt.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("Unmarshal() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestEntrySize(t *testing.T) {
	e := NewEntry([]byte("key"), []byte("value"))
	expected := int64(8 + 4 + len(e.Key) + 4 + len(e.Value))
	if size := e.Size(); size != expected {
		t.Errorf("Size() = %v, want %v", size, expected)
	}
}

func TestEntryTimestamp(t *testing.T) {
	before := time.Now().UnixNano()
	e := NewEntry([]byte("key"), []byte("value"))
	after := time.Now().UnixNano()

	if e.Timestamp < before || e.Timestamp > after {
		t.Errorf("Timestamp %v not within expected range [%v, %v]", e.Timestamp, before, after)
	}
}

func TestHeaderCompression(t *testing.T) {
	h := NewHeader(100, CompressLZ4)
	if !h.IsCompressed() {
		t.Error("Expected header to be compressed")
	}

	h = NewHeader(100, CompressNone)
	if h.IsCompressed() {
		t.Error("Expected header to not be compressed")
	}
}
