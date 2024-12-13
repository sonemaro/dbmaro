package checksum

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestChecksum(t *testing.T) {
	m := NewManager()

	t.Run("basic calculation", func(t *testing.T) {
		data := []byte("hello world")
		checksum1 := m.Calculate(data)
		checksum2 := m.Calculate(data)

		if checksum1 != checksum2 {
			t.Error("Same data should produce same checksum")
		}

		// Modify data
		data[0] = 'H'
		checksum3 := m.Calculate(data)
		if checksum1 == checksum3 {
			t.Error("Modified data should produce different checksum")
		}
	})

	t.Run("append and verify", func(t *testing.T) {
		data := []byte("test data")
		withChecksum := m.Append(data)

		if len(withChecksum) != len(data)+Size {
			t.Errorf("Expected length %d, got %d", len(data)+Size, len(withChecksum))
		}

		content, valid := m.ExtractAndVerify(withChecksum)
		if !valid {
			t.Error("Checksum verification failed")
		}

		if !bytes.Equal(content, data) {
			t.Error("Extracted content doesn't match original")
		}
	})

	t.Run("concurrent operations", func(t *testing.T) {
		data := []byte("concurrent test")
		done := make(chan bool)

		for i := 0; i < 10; i++ {
			go func() {
				checksum := m.Calculate(data)
				if !m.Verify(data, checksum) {
					t.Error("Concurrent verification failed")
				}
				done <- true
			}()
		}

		for i := 0; i < 10; i++ {
			<-done
		}
	})

	t.Run("data corruption", func(t *testing.T) {
		data := []byte("test data")
		withChecksum := m.Append(data)

		// Corrupt the data
		withChecksum[0] ^= 0xFF

		_, valid := m.ExtractAndVerify(withChecksum)
		if valid {
			t.Error("Corrupted data should fail verification")
		}
	})

	t.Run("empty data", func(t *testing.T) {
		checksum := m.Calculate([]byte{})
		if !m.Verify([]byte{}, checksum) {
			t.Error("Empty data checksum verification failed")
		}
	})

	t.Run("invalid size", func(t *testing.T) {
		_, valid := m.ExtractAndVerify([]byte{1, 2, 3}) // Less than Size
		if valid {
			t.Error("Should fail for data smaller than checksum size")
		}
	})

	t.Run("checksum extraction", func(t *testing.T) {
		data := []byte("test")
		checksum := uint32(0x12345678)

		combined := make([]byte, len(data)+Size)
		copy(combined, data)
		binary.BigEndian.PutUint32(combined[len(data):], checksum)

		content, valid := m.ExtractAndVerify(combined)
		if valid { // Should be false as checksum won't match
			t.Error("Invalid checksum should fail verification")
		}
		if !bytes.Equal(content, data) {
			t.Error("Extracted content doesn't match even with invalid checksum")
		}
	})
}

func BenchmarkChecksum(b *testing.B) {
	m := NewManager()
	data := bytes.Repeat([]byte("benchmark"), 100)

	b.Run("calculate", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = m.Calculate(data)
		}
	})

	b.Run("append", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = m.Append(data)
		}
	})

	withChecksum := m.Append(data)
	b.Run("verify", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = m.ExtractAndVerify(withChecksum)
		}
	})
}
