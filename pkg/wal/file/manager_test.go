package file

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestFileManager(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	t.Run("basic operations", func(t *testing.T) {
		fm, err := NewFileManager(path, nil)
		if err != nil {
			t.Fatalf("NewFileManager() error = %v", err)
		}
		defer fm.Close()

		// Write
		data := []byte("hello world")
		if err := fm.Write(data, 0); err != nil {
			t.Errorf("Write() error = %v", err)
		}

		// Read
		read, err := fm.Read(0, int64(len(data)))
		if err != nil {
			t.Errorf("Read() error = %v", err)
		}
		if !bytes.Equal(read, data) {
			t.Errorf("Read() = %v, want %v", read, data)
		}
	})

	t.Run("readonly mode", func(t *testing.T) {
		fm, err := NewFileManager(path, &Options{ReadOnly: true})
		if err != nil {
			t.Fatalf("NewFileManager() error = %v", err)
		}
		defer fm.Close()

		if err := fm.Write([]byte("test"), 0); err == nil {
			t.Error("Write() should fail in readonly mode")
		}
	})

	t.Run("grow file", func(t *testing.T) {
		fm, err := NewFileManager(path, &Options{InitSize: 4096})
		if err != nil {
			t.Fatalf("NewFileManager() error = %v", err)
		}
		defer fm.Close()

		largeData := make([]byte, 8192)
		if err := fm.Write(largeData, 0); err != nil {
			t.Errorf("Write() error = %v", err)
		}

		if fm.Size() < 8192 {
			t.Errorf("Size() = %v, want >= %v", fm.Size(), 8192)
		}
	})

	t.Run("concurrent operations", func(t *testing.T) {
		fm, err := NewFileManager(path, nil)
		if err != nil {
			t.Fatalf("NewFileManager() error = %v", err)
		}
		defer fm.Close()

		done := make(chan bool)
		for i := 0; i < 10; i++ {
			go func(offset int64) {
				data := []byte("test")
				if err := fm.Write(data, offset); err != nil {
					t.Errorf("Write() error = %v", err)
				}
				done <- true
			}(int64(i * 4))
		}

		for i := 0; i < 10; i++ {
			<-done
		}
	})

	t.Run("truncate", func(t *testing.T) {
		fm, err := NewFileManager(path, nil)
		if err != nil {
			t.Fatalf("NewFileManager() error = %v", err)
		}
		defer fm.Close()

		initialSize := fm.Size()
		newSize := initialSize / 2

		if err := fm.Truncate(newSize); err != nil {
			t.Errorf("Truncate() error = %v", err)
		}

		if fm.Size() != newSize {
			t.Errorf("Size() = %v, want %v", fm.Size(), newSize)
		}
	})

	t.Run("stats", func(t *testing.T) {
		fm, err := NewFileManager(path, &Options{ReadOnly: true})
		if err != nil {
			t.Fatalf("NewFileManager() error = %v", err)
		}
		defer fm.Close()

		stats := fm.Stats()
		if stats.Path != path {
			t.Errorf("Stats().Path = %v, want %v", stats.Path, path)
		}
		if !stats.ReadOnly {
			t.Error("Stats().ReadOnly = false, want true")
		}
	})
}

func TestFileManagerErrors(t *testing.T) {
	t.Run("invalid path", func(t *testing.T) {
		_, err := NewFileManager("/nonexistent/path/test.db", nil)
		if err == nil {
			t.Error("NewFileManager() should fail with invalid path")
		}
	})

	t.Run("read beyond size", func(t *testing.T) {
		fm, err := NewFileManager(filepath.Join(t.TempDir(), "test.db"), nil)
		if err != nil {
			t.Fatalf("NewFileManager() error = %v", err)
		}
		defer fm.Close()

		_, err = fm.Read(fm.Size()+1, 10)
		if err == nil {
			t.Error("Read() should fail when reading beyond file size")
		}
	})
}

func TestFileManagerCleanup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	fm, err := NewFileManager(path, nil)
	if err != nil {
		t.Fatalf("NewFileManager() error = %v", err)
	}

	if err := fm.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}

	// Verify file exists after close
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Error("File should exist after close")
	}
}
