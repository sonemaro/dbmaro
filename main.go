package main

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/sonemaro/dbmaro/pkg/wal"
	"github.com/sonemaro/dbmaro/pkg/wal/entry"
	"github.com/sonemaro/dbmaro/pkg/wal/options"
)

// FileEntry represents a file in our package
type FileEntry struct {
	Path    string
	Content []byte
}

func main() {
	if len(os.Args) != 4 {
		fmt.Printf("Usage: %s <input_file> <wal_dir> <output_file>\n", os.Args[0])
		os.Exit(1)
	}

	inputFile := os.Args[1]  // package_contents.txt
	walDir := os.Args[2]     // directory for WAL
	outputFile := os.Args[3] // reconstructed file

	// Step 1: Read the package contents file
	files, err := readPackageContents(inputFile)
	if err != nil {
		fmt.Printf("Failed to read package contents: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Read %d files from package contents\n", len(files))

	// Step 2: Write to WAL
	if err := writeToWAL(files, walDir); err != nil {
		fmt.Printf("Failed to write to WAL: %v\n", err)
		os.Exit(1)
	}

	// Step 3: Read from WAL and reconstruct
	if err := reconstructFromWAL(walDir, outputFile); err != nil {
		fmt.Printf("Failed to reconstruct from WAL: %v\n", err)
		os.Exit(1)
	}

	// Step 4: Compare files
	if err := compareFiles(inputFile, outputFile); err != nil {
		fmt.Printf("Files differ: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Success! Files are identical")
}

func readPackageContents(filename string) ([]FileEntry, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var files []FileEntry
	var currentFile FileEntry
	var content bytes.Buffer

	scanner := bufio.NewScanner(file)
	isFirstLine := true

	for scanner.Scan() {
		line := scanner.Text()

		if strings.HasPrefix(line, "=== ") && strings.HasSuffix(line, " ===") {
			// Save previous file if exists
			if currentFile.Path != "" {
				currentFile.Content = content.Bytes()
				files = append(files, currentFile)
				content.Reset()
			}

			// Start new file
			currentFile.Path = strings.TrimPrefix(strings.TrimSuffix(line, " ==="), "=== ")
			isFirstLine = true
		} else {
			if !isFirstLine {
				content.WriteByte('\n')
			}
			content.WriteString(line)
			isFirstLine = false
		}
	}

	// Save last file
	if currentFile.Path != "" {
		currentFile.Content = content.Bytes()
		files = append(files, currentFile)
	}

	return files, scanner.Err()
}

func writeToWAL(files []FileEntry, walDir string) error {
	opts := options.DefaultOptions()
	w, err := wal.Open(filepath.Join(walDir, "test.wal"), opts)
	if err != nil {
		return fmt.Errorf("open WAL: %w", err)
	}
	defer w.Close()

	for _, file := range files {
		e := entry.NewEntry([]byte(file.Path), file.Content)
		if err := w.Write(e); err != nil {
			return fmt.Errorf("write entry: %w", err)
		}
	}

	return w.Sync()
}

func reconstructFromWAL(walDir, outputFile string) error {
	opts := options.DefaultOptions()
	w, err := wal.Open(filepath.Join(walDir, "test.wal"), opts)
	if err != nil {
		return fmt.Errorf("open WAL: %w", err)
	}
	defer w.Close()

	entries, err := w.Read()
	if err != nil {
		return fmt.Errorf("read entries: %w", err)
	}

	out, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("create output file: %w", err)
	}
	defer out.Close()

	writer := bufio.NewWriter(out)
	for i, e := range entries {
		if i > 0 {
			writer.WriteString("\n")
		}
		fmt.Fprintf(writer, "=== %s ===\n\n", string(e.Key))
		writer.Write(e.Value)
	}

	return writer.Flush()
}

func compareFiles(file1, file2 string) error {
	content1, err := os.ReadFile(file1)
	if err != nil {
		return fmt.Errorf("reading first file: %w", err)
	}

	content2, err := os.ReadFile(file2)
	if err != nil {
		return fmt.Errorf("reading second file: %w", err)
	}

	if len(content1) != len(content2) {
		return fmt.Errorf("file sizes differ: %d != %d", len(content1), len(content2))
	}

	// Find first difference
	for i := 0; i < len(content1); i++ {
		if content1[i] != content2[i] {
			// Print context around difference
			start := max(0, i-50)
			end := min(len(content1), i+50)

			fmt.Printf("Difference at offset %d:\n", i)
			fmt.Printf("Original: %q\n", content1[start:end])
			fmt.Printf("Reconstructed: %q\n", content2[start:end])

			return fmt.Errorf("content differs at offset %d", i)
		}
	}

	return nil
}
