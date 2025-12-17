package backends

import (
	"bytes"
	"crypto/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestDiskConcurrentPutSameKey(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "disk-backend-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	backend, err := NewDisk(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	// Use the same actionID for all goroutines
	actionID := []byte("test-action-id")
	numGoroutines := 10

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)

	// Launch multiple goroutines trying to Put with the same actionID
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(iteration int) {
			defer wg.Done()

			outputID := []byte{byte(iteration)}
			data := []byte("test data")
			body := bytes.NewReader(data)

			_, err := backend.Put(actionID, outputID, body, int64(len(data)))
			if err != nil {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("Put failed: %v", err)
	}

	// Verify that we can successfully Get the result
	outputID, diskPath, size, putTime, miss, err := backend.Get(actionID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if miss {
		t.Fatal("Expected cache hit, got miss")
	}
	if outputID == nil {
		t.Fatal("Expected outputID, got nil")
	}
	if diskPath == "" {
		t.Fatal("Expected diskPath, got empty string")
	}
	if size != 9 {
		t.Errorf("Expected size 9, got %d", size)
	}
	if putTime == nil {
		t.Fatal("Expected putTime, got nil")
	}

	t.Logf("Final state: outputID=%v, size=%d", outputID, size)
}

func TestDiskConcurrentPutDifferentKeys(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "disk-backend-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	backend, err := NewDisk(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	numGoroutines := 100
	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)

	// Launch multiple goroutines with different actionIDs
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(iteration int) {
			defer wg.Done()

			actionID := []byte{byte(iteration >> 8), byte(iteration)}
			outputID := []byte{byte(iteration)}
			data := []byte("test data")
			body := bytes.NewReader(data)

			_, err := backend.Put(actionID, outputID, body, int64(len(data)))
			if err != nil {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("Put failed: %v", err)
	}

	// Verify all entries were stored
	for i := 0; i < numGoroutines; i++ {
		actionID := []byte{byte(i >> 8), byte(i)}
		_, _, _, _, miss, err := backend.Get(actionID)
		if err != nil {
			t.Errorf("Get failed for iteration %d: %v", i, err)
		}
		if miss {
			t.Errorf("Expected cache hit for iteration %d, got miss", i)
		}
	}
}

func TestDiskConcurrentGetWhilePut(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "disk-backend-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	backend, err := NewDisk(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	actionID := []byte("test-action-id")
	outputID := []byte("test-output-id")
	data := make([]byte, 1024*1024) // 1MB of data
	rand.Read(data)

	var wg sync.WaitGroup
	errors := make(chan error, 100)

	// Start a Put operation with large data
	wg.Add(1)
	go func() {
		defer wg.Done()
		body := bytes.NewReader(data)
		_, err := backend.Put(actionID, outputID, body, int64(len(data)))
		if err != nil {
			errors <- err
		}
	}()

	// Start multiple Get operations concurrently
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(time.Millisecond) // Small delay to let Put start
			_, _, _, _, _, err := backend.Get(actionID)
			// Get might return miss or hit, both are valid
			// The important thing is it shouldn't error or see partial data
			if err != nil {
				errors <- err
			}
		}()
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("Operation failed: %v", err)
	}

	// Final Get should succeed
	gotOutputID, diskPath, size, _, miss, err := backend.Get(actionID)
	if err != nil {
		t.Fatalf("Final Get failed: %v", err)
	}
	if miss {
		t.Fatal("Expected cache hit, got miss")
	}
	if !bytes.Equal(gotOutputID, outputID) {
		t.Errorf("OutputID mismatch: expected %v, got %v", outputID, gotOutputID)
	}
	if size != int64(len(data)) {
		t.Errorf("Size mismatch: expected %d, got %d", len(data), size)
	}

	// Verify the actual file content
	fileData, err := os.ReadFile(diskPath)
	if err != nil {
		t.Fatalf("Failed to read cache file: %v", err)
	}
	if !bytes.Equal(fileData, data) {
		t.Error("Cache file content doesn't match original data")
	}
}

func TestDiskAtomicPut(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "disk-backend-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	backend, err := NewDisk(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	actionID := []byte("test-action-id")
	outputID := []byte("test-output-id")
	data := []byte("test data")

	// Put data
	_, err = backend.Put(actionID, outputID, bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Verify no temp files are left behind
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("Failed to read dir: %v", err)
	}

	for _, entry := range entries {
		name := entry.Name()
		if filepath.Ext(name) == ".tmp" || bytes.HasPrefix([]byte(name), []byte(".tmp.")) {
			t.Errorf("Found leftover temp file: %s", name)
		}
	}
}

func TestDiskClear(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "disk-backend-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	backend, err := NewDisk(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	// Add some entries
	for i := 0; i < 10; i++ {
		actionID := []byte{byte(i)}
		outputID := []byte{byte(i)}
		data := []byte("test data")
		_, err := backend.Put(actionID, outputID, bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Clear the cache
	if err := backend.Clear(); err != nil {
		t.Fatalf("Clear failed: %v", err)
	}

	// Verify all entries are gone
	for i := 0; i < 10; i++ {
		actionID := []byte{byte(i)}
		_, _, _, _, miss, err := backend.Get(actionID)
		if err != nil {
			t.Errorf("Get failed: %v", err)
		}
		if !miss {
			t.Errorf("Expected miss after clear, got hit for entry %d", i)
		}
	}

	// Verify directory is empty
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("Failed to read dir: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("Expected empty directory, found %d entries", len(entries))
	}
}

func TestDiskGetMiss(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "disk-backend-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	backend, err := NewDisk(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	actionID := []byte("nonexistent")
	_, _, _, _, miss, err := backend.Get(actionID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !miss {
		t.Error("Expected miss for nonexistent key, got hit")
	}
}

func BenchmarkDiskPutConcurrent(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "disk-backend-bench-*")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	backend, err := NewDisk(tmpDir)
	if err != nil {
		b.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	data := []byte("benchmark data")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			actionID := []byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)}
			outputID := []byte{byte(i)}
			body := bytes.NewReader(data)
			_, err := backend.Put(actionID, outputID, body, int64(len(data)))
			if err != nil {
				b.Errorf("Put failed: %v", err)
			}
			i++
		}
	})
}
