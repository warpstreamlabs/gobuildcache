package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// localCache manages the local disk cache where Go build tools access cached files.
// It handles writing, reading, and metadata management for cached entries.
type localCache struct {
	cacheDir string // Absolute path to cache directory
	logger   *slog.Logger
}

// localCacheMetadata holds metadata for a cached entry.
type localCacheMetadata struct {
	OutputID []byte
	Size     int64
	PutTime  time.Time
}

// newLocalCache creates a new local cache instance.
// cacheDir is the directory where cached files will be stored.
func newLocalCache(cacheDir string, logger *slog.Logger) (*localCache, error) {
	// Ensure cache directory exists
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create cache directory: %w", err)
	}

	// Convert to absolute path once at initialization
	// This avoids repeated filepath.Abs() calls later
	absCacheDir, err := filepath.Abs(cacheDir)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path: %w", err)
	}

	return &localCache{
		cacheDir: absCacheDir,
		logger:   logger,
	}, nil
}

// actionIDToPath converts an actionID to a local cache file path.
func (lc *localCache) actionIDToPath(actionID []byte) string {
	hexID := hex.EncodeToString(actionID)
	return filepath.Join(lc.cacheDir, hexID)
}

// metadataPath returns the path to the metadata file for an actionID.
func (lc *localCache) metadataPath(actionID []byte) string {
	return lc.actionIDToPath(actionID) + ".meta"
}

// writeMetadata writes metadata for a cache entry.
func (lc *localCache) writeMetadata(actionID []byte, meta localCacheMetadata) error {
	metaPath := lc.metadataPath(actionID)

	// Format: outputID:hex\nsize:num\ntime:unix\n
	content := fmt.Sprintf("outputID:%s\nsize:%d\ntime:%d\n",
		hex.EncodeToString(meta.OutputID),
		meta.Size,
		meta.PutTime.Unix())

	// Write to temp file first for atomic operation
	tmpPath := metaPath + ".tmp"
	if err := os.WriteFile(tmpPath, []byte(content), 0644); err != nil {
		return fmt.Errorf("failed to write temp metadata: %w", err)
	}

	// Atomically rename
	if err := os.Rename(tmpPath, metaPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to rename metadata: %w", err)
	}

	return nil
}

// readMetadata reads metadata for a cache entry.
// Returns an error if metadata doesn't exist or is corrupted.
func (lc *localCache) readMetadata(actionID []byte) (*localCacheMetadata, error) {
	metaPath := lc.metadataPath(actionID)

	data, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}

	var outputIDHex string
	var size int64
	var putTimeUnix int64

	// Parse each line
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "outputID:") {
			fmt.Sscanf(line, "outputID:%s", &outputIDHex)
		} else if strings.HasPrefix(line, "size:") {
			fmt.Sscanf(line, "size:%d", &size)
		} else if strings.HasPrefix(line, "time:") {
			fmt.Sscanf(line, "time:%d", &putTimeUnix)
		}
	}

	if outputIDHex == "" {
		return nil, fmt.Errorf("metadata missing outputID field")
	}

	outputID, err := hex.DecodeString(outputIDHex)
	if err != nil {
		return nil, fmt.Errorf("failed to decode outputID: %w", err)
	}

	return &localCacheMetadata{
		OutputID: outputID,
		Size:     size,
		PutTime:  time.Unix(putTimeUnix, 0),
	}, nil
}

// Write atomically writes data from a reader to the local cache.
// Returns the absolute path to the cached file.
func (lc *localCache) write(actionID []byte, body io.Reader) (string, error) {
	diskPath := lc.actionIDToPath(actionID)

	// Create a temporary file in the same directory for atomic write
	tmpFile, err := os.CreateTemp(lc.cacheDir, ".tmp-*")
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath) // Clean up if something goes wrong

	// Copy data to temp file
	_, err = io.Copy(tmpFile, body)
	closeErr := tmpFile.Close()
	if err != nil {
		return "", fmt.Errorf("failed to write to temp file: %w", err)
	}
	if closeErr != nil {
		return "", fmt.Errorf("failed to close temp file: %w", closeErr)
	}

	// Atomically rename temp file to final destination
	if err := os.Rename(tmpPath, diskPath); err != nil {
		return "", fmt.Errorf("failed to rename cache file: %w", err)
	}

	// diskPath is already absolute (cacheDir is absolute)
	return diskPath, nil
}

// WriteWithMetadata writes data and metadata to the local cache.
// Returns the absolute path to the cached file.
func (lc *localCache) writeWithMetadata(actionID []byte, body io.Reader, meta localCacheMetadata) (string, error) {
	// Write data
	diskPath, err := lc.write(actionID, body)
	if err != nil {
		return "", err
	}

	// Write metadata
	if err := lc.writeMetadata(actionID, meta); err != nil {
		lc.logger.Warn("failed to write local cache metadata",
			"actionID", hex.EncodeToString(actionID),
			"error", err)
		// Continue - data is cached, just missing metadata
	}

	return diskPath, nil
}

// Check checks if a file exists in the local cache and returns its metadata.
// Returns nil if not found, and logs a warning if metadata is missing/corrupted.
func (lc *localCache) check(actionID []byte) *localCacheMetadata {
	// Try to read metadata directly (avoids extra Stat syscall)
	// If the data file doesn't exist, the metadata file likely won't either
	meta, err := lc.readMetadata(actionID)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// Neither data nor metadata exists - this is a cache miss
			return nil
		}

		lc.logger.Warn(
			"failed to read local cache metadata",
			"actionID", hex.EncodeToString(actionID),
			"error", err,
		)

		// Metadata is missing or corrupted but data file might exist
		// Check if data file exists
		diskPath := lc.actionIDToPath(actionID)
		if _, statErr := os.Stat(diskPath); statErr == nil {
			// Data file exists but metadata is missing/corrupted
			lc.logger.Warn(
				"local cache file exists but metadata is missing/corrupted",
				"actionID", hex.EncodeToString(actionID),
				"error", err,
			)
		}
		return nil
	}

	return meta
}

// GetPath returns the absolute path for an actionID in the local cache.
// Does not check if the file actually exists.
func (lc *localCache) getPath(actionID []byte) string {
	// Since cacheDir is already absolute, the path is already absolute
	return lc.actionIDToPath(actionID)
}
