package backends

import (
	"io"
	"time"
)

// Noop is a no-op backend that does nothing.
// This is useful when using only local disk caching (via server.go's localCache)
// without any distributed/remote backend storage.
type Noop struct{}

// NewNoop creates a new no-op backend.
func NewNoop() *Noop {
	return &Noop{}
}

// Put does nothing and always succeeds.
// The local cache in server.go handles the actual storage.
func (n *Noop) Put(actionID, outputID []byte, body io.Reader, bodySize int64) error {
	return nil
}

// Get always returns a miss.
// The local cache in server.go handles retrieving cached entries.
func (n *Noop) Get(actionID []byte) ([]byte, io.ReadCloser, int64, *time.Time, bool, error) {
	return nil, nil, 0, nil, true, nil
}

// Close does nothing.
func (n *Noop) Close() error {
	return nil
}

// Clear does nothing.
// The local cache in server.go manages its own clearing if needed.
func (n *Noop) Clear() error {
	return nil
}
