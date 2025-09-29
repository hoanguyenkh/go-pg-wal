package state

import (
	"context"
	"os"

	"github.com/jackc/pglogrepl"
)

// FileStore implements IStateStore using file system
type FileStore struct{}

// NewFileStore creates a new file-based state store
func NewFileStore() *FileStore {
	return &FileStore{}
}

// SaveLSN saves the LSN to a file
func (fs *FileStore) SaveLSN(ctx context.Context, filename string, lsn pglogrepl.LSN) error {
	return os.WriteFile(filename, []byte(lsn.String()), 0644)
}

// LoadLSN loads the LSN from a file
func (fs *FileStore) LoadLSN(ctx context.Context, filename string) (pglogrepl.LSN, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return 0, err
	}
	return pglogrepl.ParseLSN(string(data))
}

// Close is a no-op for file store
func (fs *FileStore) Close() error {
	return nil
}
