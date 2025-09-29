package state

import (
	"context"

	"github.com/jackc/pglogrepl"
)

// IStateStore defines the interface for storing and retrieving LSN state
type IStateStore interface {
	// SaveLSN saves the current LSN
	SaveLSN(ctx context.Context, key string, lsn pglogrepl.LSN) error

	// LoadLSN loads the LSN from storage
	LoadLSN(ctx context.Context, key string) (pglogrepl.LSN, error)

	// Close closes the storage connection
	Close() error
}
