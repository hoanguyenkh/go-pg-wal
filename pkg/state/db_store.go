package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/hoanguyenkh/go-pg-wal/pkg/utils"
	"github.com/jackc/pglogrepl"
)

// DBStore implements IStateStore using SQL database
type DBStore struct {
	db        *sql.DB
	tableName string
}

// DBConfig holds database connection configuration
type DBConfig struct {
	DB        *sql.DB // Database connection
	TableName string  // Table name to store LSN (default: "wal_lsn_state")
}

// NewDBStore creates a new database-based state store
func NewDBStore(config *DBConfig) (*DBStore, error) {
	if config == nil || config.DB == nil {
		return nil, fmt.Errorf("database connection is required")
	}

	tableName := config.TableName
	if tableName == "" {
		tableName = utils.WalLsnState
	}

	store := &DBStore{
		db:        config.DB,
		tableName: tableName,
	}

	// Create table if it doesn't exist
	err := store.createTableIfNotExists()
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	return store, nil
}

// createTableIfNotExists creates the LSN state table if it doesn't exist
func (ds *DBStore) createTableIfNotExists() error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			key_name VARCHAR(255) PRIMARY KEY,
			lsn_value VARCHAR(255) NOT NULL,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`, ds.tableName)

	_, err := ds.db.Exec(query)
	return err
}

// SaveLSN saves the LSN to database
func (ds *DBStore) SaveLSN(ctx context.Context, key string, lsn pglogrepl.LSN) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (key_name, lsn_value, updated_at) 
		VALUES ($1, $2, CURRENT_TIMESTAMP)
		ON CONFLICT (key_name) 
		DO UPDATE SET lsn_value = $2, updated_at = CURRENT_TIMESTAMP
	`, ds.tableName)

	_, err := ds.db.ExecContext(ctx, query, key, lsn.String())
	if err != nil {
		return fmt.Errorf("failed to save LSN to database: %w", err)
	}
	return nil
}

// LoadLSN loads the LSN from database
func (ds *DBStore) LoadLSN(ctx context.Context, key string) (pglogrepl.LSN, error) {
	query := fmt.Sprintf("SELECT lsn_value FROM %s WHERE key_name = $1", ds.tableName)

	var lsnStr string
	err := ds.db.QueryRowContext(ctx, query, key).Scan(&lsnStr)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, fmt.Errorf("LSN key not found: %s", key)
		}
		return 0, fmt.Errorf("failed to load LSN from database: %w", err)
	}

	lsn, err := pglogrepl.ParseLSN(lsnStr)
	if err != nil {
		return 0, fmt.Errorf("failed to parse LSN from database: %w", err)
	}

	return lsn, nil
}

// Close closes the database connection
func (ds *DBStore) Close() error {
	return ds.db.Close()
}

// Ping tests the database connection
func (ds *DBStore) Ping(ctx context.Context) error {
	return ds.db.PingContext(ctx)
}
