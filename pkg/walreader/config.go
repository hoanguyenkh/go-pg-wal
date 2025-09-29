package walreader

import "time"

// Config holds configuration for the WAL reader
type Config struct {
	// PostgreSQL connection string with replication=database parameter
	ConnString string

	// Replication slot name
	SlotName string

	// Publication name
	PublicationName string

	// File path to store LSN state for resuming from last position
	LSNStateFile string

	// Timeout for standby status messages (default: 10s)
	StandbyMessageTimeout time.Duration

	// Plugin arguments (default: proto_version '1')
	PluginArgs []string
}

// NewConfig creates a new config with default values
func NewConfig(connString, slotName, publicationName string) *Config {
	return &Config{
		ConnString:            connString,
		SlotName:              slotName,
		PublicationName:       publicationName,
		LSNStateFile:          "wal_sync_state.lsn",
		StandbyMessageTimeout: 10 * time.Second,
		PluginArgs:            []string{"proto_version '1'"},
	}
}
