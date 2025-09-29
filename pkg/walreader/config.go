package walreader

import (
	"time"

	"github.com/KyberNetwork/kutils/cache"
	"github.com/hoanguyenkh/go-pg-wal/pkg/state"
)

// Config holds configuration for the WAL reader
type Config struct {
	// PostgreSQL connection string with replication=database parameter
	ConnString string

	// Replication slot name
	SlotName string

	// Publication name
	PublicationName string

	// StateStore for LSN persistence (if nil, uses file store with LSNStateFile)
	StateStore state.IStateStore

	// LSN state key for storage (default: slot name)
	LSNStateKey string

	// File path to store LSN state (used only when StateStore is nil)
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
		LSNStateKey:           slotName, // Use slot name as default key
		LSNStateFile:          "wal_sync_state.lsn",
		StandbyMessageTimeout: 10 * time.Second,
		PluginArgs:            []string{"proto_version '1'"},
	}
}

// WithFileStore sets the config to use file-based LSN storage
func (c *Config) WithFileStore(filename string) *Config {
	c.StateStore = state.NewFileStore()
	c.LSNStateFile = filename
	c.LSNStateKey = filename // Use filename as key for file store
	return c
}

// WithDBStore sets the config to use database-based LSN storage
func (c *Config) WithDBStore(dbConfig *state.DBConfig, key string) (*Config, error) {
	dbStore, err := state.NewDBStore(dbConfig)
	if err != nil {
		return nil, err
	}
	c.StateStore = dbStore
	if key != "" {
		c.LSNStateKey = key
	}
	return c, nil
}

func (c *Config) WithRedisStore(redisCache *cache.RedisCache) *Config {
	redisStore := state.NewRedisStore(redisCache, c.PublicationName)
	c.StateStore = redisStore
	return c
}
