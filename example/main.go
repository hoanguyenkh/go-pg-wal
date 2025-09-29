package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/KyberNetwork/kutils/cache"
	_ "github.com/jackc/pgx/v5/stdlib" // Import pgx driver for database/sql

	"github.com/hoanguyenkh/go-pg-wal/pkg/message/format"
	"github.com/hoanguyenkh/go-pg-wal/pkg/walreader"
)

const (
	PostgresConnStr     = "postgres://postgres:123456@localhost:5432/kyberdata?replication=database"
	ReplicationSlotName = "my_replication_slot"
	PublicationName     = "my_publication"
	LsnStateFile        = "wal_sync_state.lsn"
)

func main() {
	// Parse command line flags
	storageType := flag.String("storage", "redis", "Storage type for LSN state: file, redis, or db")
	flag.Parse()

	log.Printf("Using %s storage for LSN state", *storageType)

	// Run the appropriate example based on storage type
	switch *storageType {
	case "file":
		FileExample()
	case "redis":
		RedisExample()
	default:
		log.Fatalf("Unknown storage type: %s. Use 'file', 'redis', or 'db'", *storageType)
	}
}

// FileExample demonstrates file-based LSN storage (default)
func FileExample() {
	// Create configuration with file store (default)
	config := walreader.NewConfig(PostgresConnStr, ReplicationSlotName, PublicationName)
	config.WithFileStore(LsnStateFile)

	runWALReader(config, "file")
}

// runWALReader is the common WAL reader logic
func runWALReader(config *walreader.Config, storageType string) {
	// Create message handler
	handler := &MySQLHandler{}

	// Create WAL reader
	reader := walreader.NewReader(config, handler)

	// Handle shutdown signals to exit gracefully
	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Println("Received stop signal, shutting down...")
		cancel()
	}()

	// Connect to PostgreSQL
	err := reader.Connect(ctx)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer reader.Close(ctx)

	// Start replication
	err = reader.StartReplication(ctx)
	if err != nil {
		log.Fatalf("Failed to start replication: %v", err)
	}

	log.Printf("Starting WAL replication with %s state storage...", storageType)

	// Run the main replication loop
	err = reader.Run(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("Replication error: %v", err)
	}

	log.Println("Application stopped.")
}

// MySQLHandler implements walreader.MessageHandler to generate MySQL queries
type MySQLHandler struct {
	walreader.DefaultHandler
}

// HandleInsert processes INSERT messages
func (h *MySQLHandler) HandleInsert(msg *format.Insert) error {
	log.Printf("[INSERT] Table: %s", msg.TableName)
	log.Printf("[INSERT] Data: %+v\n", msg.Decoded)
	return nil
}

// HandleUpdate processes UPDATE messages
func (h *MySQLHandler) HandleUpdate(msg *format.Update) error {
	log.Printf("[UPDATE] Table: %s", msg.TableName)
	log.Printf("[UPDATE] Old Data: %+v", msg.OldDecoded)
	log.Printf("[UPDATE] New Data: %+v\n", msg.NewDecoded)
	return nil
}

// HandleDelete processes DELETE messages
func (h *MySQLHandler) HandleDelete(msg *format.Delete) error {
	log.Printf("[DELETE] Table: %s", msg.TableName)
	log.Printf("[DELETE] Data: %+v\n", msg.OldDecoded)
	return nil
}

// HandleRelation processes relation (table schema) messages
func (h *MySQLHandler) HandleRelation(msg *format.Relation) error {
	log.Printf("Received schema for table: %s.%s", msg.Namespace, msg.Name)
	return nil
}

// HandleBeginTransaction processes transaction begin
func (h *MySQLHandler) HandleBeginTransaction() error {
	log.Println("=== Begin transaction ===")
	return nil
}

// HandleCommitTransaction processes transaction commit
func (h *MySQLHandler) HandleCommitTransaction() error {
	log.Println("=== End transaction ===")
	return nil
}

func RedisExample() {
	redisCache := cache.NewRedisCache(&cache.RedisConfig{})
	config := walreader.NewConfig(PostgresConnStr, ReplicationSlotName, PublicationName)
	config.WithRedisStore(redisCache)
	runWALReader(config, "database")
}
