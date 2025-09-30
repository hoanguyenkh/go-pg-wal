package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/KyberNetwork/kutils/cache"
	"github.com/hoanguyenkh/go-pg-wal/pkg/state"
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
	storageType := flag.String("storage", "db", "Storage type for LSN state: file, redis, or db")
	useBatch := flag.Bool("batch", false, "Enable batch processing mode")
	flag.Parse()

	log.Printf("Using %s storage for LSN state", *storageType)

	if *useBatch {
		log.Println("Batch processing mode enabled")
		BatchExample()
		return
	}

	// Run the appropriate example based on storage type
	switch *storageType {
	case "file":
		FileExample()
	case "redis":
		RedisExample()
	case "db":
		DBExample()
	default:
		log.Fatalf("Unknown storage type: %s. Use 'file', 'redis', or 'db'", *storageType)
	}
}

// FileExample demonstrates file-based LSN storage (default)
func FileExample() {
	// Create configuration with file store (default)
	config := walreader.NewConfig(PostgresConnStr, ReplicationSlotName, PublicationName,
		"public",
		"pool_positions,pool_state_dbs")
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
	return nil
}

// HandleCommitTransaction processes transaction commit
func (h *MySQLHandler) HandleCommitTransaction() error {
	return nil
}

func RedisExample() {
	redisCache := cache.NewRedisCache(&cache.RedisConfig{})
	config := walreader.NewConfig(PostgresConnStr, ReplicationSlotName, PublicationName,
		"public",
		"pool_positions,pool_state_dbs")
	config.WithRedisStore(redisCache)
	runWALReader(config, "database")
}

func DBExample() {
	// Create database connection for state storage
	// This should be a separate connection from the replication connection
	stateDB, err := sql.Open("pgx", "postgres://postgres:123456@localhost:5432/kyberdata")
	if err != nil {
		log.Fatalf("Failed to connect to state database: %v", err)
	}
	defer stateDB.Close()

	// Test database connection
	ctx := context.Background()
	if err := stateDB.PingContext(ctx); err != nil {
		log.Fatalf("Failed to ping state database: %v", err)
	}
	log.Println("State database connection successful")

	// Create database configuration
	dbConfig := &state.DBConfig{
		DB:        stateDB,
		TableName: "wal_lsn_state", // Optional, defaults to "wal_lsn_state"
	}

	// Create configuration with database store
	config := walreader.NewConfig(
		"postgres://postgres:123456@localhost:5432/kyberdata?replication=database",
		ReplicationSlotName,
		PublicationName,
		"public",
		"pool_positions,pool_state_dbs",
	)

	config, err = config.WithDBStore(dbConfig, "my_slot_lsn")
	if err != nil {
		log.Fatalf("Failed to create DB store: %v", err)
	}

	// Create message handler
	handler := &MySQLHandler{}

	// Create WAL reader
	reader := walreader.NewReader(config, handler)

	// Handle shutdown signals
	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Println("Received stop signal, shutting down...")
		cancel()
	}()

	// Connect to PostgreSQL
	err = reader.Connect(ctx)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer reader.Close(ctx)

	// Start replication
	err = reader.StartReplication(ctx)
	if err != nil {
		log.Fatalf("Failed to start replication: %v", err)
	}

	log.Println("Starting WAL replication with database state storage...")

	// Run the main replication loop
	err = reader.Run(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("Replication error: %v", err)
	}

	log.Println("Application stopped.")
}

// BatchExample demonstrates batch processing mode
func BatchExample() {
	// Create configuration with batch processing enabled
	config := walreader.NewConfig(PostgresConnStr, ReplicationSlotName, PublicationName,
		"public",
		"pool_positions,pool_state_dbs")
	config.WithFileStore(LsnStateFile)

	// Configure batch processing
	config.BatchSize = 100                // Process 100 messages at a time
	config.BatchTimeout = 5 * time.Second // Or flush after 5 seconds

	// Create batch message handler
	handler := &BatchHandler{}

	// Create WAL reader
	reader := walreader.NewReader(config, handler)

	// Handle shutdown signals
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

	log.Println("Starting WAL replication with batch processing...")

	// Run the main replication loop
	err = reader.Run(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("Replication error: %v", err)
	}

	log.Println("Application stopped.")
}

// BatchHandler implements walreader.BatchMessageHandler for batch processing
type BatchHandler struct {
	walreader.DefaultHandler
}

// HandleBatch processes a batch of messages at once
func (h *BatchHandler) HandleBatch(messages []walreader.BatchMessage) error {
	log.Printf("Processing batch of %d messages", len(messages))

	// Count message types
	inserts := 0
	updates := 0
	deletes := 0
	relations := 0

	for _, msg := range messages {
		switch msg.Type {
		case "insert":
			inserts++
			// Process insert in batch
			// e.g., collect all inserts and do bulk insert to target database
			log.Printf("  [INSERT] Table: %s, Data: %+v", msg.Insert.TableName, msg.Insert.Decoded)

		case "update":
			updates++
			// Process update in batch
			log.Printf("  [UPDATE] Table: %s, New: %+v", msg.Update.TableName, msg.Update.NewDecoded)

		case "delete":
			deletes++
			// Process delete in batch
			log.Printf("  [DELETE] Table: %s, Data: %+v", msg.Delete.TableName, msg.Delete.OldDecoded)

		case "relation":
			relations++
			log.Printf("  [RELATION] Table: %s.%s", msg.Relation.Namespace, msg.Relation.Name)
		}
	}

	log.Printf("Batch summary: %d inserts, %d updates, %d deletes, %d relations",
		inserts, updates, deletes, relations)

	// Here you can do bulk operations like:
	// - Bulk insert to database
	// - Bulk update using transaction
	// - Send batch to message queue
	// - etc.

	return nil
}
