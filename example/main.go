package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

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
	flag.Parse()

	log.Printf("Using %s storage for LSN state", *storageType)

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

	// Create WAL reader
	reader := walreader.NewReader(config, Handler)

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
	// Create WAL reader
	reader := walreader.NewReader(config, Handler)

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

func Handler(ctx *walreader.ListenerContext) {
	switch msg := ctx.Message.(type) {
	case *format.Insert:
		fmt.Println("Insert table", msg.TableName)
	case *format.Update:
		fmt.Println("Update table", msg.TableName)
	}

	if err := ctx.Ack(); err != nil {
		slog.Error("ack", "error", err)
	}
}
