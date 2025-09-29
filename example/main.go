package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/hoanguyenkh/go-pg-wal/pkg/message/format"
	"github.com/hoanguyenkh/go-pg-wal/pkg/sql"
	"github.com/hoanguyenkh/go-pg-wal/pkg/walreader"
)

const (
	PostgresConnStr     = "postgres://postgres:123456@localhost:5432/kyberdata?replication=database"
	ReplicationSlotName = "my_replication_slot"
	PublicationName     = "my_publication"
	LsnStateFile        = "wal_sync_state.lsn"
)

func main() {
	// Create configuration
	config := walreader.NewConfig(PostgresConnStr, ReplicationSlotName, PublicationName)
	config.LSNStateFile = LsnStateFile

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
	query, args := sql.BuildInsertQuery(msg.TableName, msg.Decoded)
	log.Printf("[INSERT] Table: %s", msg.TableName)
	log.Printf("[INSERT] Query: %s", query)
	log.Printf("[INSERT] Args: %v", args)
	log.Printf("[INSERT] Data: %+v\n", msg.Decoded)
	return nil
}

// HandleUpdate processes UPDATE messages
func (h *MySQLHandler) HandleUpdate(msg *format.Update) error {
	// Simple assumption: first column is the primary key.
	// IMPORTANT: For production code, determine the primary key by checking
	// the 'ColumnFlagKey' flag in the relation columns.
	var pkColumn string
	var pkValue interface{}

	// Find the first non-nil value in old data as PK
	for k, v := range msg.OldDecoded {
		if v != nil {
			pkColumn = k
			pkValue = v
			break
		}
	}

	if pkColumn == "" {
		log.Printf("[UPDATE] Warning: no primary key found for table %s", msg.TableName)
		return nil
	}

	query, args := sql.BuildUpdateQuery(msg.TableName, msg.NewDecoded, pkColumn, pkValue)
	log.Printf("[UPDATE] Table: %s", msg.TableName)
	log.Printf("[UPDATE] Query: %s", query)
	log.Printf("[UPDATE] Args: %v", args)
	log.Printf("[UPDATE] Old Data: %+v", msg.OldDecoded)
	log.Printf("[UPDATE] New Data: %+v\n", msg.NewDecoded)
	return nil
}

// HandleDelete processes DELETE messages
func (h *MySQLHandler) HandleDelete(msg *format.Delete) error {
	// Simple assumption: first column is the primary key.
	var pkColumn string
	var pkValue interface{}

	// Find the first non-nil value as PK
	for k, v := range msg.OldDecoded {
		if v != nil {
			pkColumn = k
			pkValue = v
			break
		}
	}

	if pkColumn == "" {
		log.Printf("[DELETE] Warning: no primary key found for table %s", msg.TableName)
		return nil
	}

	query, args := sql.BuildDeleteQuery(msg.TableName, pkColumn, pkValue)
	log.Printf("[DELETE] Table: %s", msg.TableName)
	log.Printf("[DELETE] Query: %s", query)
	log.Printf("[DELETE] Args: %v", args)
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
