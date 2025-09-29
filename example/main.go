package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/hoanguyenkh/go-pg-wal/pkg/message"
	"github.com/hoanguyenkh/go-pg-wal/pkg/message/format"
	"github.com/hoanguyenkh/go-pg-wal/pkg/sql"
	"github.com/hoanguyenkh/go-pg-wal/pkg/state"
)

const (
	PostgresConnStr     = "postgres://postgres:123456@localhost:5432/kyberdata?replication=database"
	ReplicationSlotName = "my_replication_slot"
	PublicationName     = "my_publication"
	LsnStateFile        = "wal_sync_state.lsn"
)

func main() {
	conn, err := pgconn.Connect(context.Background(), PostgresConnStr)
	if err != nil {
		log.Fatalf("failed to connect to PostgreSQL: %v", err)
	}
	defer conn.Close(context.Background())
	lastLSN, err := state.LoadLSNState(LsnStateFile)
	if err != nil {
		lastLSN = 0
	}

	pluginArgs := []string{"proto_version '1'", fmt.Sprintf("publication_names '%s'", PublicationName)}
	err = pglogrepl.StartReplication(context.Background(), conn, ReplicationSlotName, lastLSN, pglogrepl.StartReplicationOptions{PluginArgs: pluginArgs})
	if err != nil {
		log.Fatalf("Cannot start replication: %v", err)
	}
	log.Printf("Replication started on slot '%s'", ReplicationSlotName)

	// Handle shutdown signals to exit gracefully
	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Println("Received stop signal, shutting down...")
		cancel()
	}()

	// Main loop to receive and process messages
	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

	relations := make(map[uint32]*format.Relation)

	for ctx.Err() == nil {
		// Send standby status to keep connection alive and report current LSN
		if time.Now().After(nextStandbyMessageDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(context.Background(), conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: lastLSN})
			if err != nil {
				log.Fatalf("Failed to send standby status update: %v", err)
			}
			log.Printf("Sent standby status, LSN: %s", lastLSN)
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		// Receive message from PostgreSQL with timeout
		rawMsg, err := conn.ReceiveMessage(context.Background())
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			log.Fatalf("Error receiving message: %v", err)
		}

		// Handle PostgreSQL error response
		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			log.Fatalf("Error from PostgreSQL: %s", errMsg.Message)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			log.Printf("Unexpected message type: %T", rawMsg)
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			// PostgreSQL sends keepalive to check connection
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				log.Fatalf("Failed to parse PrimaryKeepaliveMessage: %s", err)
			}
			if pkm.ReplyRequested {
				nextStandbyMessageDeadline = time.Time{} // Send immediately
			}

		case pglogrepl.XLogDataByteID:
			// This message contains WAL (Write-Ahead Log) change data
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				log.Fatalf("Failed to parse XLogData: %s", err)
			}

			// Process the logical message using pkg/message
			err = handleLogicalMessage(xld.WALData, relations, time.Now())
			if err != nil {
				log.Fatalf("Error processing logical message: %v", err)
			}

			// Update and save the latest LSN
			lastLSN = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
			err = state.SaveLSNState(LsnStateFile, lastLSN)
			if err != nil {
				log.Fatalf("CRITICAL: Cannot save LSN state: %v", err)
			}
		}
	}

	log.Println("Application stopped.")
}

// handleLogicalMessage processes replication messages using pkg/message
func handleLogicalMessage(data []byte, relations map[uint32]*format.Relation, serverTime time.Time) error {
	msg, err := message.New(data, serverTime, relations)
	if err != nil {
		// Ignore unsupported messages
		if err.Error() == "message byte not supported" {
			log.Printf("Unsupported message type: %c", data[0])
			return nil
		}
		return nil
	}

	if msg == nil {
		// Some messages return nil (like stream control messages)
		return nil
	}

	switch m := msg.(type) {
	case *format.Relation:
		log.Printf("Received schema for table: %s.%s", m.Namespace, m.Name)

	case *format.Insert:
		query, args := sql.BuildInsertQuery(m.TableName, m.Decoded)
		log.Printf("[INSERT] Table: %s", m.TableName)
		log.Printf("[INSERT] Query: %s", query)
		log.Printf("[INSERT] Args: %v", args)
		log.Printf("[INSERT] Data: %+v\n", m.Decoded)

	case *format.Update:
		// Simple assumption: first column is the primary key.
		// IMPORTANT: For production code, determine the primary key by checking
		// the 'ColumnFlagKey' flag in the relation columns.
		var pkColumn string
		var pkValue interface{}

		// Find the first non-nil value in old data as PK
		for k, v := range m.OldDecoded {
			if v != nil {
				pkColumn = k
				pkValue = v
				break
			}
		}

		if pkColumn == "" {
			return fmt.Errorf("no primary key found for update")
		}

		query, args := sql.BuildUpdateQuery(m.TableName, m.NewDecoded, pkColumn, pkValue)
		log.Printf("[UPDATE] Table: %s", m.TableName)
		log.Printf("[UPDATE] Query: %s", query)
		log.Printf("[UPDATE] Args: %v", args)
		log.Printf("[UPDATE] Old Data: %+v", m.OldDecoded)
		log.Printf("[UPDATE] New Data: %+v\n", m.NewDecoded)

	case *format.Delete:
		// Simple assumption: first column is the primary key.
		var pkColumn string
		var pkValue interface{}

		// Find the first non-nil value as PK
		for k, v := range m.OldDecoded {
			if v != nil {
				pkColumn = k
				pkValue = v
				break
			}
		}

		if pkColumn == "" {
			return fmt.Errorf("no primary key found for delete")
		}

		query, args := sql.BuildDeleteQuery(m.TableName, pkColumn, pkValue)
		log.Printf("[DELETE] Table: %s", m.TableName)
		log.Printf("[DELETE] Query: %s", query)
		log.Printf("[DELETE] Args: %v", args)
		log.Printf("[DELETE] Data: %+v\n", m.OldDecoded)

	default:
		log.Printf("Unhandled message type: %T", m)
	}
	return nil
}
