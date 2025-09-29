package walreader

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/hoanguyenkh/go-pg-wal/pkg/utils"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/hoanguyenkh/go-pg-wal/pkg/message"
	"github.com/hoanguyenkh/go-pg-wal/pkg/message/format"
	"github.com/hoanguyenkh/go-pg-wal/pkg/state"
)

// Reader handles PostgreSQL WAL replication
type Reader struct {
	config     *Config
	handler    MessageHandler
	conn       *pgconn.PgConn
	relations  map[uint32]*format.Relation
	lastLSN    pglogrepl.LSN
	stateStore state.IStateStore
}

// NewReader creates a new WAL reader
func NewReader(config *Config, handler MessageHandler) *Reader {
	// Use provided IStateStore or default to file store
	stateStore := config.StateStore
	if stateStore == nil {
		stateStore = state.NewFileStore()
	}

	return &Reader{
		config:     config,
		handler:    handler,
		relations:  make(map[uint32]*format.Relation),
		stateStore: stateStore,
	}
}

// Connect establishes connection to PostgreSQL
func (r *Reader) Connect(ctx context.Context) error {
	conn, err := pgconn.Connect(ctx, r.config.ConnString)
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}
	r.conn = conn

	// Load last LSN from state store
	lastLSN, err := r.stateStore.LoadLSN(ctx, r.config.LSNStateKey)
	if err != nil {
		lastLSN = 0 // Start from beginning if no state found
		log.Printf("No previous LSN state found, starting from beginning: %v", err)
	}
	r.lastLSN = lastLSN

	return nil
}

// StartReplication begins the replication process
func (r *Reader) StartReplication(ctx context.Context) error {
	if r.conn == nil {
		return fmt.Errorf("not connected - call Connect() first")
	}

	// Prepare plugin arguments
	pluginArgs := append(r.config.PluginArgs, fmt.Sprintf("publication_names '%s'", r.config.PublicationName))

	err := pglogrepl.StartReplication(ctx, r.conn, r.config.SlotName, r.lastLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: pluginArgs,
	})
	if err != nil {
		return fmt.Errorf("cannot start replication: %w", err)
	}

	log.Printf("Replication started on slot '%s' from LSN %s", r.config.SlotName, r.lastLSN)
	return nil
}

// Run starts the main replication loop
func (r *Reader) Run(ctx context.Context) error {
	if r.conn == nil {
		return fmt.Errorf("not connected - call Connect() first")
	}

	nextStandbyMessageDeadline := time.Now().Add(r.config.StandbyMessageTimeout)

	for {
		select {
		case <-ctx.Done():
			log.Println("Replication stopped by context")
			return ctx.Err()
		default:
		}

		// Send standby status to keep connection alive
		if time.Now().After(nextStandbyMessageDeadline) {
			err := pglogrepl.SendStandbyStatusUpdate(ctx, r.conn, pglogrepl.StandbyStatusUpdate{
				WALWritePosition: r.lastLSN,
			})
			if err != nil {
				return fmt.Errorf("failed to send standby status update: %w", err)
			}
			log.Printf("Sent standby status, LSN: %s", r.lastLSN)
			nextStandbyMessageDeadline = time.Now().Add(r.config.StandbyMessageTimeout)
		}

		// Receive message from PostgreSQL
		rawMsg, err := r.conn.ReceiveMessage(ctx)
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			return fmt.Errorf("error receiving message: %w", err)
		}

		// Handle PostgreSQL error response
		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			return fmt.Errorf("error from PostgreSQL: %s", errMsg.Message)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			log.Printf("Unexpected message type: %T", rawMsg)
			continue
		}

		err = r.processMessage(ctx, msg)
		if err != nil {
			return fmt.Errorf("error processing message: %w", err)
		}
	}
}

// processMessage handles different types of replication messages
func (r *Reader) processMessage(ctx context.Context, msg *pgproto3.CopyData) error {
	switch msg.Data[0] {
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		return r.handleKeepalive(msg.Data[1:])

	case pglogrepl.XLogDataByteID:
		return r.handleXLogData(msg.Data[1:])

	default:
		log.Printf("Unknown message type: %c", msg.Data[0])
		return nil
	}
}

// handleKeepalive processes keepalive messages
func (r *Reader) handleKeepalive(data []byte) error {
	pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(data)
	if err != nil {
		return fmt.Errorf("failed to parse PrimaryKeepaliveMessage: %w", err)
	}

	if pkm.ReplyRequested {
		// Send immediate reply if requested
		err = pglogrepl.SendStandbyStatusUpdate(context.Background(), r.conn, pglogrepl.StandbyStatusUpdate{
			WALWritePosition: r.lastLSN,
		})
		if err != nil {
			return fmt.Errorf("failed to send requested standby status: %w", err)
		}
	}
	return nil
}

// handleXLogData processes WAL data messages
func (r *Reader) handleXLogData(data []byte) error {
	xld, err := pglogrepl.ParseXLogData(data)
	if err != nil {
		return fmt.Errorf("failed to parse XLogData: %w", err)
	}

	// Process the logical message using pkg/message
	err = r.handleLogicalMessage(xld.WALData, time.Now())
	if err != nil {
		return fmt.Errorf("error processing logical message: %w", err)
	}

	// Update and save the latest LSN
	r.lastLSN = xld.WALStart + pglogrepl.LSN(len(xld.WALData))

	err = r.stateStore.SaveLSN(context.Background(), r.config.LSNStateKey, r.lastLSN)
	if err != nil {
		return fmt.Errorf("CRITICAL: cannot save LSN state: %w", err)
	}
	return nil
}

// handleLogicalMessage processes logical replication messages
func (r *Reader) handleLogicalMessage(data []byte, serverTime time.Time) error {
	msg, err := message.New(data, serverTime, r.relations)
	if err != nil {
		// Ignore unsupported messages
		if err.Error() == "message byte not supported" {
			log.Printf("Unsupported message type: %c", data[0])
			return nil
		}
		// Don't fail on parse errors, just log them
		log.Printf("Warning: failed to parse message: %v", err)
		return nil
	}

	if msg == nil {
		// Some messages return nil (like stream control messages)
		return nil
	}

	// Handle different message types
	switch m := msg.(type) {
	case *format.Relation:
		if m.Name == utils.WalLsnState {
			return nil
		}
		return r.handler.HandleRelation(m)
	case *format.Insert:
		if m.TableName == utils.WalLsnState {
			return nil
		}
		return r.handler.HandleInsert(m)
	case *format.Update:
		if m.TableName == utils.WalLsnState {
			return nil
		}
		return r.handler.HandleUpdate(m)
	case *format.Delete:
		if m.TableName == utils.WalLsnState {
			return nil
		}
		return r.handler.HandleDelete(m)

	default:
		log.Printf("Unhandled message type: %T", m)
		return nil
	}
}

// Close closes the connection and state store
func (r *Reader) Close(ctx context.Context) error {
	var err error

	// Close PostgreSQL connection
	if r.conn != nil {
		if connErr := r.conn.Close(ctx); connErr != nil {
			err = connErr
		}
	}

	// Close state store
	if r.stateStore != nil {
		if storeErr := r.stateStore.Close(); storeErr != nil {
			if err != nil {
				return fmt.Errorf("multiple close errors - conn: %w, store: %v", err, storeErr)
			}
			err = storeErr
		}
	}

	return err
}

// GetLastLSN returns the last processed LSN
func (r *Reader) GetLastLSN() pglogrepl.LSN {
	return r.lastLSN
}
