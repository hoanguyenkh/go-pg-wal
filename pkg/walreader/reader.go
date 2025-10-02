package walreader

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"github.com/KyberNetwork/logger"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/hoanguyenkh/go-pg-wal/pkg/message"
	"github.com/hoanguyenkh/go-pg-wal/pkg/message/format"
	"github.com/hoanguyenkh/go-pg-wal/pkg/state"
)

type ListenerContext struct {
	Message any
	Ack     func() error
}

type ListenerFunc func(ctx *ListenerContext)

// Reader handles PostgreSQL WAL replication
type Reader struct {
	config       *Config
	conn         *pgconn.PgConn
	relations    map[uint32]*format.Relation
	lastLSN      pglogrepl.LSN
	stateStore   state.IStateStore
	listenerFunc ListenerFunc
	messageCH    chan *message.Message
	lastSaveDb   atomic.Int64
}

// NewReader creates a new WAL reader
func NewReader(config *Config, listenerFunc ListenerFunc) *Reader {
	// Use provided IStateStore or default to file store
	stateStore := config.StateStore
	if stateStore == nil {
		stateStore = state.NewFileStore()
	}

	return &Reader{
		config:       config,
		listenerFunc: listenerFunc,
		messageCH:    make(chan *message.Message, 2048),
		relations:    make(map[uint32]*format.Relation),
		stateStore:   stateStore,
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
		// Start from current LSN if no state found (first run)
		currentLSN, err := r.getCurrentLSN(ctx)
		if err != nil {
			log.Printf("Failed to get current LSN, starting from beginning: %v", err)
			lastLSN = 0
		} else {
			lastLSN = currentLSN
			log.Printf("No previous LSN state found, starting from current LSN: %s", lastLSN)
		}
	} else {
		log.Printf("Loaded previous LSN state: %s", lastLSN)
	}
	r.lastLSN = lastLSN
	r.lastSaveDb.Store(int64(lastLSN))

	return nil
}

// getCurrentLSN gets the current WAL LSN from PostgreSQL
func (r *Reader) getCurrentLSN(ctx context.Context) (pglogrepl.LSN, error) {
	// Query the current WAL LSN position
	query := "SELECT pg_current_wal_lsn()"
	result := r.conn.Exec(ctx, query)

	results, err := result.ReadAll()
	if err != nil {
		return 0, fmt.Errorf("failed to get current WAL LSN: %w", err)
	}

	if len(results) == 0 || len(results[0].Rows) == 0 {
		return 0, fmt.Errorf("no result returned from pg_current_wal_lsn()")
	}

	lsnStr := string(results[0].Rows[0][0])
	lsn, err := pglogrepl.ParseLSN(lsnStr)
	if err != nil {
		return 0, fmt.Errorf("failed to parse LSN '%s': %w", lsnStr, err)
	}

	return lsn, nil
}

// ensureReplicationSlot checks if the replication slot exists and creates it if it doesn't
func (r *Reader) ensureReplicationSlot(ctx context.Context) error {
	// Check if slot exists by querying pg_replication_slots
	query := fmt.Sprintf("SELECT 1 FROM pg_replication_slots WHERE slot_name = '%s'", r.config.SlotName)
	result := r.conn.Exec(ctx, query)

	// Read the result
	results, err := result.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to check if replication slot exists: %w", err)
	}

	// If we have results, the slot exists
	if len(results) > 0 && len(results[0].Rows) > 0 {
		log.Printf("Replication slot '%s' already exists", r.config.SlotName)
		return nil
	}

	// Slot doesn't exist, create it
	log.Printf("Creating replication slot '%s'", r.config.SlotName)

	// Determine output plugin (default to pgoutput for logical replication)
	outputPlugin := "pgoutput"
	if r.config.OutputPlugin != "" {
		outputPlugin = r.config.OutputPlugin
	}

	_, err = pglogrepl.CreateReplicationSlot(ctx, r.conn, r.config.SlotName, outputPlugin, pglogrepl.CreateReplicationSlotOptions{
		Temporary: false,
		Mode:      pglogrepl.LogicalReplication,
	})
	if err != nil {
		// Check if error is because slot already exists (race condition)
		if strings.Contains(err.Error(), "already exists") {
			log.Printf("Replication slot '%s' was created by another process", r.config.SlotName)
			return nil
		}
		return fmt.Errorf("failed to create replication slot '%s': %w", r.config.SlotName, err)
	}

	log.Printf("Successfully created replication slot '%s'", r.config.SlotName)
	return nil
}

// ensurePublication checks if the publication exists and creates it if it doesn't
func (r *Reader) ensurePublication(ctx context.Context) error {
	// Check if publication exists by querying pg_publication
	query := fmt.Sprintf("SELECT 1 FROM pg_publication WHERE pubname = '%s'", r.config.PublicationName)
	result := r.conn.Exec(ctx, query)

	// Read the result
	results, err := result.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to check if publication exists: %w", err)
	}

	// If we have results, the publication exists
	if len(results) > 0 && len(results[0].Rows) > 0 {
		log.Printf("Publication '%s' already exists", r.config.PublicationName)

		// Check if we need to fix publication ownership
		err = r.fixPublicationOwnership(ctx)
		if err != nil {
			log.Printf("Warning: Could not fix publication ownership: %v", err)
		}

		return nil
	}

	// Publication doesn't exist, create it
	log.Printf("Creating publication '%s'", r.config.PublicationName)

	// Build CREATE PUBLICATION statement
	var createQuery string
	if len(r.config.MapTableName) == 0 || (len(r.config.MapTableName) == 1 && r.config.MapTableName[""]) {
		// Create publication for all tables
		createQuery = fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES", r.config.PublicationName)
	} else {
		// Create publication for specific tables
		var tables []string
		for tableName := range r.config.MapTableName {
			if tableName != "" { // Skip empty table names
				// Add schema prefix if specified
				if r.config.Schema != "" {
					tables = append(tables, fmt.Sprintf("%s.%s", r.config.Schema, tableName))
				} else {
					tables = append(tables, tableName)
				}
			}
		}

		if len(tables) == 0 {
			// Fallback to all tables if no valid tables specified
			createQuery = fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES", r.config.PublicationName)
		} else {
			tableList := strings.Join(tables, ", ")
			createQuery = fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", r.config.PublicationName, tableList)
		}
	}

	result = r.conn.Exec(ctx, createQuery)
	_, err = result.ReadAll()
	if err != nil {
		// Check if error is because publication already exists (race condition)
		if strings.Contains(err.Error(), "already exists") {
			log.Printf("Publication '%s' was created by another process", r.config.PublicationName)
			return nil
		}
		return fmt.Errorf("failed to create publication '%s': %w", r.config.PublicationName, err)
	}

	log.Printf("Successfully created publication '%s'", r.config.PublicationName)
	return nil
}

// fixPublicationOwnership ensures the publication is owned by the current user
func (r *Reader) fixPublicationOwnership(ctx context.Context) error {
	// Get current user info
	query := "SELECT current_user, usesysid FROM pg_user WHERE usename = current_user"
	result := r.conn.Exec(ctx, query)

	results, err := result.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to get current user info: %w", err)
	}

	if len(results) == 0 || len(results[0].Rows) == 0 {
		return fmt.Errorf("could not determine current user")
	}

	currentUser := string(results[0].Rows[0][0])
	currentUserID := string(results[0].Rows[0][1])

	// Check publication ownership
	query = fmt.Sprintf("SELECT pubowner FROM pg_publication WHERE pubname = '%s'", r.config.PublicationName)
	result = r.conn.Exec(ctx, query)

	results, err = result.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to check publication ownership: %w", err)
	}

	if len(results) > 0 && len(results[0].Rows) > 0 {
		pubOwnerID := string(results[0].Rows[0][0])

		if pubOwnerID != currentUserID {
			// Try to change ownership
			alterQuery := fmt.Sprintf("ALTER PUBLICATION %s OWNER TO %s", r.config.PublicationName, currentUser)
			result = r.conn.Exec(ctx, alterQuery)
			_, err = result.ReadAll()
			if err != nil {
				// If we can't change ownership, try to grant permissions
				grantQuery := fmt.Sprintf("GRANT USAGE ON PUBLICATION %s TO %s", r.config.PublicationName, currentUser)
				result = r.conn.Exec(ctx, grantQuery)
				_, err = result.ReadAll()
				if err != nil {
					return fmt.Errorf("failed to grant publication permissions: %w", err)
				}
			}
		}
	}

	return nil
}

// verifyPublicationBeforeReplication does a final check that publication is visible to replication
func (r *Reader) verifyPublicationBeforeReplication(ctx context.Context) error {
	// Check publication exists and get ownership details
	query := fmt.Sprintf(`
		SELECT pubname, pubowner, puballtables
		FROM pg_publication 
		WHERE pubname = '%s'`, r.config.PublicationName)
	result := r.conn.Exec(ctx, query)

	results, err := result.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to verify publication: %w", err)
	}

	if len(results) == 0 || len(results[0].Rows) == 0 {
		return fmt.Errorf("publication '%s' not found during verification", r.config.PublicationName)
	}

	// Check if publication has tables (if not FOR ALL TABLES)
	if len(results) > 0 && len(results[0].Rows) > 0 && len(results[0].Rows[0]) >= 3 {
		allTables := string(results[0].Rows[0][2])
		if allTables == "f" {
			// Check if publication has any tables
			query = fmt.Sprintf(`
				SELECT COUNT(*) 
				FROM pg_publication_tables 
				WHERE pubname = '%s'`, r.config.PublicationName)

			result = r.conn.Exec(ctx, query)
			results, err = result.ReadAll()
			if err == nil && len(results) > 0 && len(results[0].Rows) > 0 {
				count := string(results[0].Rows[0][0])
				if count == "0" {
					return fmt.Errorf("publication '%s' exists but contains no tables", r.config.PublicationName)
				}
			}
		}
	}

	return nil
}

// StartReplication begins the replication process
func (r *Reader) StartReplication(ctx context.Context) error {
	if r.conn == nil {
		return fmt.Errorf("not connected - call Connect() first")
	}

	// Ensure replication slot exists
	err := r.ensureReplicationSlot(ctx)
	if err != nil {
		return fmt.Errorf("failed to ensure replication slot: %w", err)
	}

	// Ensure publication exists
	err = r.ensurePublication(ctx)
	if err != nil {
		return fmt.Errorf("failed to ensure publication: %w", err)
	}

	// Double-check publication exists right before starting replication
	err = r.verifyPublicationBeforeReplication(ctx)
	if err != nil {
		log.Printf("Publication verification failed: %v", err)
		return fmt.Errorf("publication verification failed: %w", err)
	}

	// Prepare plugin arguments
	pluginArgs := append(r.config.PluginArgs, fmt.Sprintf("publication_names '%s'", r.config.PublicationName))
	err = pglogrepl.StartReplication(ctx, r.conn, r.config.SlotName, r.lastLSN, pglogrepl.StartReplicationOptions{
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

	go r.process(ctx)

	for {
		select {
		case <-ctx.Done():
			log.Println("Replication stopped by context")
			return ctx.Err()
		default:
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
			log.Printf("PostgreSQL error during replication: %s (Code: %s)", errMsg.Message, errMsg.Code)
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
		return nil
	case pglogrepl.XLogDataByteID:
		return r.handleXLogData(msg.Data[1:])
	default:
		log.Printf("Unknown message type: %c", msg.Data[0])
		return nil
	}
}

// handleXLogData processes WAL data messages
func (r *Reader) handleXLogData(data []byte) error {
	xld, err := pglogrepl.ParseXLogData(data)
	if err != nil {
		return fmt.Errorf("failed to parse XLogData: %w", err)
	}

	// Process the logical message using pkg/message
	err = r.handleLogicalMessage(xld.WALData, time.Now(), xld.WALStart)
	if err != nil {
		return fmt.Errorf("error processing logical message: %w", err)
	}
	return nil
}

// handleLogicalMessage processes logical replication messages
func (r *Reader) handleLogicalMessage(data []byte, serverTime time.Time, walStart pglogrepl.LSN) error {
	if walStart == 0 {
		log.Printf("DEBUG: Message with WALStart=0, type=%c (0x%02x)", data[0], data[0])
	}
	decodedMsg, err := message.New(data, serverTime, r.relations)
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
	if decodedMsg == nil {
		// Some messages return nil (like stream control messages)
		return nil
	}
	// Skip Relation messages - they are metadata only and already stored in r.relations
	if _, isRelation := decodedMsg.(*format.Relation); isRelation {
		log.Printf("DEBUG: Skipping Relation message (metadata only)")
		return nil
	}

	r.lastLSN = walStart

	r.messageCH <- &message.Message{
		Message:  decodedMsg,
		WalStart: walStart,
	}
	return nil
}

func (r *Reader) process(ctx context.Context) {
	logger.Info("postgres message process started")

	for {
		msg, ok := <-r.messageCH
		if !ok {
			break
		}

		lCtx := &ListenerContext{
			Message: msg.Message,
			Ack: func() error {
				if r.lastSaveDb.Load() < int64(msg.WalStart) {
					err := r.stateStore.SaveLSN(context.Background(), r.config.LSNStateKey, msg.WalStart)
					if err != nil {
						return err
					}
					r.lastSaveDb.Store(int64(msg.WalStart))
					return pglogrepl.SendStandbyStatusUpdate(ctx, r.conn, pglogrepl.StandbyStatusUpdate{
						WALWritePosition: msg.WalStart,
						WALFlushPosition: msg.WalStart,
					})
				}
				return nil
			},
		}
		r.listenerFunc(lCtx)
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
