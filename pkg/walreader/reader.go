package walreader

import (
	"context"
	"errors"
	"fmt"
	"log"
	"regexp"
	"strings"
	"sync"
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
	// Message is the decoded logical replication message.
	Message any
	// Ack records this message as durably processed. Checkpoints advance only
	// through contiguous acknowledged messages. A Commit acknowledgement covers
	// every delivered message in that transaction, so consumers can persist a
	// large transaction atomically and acknowledge its Commit once. Callers must
	// retry ErrAckWindowFull after an earlier acknowledgement advances.
	Ack func() error
}

type ListenerFunc func(ctx *ListenerContext)

const defaultDeliveryWindow uint64 = 2048

// ErrAckWindowFull indicates that an out-of-order acknowledgement cannot be
// retained until the missing prefix is acknowledged.
var ErrAckWindowFull = errors.New("WAL acknowledgement window is full")

type standbyStatusSender func(ctx context.Context, status pglogrepl.StandbyStatusUpdate) error

type queuedMessage struct {
	message          *message.Message
	sequence         uint64
	ackStartSequence uint64
}

type acknowledgedRange struct {
	end uint64
	lsn pglogrepl.LSN
}

// Reader handles PostgreSQL WAL replication
type Reader struct {
	config                   *Config
	conn                     *pgconn.PgConn
	relations                map[uint32]*format.Relation
	appliedLSN               atomic.Uint64 // latest LSN durably saved by ListenerContext.Ack.
	ackMu                    sync.Mutex
	acknowledgedRanges       map[uint64]acknowledgedRange
	nextMessageSequence      uint64 // owned by the replication loop
	transactionStartSequence uint64 // owned by the replication loop
	nextPersistSequence      uint64 // protected by ackMu
	ackWindow                uint64
	ackProgress              chan struct{}
	stateStore               state.IStateStore
	listenerFunc             ListenerFunc
	messageCH                chan *queuedMessage
	statusUpdateRequested    chan struct{}
	sendStandbyStatus        standbyStatusSender
}

// NewReader creates a new WAL reader
func NewReader(config *Config, listenerFunc ListenerFunc) *Reader {
	// Use provided IStateStore or default to file store
	stateStore := config.StateStore
	if stateStore == nil {
		stateStore = state.NewFileStore()
	}

	ackWindow := config.AckWindow
	if ackWindow == 0 {
		ackWindow = defaultDeliveryWindow
	}

	reader := &Reader{
		config:                config,
		listenerFunc:          listenerFunc,
		messageCH:             make(chan *queuedMessage, defaultDeliveryWindow),
		relations:             make(map[uint32]*format.Relation),
		acknowledgedRanges:    make(map[uint64]acknowledgedRange),
		nextPersistSequence:   1,
		ackWindow:             ackWindow,
		ackProgress:           make(chan struct{}, 1),
		stateStore:            stateStore,
		statusUpdateRequested: make(chan struct{}, 1),
	}
	reader.sendStandbyStatus = func(ctx context.Context, status pglogrepl.StandbyStatusUpdate) error {
		return pglogrepl.SendStandbyStatusUpdate(ctx, reader.conn, status)
	}
	return reader
}

// Connect establishes connection to PostgreSQL
func (r *Reader) Connect(ctx context.Context) error {
	conn, err := pgconn.Connect(ctx, r.config.ConnString)
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}
	r.conn = conn

	if err := r.applyLogicalDecodingWorkMem(ctx); err != nil {
		_ = r.conn.Close(ctx)
		r.conn = nil
		return err
	}

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
	r.appliedLSN.Store(uint64(lastLSN))

	return nil
}

// logicalDecodingWorkMemPattern matches Postgres memory-GUC syntax, e.g.
// "256MB", "65536kB", "1GB", "1TB", "65536B", or a bare integer (kB). SET is
// not parameterizable over the simple query protocol used on replication
// connections, so the value is validated before interpolation.
var logicalDecodingWorkMemPattern = regexp.MustCompile(`(?i)^[0-9]+\s*(k?b|mb|gb|tb)?$`)

// applyLogicalDecodingWorkMem sets logical_decoding_work_mem on this session
// only (no ALTER SYSTEM / reload / superuser required). It must run before
// ensureReplicationSlot/StartReplication switches the connection into
// replication mode, since regular SQL is no longer accepted afterwards.
func (r *Reader) applyLogicalDecodingWorkMem(ctx context.Context) error {
	value := strings.TrimSpace(r.config.LogicalDecodingWorkMem)
	if value == "" {
		return nil
	}
	if !logicalDecodingWorkMemPattern.MatchString(value) {
		return fmt.Errorf("invalid LogicalDecodingWorkMem %q: expected e.g. \"256MB\"", value)
	}

	query := fmt.Sprintf("SET logical_decoding_work_mem = '%s'", value)
	result := r.conn.Exec(ctx, query)
	if _, err := result.ReadAll(); err != nil {
		return fmt.Errorf("failed to set logical_decoding_work_mem to %q: %w", value, err)
	}
	log.Printf("Set logical_decoding_work_mem = '%s' for this replication session", value)
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
	startLSN := pglogrepl.LSN(r.appliedLSN.Load())
	err = pglogrepl.StartReplication(ctx, r.conn, r.config.SlotName, startLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: pluginArgs,
	})
	if err != nil {
		return fmt.Errorf("cannot start replication: %w", err)
	}

	log.Printf("Replication started on slot '%s' from LSN %s", r.config.SlotName, startLSN)
	return nil
}

// Run starts the main replication loop
func (r *Reader) Run(ctx context.Context) error {
	if r.conn == nil {
		return fmt.Errorf("not connected - call Connect() first")
	}

	processCtx, cancelProcess := context.WithCancel(ctx)
	defer cancelProcess()
	go r.process(processCtx)

	for {
		select {
		case <-ctx.Done():
			log.Println("Replication stopped by context")
			return ctx.Err()
		default:
		}

		// Run is the only goroutine allowed to use r.conn after replication starts.
		if err := r.sendRequestedStandbyStatusUpdate(ctx); err != nil {
			return fmt.Errorf("send requested standby status update: %w", err)
		}

		// Receive message from PostgreSQL
		msgCtx, cancel := r.newReceiveMessageContext(ctx)
		rawMsg, err := r.conn.ReceiveMessage(msgCtx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if pgconn.Timeout(err) {
				r.discardRequestedStandbyStatusUpdate()
				err = r.sendStandbyStatusUpdate(ctx)
				if err != nil {
					return fmt.Errorf("send stand by status update %v", err)
				}
				log.Print("[WalReader] send stand by status update")
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
		keepalive, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
		if err != nil {
			return fmt.Errorf("failed to parse primary keepalive message: %w", err)
		}
		if keepalive.ReplyRequested {
			return r.sendStandbyStatusUpdate(ctx)
		}
		return nil
	case pglogrepl.XLogDataByteID:
		return r.handleXLogData(ctx, msg.Data[1:])
	default:
		log.Printf("Unknown message type: %c", msg.Data[0])
		return nil
	}
}

// handleXLogData processes WAL data messages
func (r *Reader) handleXLogData(ctx context.Context, data []byte) error {
	xld, err := pglogrepl.ParseXLogData(data)
	if err != nil {
		return fmt.Errorf("failed to parse XLogData: %w", err)
	}

	// Process the logical message using pkg/message
	err = r.handleLogicalMessage(ctx, xld.WALData, time.Now(), xld.WALStart)
	if err != nil {
		return fmt.Errorf("error processing logical message: %w", err)
	}
	return nil
}

// handleLogicalMessage processes logical replication messages
func (r *Reader) handleLogicalMessage(ctx context.Context, data []byte, serverTime time.Time, walStart pglogrepl.LSN) error {
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

	r.nextMessageSequence++
	ackStartSequence := r.nextMessageSequence
	switch decodedMsg.(type) {
	case *format.Begin:
		r.transactionStartSequence = r.nextMessageSequence
	case *format.Commit:
		if r.transactionStartSequence != 0 {
			ackStartSequence = r.transactionStartSequence
			r.transactionStartSequence = 0
		}
	}

	return r.enqueueMessage(ctx, &queuedMessage{
		message: &message.Message{
			Message:  decodedMsg,
			WalStart: walStart,
		},
		sequence:         r.nextMessageSequence,
		ackStartSequence: ackStartSequence,
	})
}

func (r *Reader) sendStandbyStatusUpdate(ctx context.Context) error {
	lsn := pglogrepl.LSN(r.appliedLSN.Load())
	return r.sendStandbyStatus(ctx, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: lsn,
		WALFlushPosition: lsn,
		WALApplyPosition: lsn,
	})
}

func (r *Reader) standbyMessageTimeout() time.Duration {
	if r.config != nil && r.config.StandbyMessageTimeout > 0 {
		return r.config.StandbyMessageTimeout
	}
	return 10 * time.Second
}

func (r *Reader) newReceiveMessageContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, r.standbyMessageTimeout())
}

func (r *Reader) requestStandbyStatusUpdate() {
	select {
	case r.statusUpdateRequested <- struct{}{}:
	default:
	}
}

func (r *Reader) discardRequestedStandbyStatusUpdate() {
	select {
	case <-r.statusUpdateRequested:
	default:
	}
}

func (r *Reader) sendRequestedStandbyStatusUpdate(ctx context.Context) error {
	select {
	case <-r.statusUpdateRequested:
		return r.sendStandbyStatusUpdate(ctx)
	default:
		return nil
	}
}

func (r *Reader) enqueueMessage(ctx context.Context, msg *queuedMessage) error {
	select {
	case r.messageCH <- msg:
		return nil
	default:
	}

	ticker := time.NewTicker(r.standbyMessageTimeout())
	defer ticker.Stop()

	for {
		select {
		case r.messageCH <- msg:
			return nil
		case <-r.statusUpdateRequested:
			if err := r.sendStandbyStatusUpdate(ctx); err != nil {
				return err
			}
		case <-ticker.C:
			if err := r.sendStandbyStatusUpdate(ctx); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (r *Reader) acknowledge(startSequence, endSequence uint64, lsn pglogrepl.LSN) error {
	r.ackMu.Lock()
	defer r.ackMu.Unlock()

	if endSequence < r.nextPersistSequence {
		r.requestStandbyStatusUpdate()
		return nil
	}

	if startSequence < r.nextPersistSequence {
		startSequence = r.nextPersistSequence
	}

	acknowledgement, acknowledged := r.acknowledgedRanges[startSequence]
	if !acknowledged && startSequence > r.nextPersistSequence && uint64(len(r.acknowledgedRanges)) >= r.ackWindow {
		return ErrAckWindowFull
	}
	if !acknowledged || acknowledgement.end < endSequence {
		r.acknowledgedRanges[startSequence] = acknowledgedRange{end: endSequence, lsn: lsn}
	}

	nextSequence := r.nextPersistSequence
	persistedSequence := uint64(0)
	persistedLSN := pglogrepl.LSN(0)
	readyToPersist := false
	for {
		acknowledgement, acknowledged = r.acknowledgedRanges[nextSequence]
		if !acknowledged {
			break
		}
		readyToPersist = true
		persistedSequence = acknowledgement.end
		persistedLSN = acknowledgement.lsn
		if persistedSequence == ^uint64(0) {
			break
		}
		nextSequence = persistedSequence + 1
	}
	if !readyToPersist {
		r.requestStandbyStatusUpdate()
		return nil
	}

	if persistedLSN > pglogrepl.LSN(r.appliedLSN.Load()) {
		if err := r.stateStore.SaveLSN(context.Background(), r.config.LSNStateKey, persistedLSN); err != nil {
			return err
		}
		r.appliedLSN.Store(uint64(persistedLSN))
	}

	for startSequence := range r.acknowledgedRanges {
		if startSequence <= persistedSequence {
			delete(r.acknowledgedRanges, startSequence)
		}
	}
	r.nextPersistSequence = persistedSequence + 1
	r.signalAckProgress()
	r.requestStandbyStatusUpdate()
	return nil
}

func (r *Reader) deliveryWindowFull(sequence uint64) bool {
	if sequence < r.nextPersistSequence {
		return false
	}
	return sequence-r.nextPersistSequence >= r.ackWindow
}

func (r *Reader) signalAckProgress() {
	select {
	case r.ackProgress <- struct{}{}:
	default:
	}
}

func (r *Reader) waitForDeliveryWindow(ctx context.Context, sequence uint64) error {
	for {
		r.ackMu.Lock()
		full := r.deliveryWindowFull(sequence)
		r.ackMu.Unlock()
		if !full {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.ackProgress:
		}
	}
}

func checkpointLSN(msg *message.Message) pglogrepl.LSN {
	if commit, ok := msg.Message.(*format.Commit); ok && commit.EndLSN != 0 {
		return pglogrepl.LSN(commit.EndLSN)
	}
	return msg.WalStart
}

func (r *Reader) process(ctx context.Context) {
	logger.Info("postgres message process started")

	inTransaction := false
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-r.messageCH:
			if !ok {
				return
			}
			// Once a transaction begins, deliver through its Commit even if it
			// exceeds AckWindow; otherwise a consumer that checkpoints at Commit
			// could never receive the message that releases the window.
			if !inTransaction {
				if err := r.waitForDeliveryWindow(ctx, msg.sequence); err != nil {
					return
				}
			}

			lCtx := &ListenerContext{
				Message: msg.message.Message,
				Ack: func() error {
					return r.acknowledge(msg.ackStartSequence, msg.sequence, checkpointLSN(msg.message))
				},
			}
			r.listenerFunc(lCtx)

			switch msg.message.Message.(type) {
			case *format.Begin:
				inTransaction = true
			case *format.Commit:
				inTransaction = false
			}
		}
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
