package walreader

import (
	"context"
	"encoding/binary"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoanguyenkh/go-pg-wal/pkg/message/format"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testStateStore struct {
	saved   []pglogrepl.LSN
	saveErr error
}

func (s *testStateStore) SaveLSN(_ context.Context, _ string, lsn pglogrepl.LSN) error {
	if s.saveErr != nil {
		return s.saveErr
	}
	s.saved = append(s.saved, lsn)
	return nil
}

func (s *testStateStore) LoadLSN(context.Context, string) (pglogrepl.LSN, error) {
	return 0, errors.New("not implemented")
}

func (*testStateStore) Close() error {
	return nil
}

func TestReaderStandbyStatusUsesAppliedLSN(t *testing.T) {
	reader := NewReader(NewConfig("", "slot", "publication", "", ""), nil)
	reader.appliedLSN.Store(100)

	var sent pglogrepl.StandbyStatusUpdate
	reader.sendStandbyStatus = func(_ context.Context, status pglogrepl.StandbyStatusUpdate) error {
		sent = status
		return nil
	}

	require.NoError(t, reader.sendStandbyStatusUpdate(context.Background()))
	assert.Equal(t, pglogrepl.LSN(100), sent.WALWritePosition)
	assert.Equal(t, pglogrepl.LSN(100), sent.WALFlushPosition)
}

func TestReaderRepliesToRequestedPrimaryKeepalive(t *testing.T) {
	reader := NewReader(NewConfig("", "slot", "publication", "", ""), nil)
	reader.appliedLSN.Store(100)

	var sent pglogrepl.StandbyStatusUpdate
	reader.sendStandbyStatus = func(_ context.Context, status pglogrepl.StandbyStatusUpdate) error {
		sent = status
		return nil
	}

	data := make([]byte, 18)
	data[0] = pglogrepl.PrimaryKeepaliveMessageByteID
	binary.BigEndian.PutUint64(data[1:], 200)
	data[17] = 1 // ReplyRequested

	require.NoError(t, reader.processMessage(context.Background(), &pgproto3.CopyData{Data: data}))
	assert.Equal(t, pglogrepl.LSN(100), sent.WALFlushPosition)
}

func TestReaderAcknowledgeAdvancesAppliedLSNOnlyAfterCheckpointSave(t *testing.T) {
	stateStore := &testStateStore{}
	reader := NewReader(&Config{StateStore: stateStore, LSNStateKey: "slot"}, nil)
	reader.appliedLSN.Store(100)

	require.NoError(t, reader.acknowledge(1, 1, 120))
	assert.Equal(t, []pglogrepl.LSN{120}, stateStore.saved)
	assert.Equal(t, uint64(120), reader.appliedLSN.Load())

	stateStore.saveErr = errors.New("checkpoint unavailable")
	require.Error(t, reader.acknowledge(2, 2, 130))
	assert.Equal(t, uint64(120), reader.appliedLSN.Load())
	assert.Equal(t, []pglogrepl.LSN{120}, stateStore.saved)

	require.NoError(t, reader.acknowledge(1, 1, 110))
	assert.Equal(t, uint64(120), reader.appliedLSN.Load())
	assert.Equal(t, []pglogrepl.LSN{120}, stateStore.saved)
}

func TestReaderAcknowledgePersistsOnlyContiguousPrefix(t *testing.T) {
	stateStore := &testStateStore{}
	reader := NewReader(&Config{StateStore: stateStore, LSNStateKey: "slot"}, nil)
	reader.appliedLSN.Store(100)

	require.NoError(t, reader.acknowledge(2, 2, 120))
	assert.Empty(t, stateStore.saved)
	assert.Equal(t, uint64(100), reader.appliedLSN.Load())

	require.NoError(t, reader.acknowledge(1, 1, 110))
	assert.Equal(t, []pglogrepl.LSN{120}, stateStore.saved)
	assert.Equal(t, uint64(120), reader.appliedLSN.Load())
}

func TestReaderAcknowledgeDefersStandbyStatusToReplicationLoop(t *testing.T) {
	stateStore := &testStateStore{}
	reader := NewReader(&Config{StateStore: stateStore, LSNStateKey: "slot"}, nil)

	var sends atomic.Int32
	reader.sendStandbyStatus = func(_ context.Context, _ pglogrepl.StandbyStatusUpdate) error {
		sends.Add(1)
		return nil
	}

	require.NoError(t, reader.acknowledge(1, 1, 120))
	assert.Zero(t, sends.Load(), "Ack must not write to PgConn from the consumer goroutine")

	require.NoError(t, reader.sendRequestedStandbyStatusUpdate(context.Background()))
	assert.Equal(t, int32(1), sends.Load())
}

func TestReaderEnqueueSendsKeepaliveWhenConsumerQueueIsFull(t *testing.T) {
	config := NewConfig("", "slot", "publication", "", "")
	config.StandbyMessageTimeout = time.Millisecond
	reader := NewReader(config, nil)
	reader.messageCH = make(chan *queuedMessage, 1)
	reader.messageCH <- &queuedMessage{}

	sent := make(chan struct{}, 1)
	reader.sendStandbyStatus = func(_ context.Context, _ pglogrepl.StandbyStatusUpdate) error {
		sent <- struct{}{}
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- reader.enqueueMessage(ctx, &queuedMessage{})
	}()

	select {
	case <-sent:
	case <-time.After(time.Second):
		t.Fatal("expected a standby status update while the consumer queue was full")
	}

	cancel()
	require.ErrorIs(t, <-errCh, context.Canceled)
}

func TestReaderCommitAcknowledgementCheckpointsWholeTransaction(t *testing.T) {
	stateStore := &testStateStore{}
	reader := NewReader(&Config{StateStore: stateStore, LSNStateKey: "slot"}, nil)
	reader.appliedLSN.Store(100)

	require.NoError(t, reader.acknowledge(1, 3, 130))
	assert.Equal(t, []pglogrepl.LSN{130}, stateStore.saved)
	assert.Equal(t, uint64(130), reader.appliedLSN.Load())
}

func TestReaderCommitAckCoversAllMessagesInTransaction(t *testing.T) {
	stateStore := &testStateStore{}
	reader := NewReader(&Config{StateStore: stateStore, LSNStateKey: "slot"}, nil)
	ackErr := make(chan error, 1)
	reader.listenerFunc = func(ctx *ListenerContext) {
		if _, isCommit := ctx.Message.(*format.Commit); isCommit {
			ackErr <- ctx.Ack()
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go reader.process(ctx)

	begin := make([]byte, 21)
	begin[0] = 'B'
	binary.BigEndian.PutUint64(begin[1:], 130)
	binary.BigEndian.PutUint32(begin[17:], 42)
	require.NoError(t, reader.handleLogicalMessage(ctx, begin, time.Now(), 110))

	commit := make([]byte, 26)
	commit[0] = 'C'
	binary.BigEndian.PutUint64(commit[2:], 130)
	binary.BigEndian.PutUint64(commit[10:], 131)
	require.NoError(t, reader.handleLogicalMessage(ctx, commit, time.Now(), 130))

	require.NoError(t, <-ackErr)
	assert.Equal(t, []pglogrepl.LSN{130}, stateStore.saved)
}
