package walreader

import (
	"context"
	"encoding/binary"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoanguyenkh/go-pg-wal/pkg/message"
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
	assert.Equal(t, pglogrepl.LSN(100), sent.WALApplyPosition)
}

func TestReaderReceiveMessageContextHonorsParentCancel(t *testing.T) {
	config := NewConfig("", "slot", "publication", "", "")
	config.StandbyMessageTimeout = time.Hour
	reader := NewReader(config, nil)

	ctx, cancel := context.WithCancel(context.Background())
	msgCtx, stop := reader.newReceiveMessageContext(ctx)
	defer stop()

	cancel()
	select {
	case <-msgCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("receive context should be canceled when the parent context is canceled")
	}
	require.ErrorIs(t, msgCtx.Err(), context.Canceled)
}

func TestApplyLogicalDecodingWorkMem_EmptyIsNoop(t *testing.T) {
	config := NewConfig("", "slot", "publication", "", "")
	reader := NewReader(config, nil)

	// r.conn is nil; an empty value must return before touching it.
	require.NoError(t, reader.applyLogicalDecodingWorkMem(context.Background()))
}

func TestApplyLogicalDecodingWorkMem_RejectsInvalidValue(t *testing.T) {
	config := NewConfig("", "slot", "publication", "", "")
	config.LogicalDecodingWorkMem = "256MB; DROP TABLE foo"
	reader := NewReader(config, nil)

	err := reader.applyLogicalDecodingWorkMem(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid LogicalDecodingWorkMem")
}

func TestApplyLogicalDecodingWorkMem_AcceptsValidSyntax(t *testing.T) {
	for _, value := range []string{"256MB", "65536kB", "1GB", "134217728"} {
		assert.True(t, logicalDecodingWorkMemPattern.MatchString(value), "expected %q to be accepted", value)
	}
	for _, value := range []string{"", "256 MB; --", "abc", "256MB'"} {
		assert.False(t, logicalDecodingWorkMemPattern.MatchString(value), "expected %q to be rejected", value)
	}
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

func TestReaderAcknowledgeRetriesCheckpointAfterSaveFailure(t *testing.T) {
	stateStore := &testStateStore{}
	reader := NewReader(&Config{StateStore: stateStore, LSNStateKey: "slot"}, nil)
	reader.appliedLSN.Store(100)

	require.NoError(t, reader.acknowledge(1, 1, 120))
	stateStore.saveErr = errors.New("checkpoint unavailable")
	require.Error(t, reader.acknowledge(2, 2, 130))
	assert.Equal(t, uint64(120), reader.appliedLSN.Load())

	stateStore.saveErr = nil
	require.NoError(t, reader.acknowledge(2, 2, 130))
	assert.Equal(t, []pglogrepl.LSN{120, 130}, stateStore.saved)
	assert.Equal(t, uint64(130), reader.appliedLSN.Load())
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

func TestReaderAcknowledgeRejectsExcessUnresolvedRanges(t *testing.T) {
	stateStore := &testStateStore{}
	reader := NewReader(&Config{StateStore: stateStore, LSNStateKey: "slot", AckWindow: 1}, nil)

	require.NoError(t, reader.acknowledge(2, 2, 120))
	require.ErrorIs(t, reader.acknowledge(3, 3, 130), ErrAckWindowFull)
	assert.Len(t, reader.acknowledgedRanges, 1)

	require.NoError(t, reader.acknowledge(1, 1, 110))
	assert.Equal(t, []pglogrepl.LSN{120}, stateStore.saved)
	assert.Empty(t, reader.acknowledgedRanges)
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

func TestReaderEnqueueDoesNotSendKeepaliveWhenQueueHasSpace(t *testing.T) {
	reader := NewReader(NewConfig("", "slot", "publication", "", ""), nil)
	var sends atomic.Int32
	reader.sendStandbyStatus = func(_ context.Context, _ pglogrepl.StandbyStatusUpdate) error {
		sends.Add(1)
		return nil
	}

	require.NoError(t, reader.enqueueMessage(context.Background(), &queuedMessage{}))
	assert.Zero(t, sends.Load())
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
	assert.Equal(t, []pglogrepl.LSN{131}, stateStore.saved)
}

func queuedTestMessage(sequence uint64, lsn pglogrepl.LSN) *queuedMessage {
	return &queuedMessage{
		message: &message.Message{
			Message:  sequence,
			WalStart: lsn,
		},
		sequence:         sequence,
		ackStartSequence: sequence,
	}
}

func TestReaderDeliveryWindowBlocksUntilContiguousAck(t *testing.T) {
	stateStore := &testStateStore{}
	reader := NewReader(&Config{StateStore: stateStore, LSNStateKey: "slot", AckWindow: 2}, nil)

	type delivery struct {
		seq uint64
		ack func() error
	}
	delivered := make(chan delivery, 8)
	reader.listenerFunc = func(ctx *ListenerContext) {
		delivered <- delivery{seq: ctx.Message.(uint64), ack: ctx.Ack}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go reader.process(ctx)

	require.NoError(t, reader.enqueueMessage(ctx, queuedTestMessage(1, 110)))
	require.NoError(t, reader.enqueueMessage(ctx, queuedTestMessage(2, 120)))
	require.NoError(t, reader.enqueueMessage(ctx, queuedTestMessage(3, 130)))

	first := <-delivered
	second := <-delivered
	require.Equal(t, uint64(1), first.seq)
	require.Equal(t, uint64(2), second.seq)

	select {
	case extra := <-delivered:
		t.Fatalf("delivered sequence %d before the ack window opened", extra.seq)
	case <-time.After(50 * time.Millisecond):
	}

	require.NoError(t, first.ack())
	third := <-delivered
	assert.Equal(t, uint64(3), third.seq)
	assert.LessOrEqual(t, len(reader.acknowledgedRanges), 2)
}

func TestReaderDeliveryWindowAllowsLargeTransactionCommit(t *testing.T) {
	stateStore := &testStateStore{}
	reader := NewReader(&Config{StateStore: stateStore, LSNStateKey: "slot", AckWindow: 2}, nil)
	commitAck := make(chan error, 1)
	reader.listenerFunc = func(ctx *ListenerContext) {
		if _, isCommit := ctx.Message.(*format.Commit); isCommit {
			commitAck <- ctx.Ack()
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go reader.process(ctx)

	require.NoError(t, reader.enqueueMessage(ctx, &queuedMessage{
		message:          &message.Message{Message: &format.Begin{}, WalStart: 110},
		sequence:         1,
		ackStartSequence: 1,
	}))
	require.NoError(t, reader.enqueueMessage(ctx, queuedTestMessage(2, 120)))
	require.NoError(t, reader.enqueueMessage(ctx, queuedTestMessage(3, 130)))
	require.NoError(t, reader.enqueueMessage(ctx, &queuedMessage{
		message:          &message.Message{Message: &format.Commit{EndLSN: 150}, WalStart: 140},
		sequence:         4,
		ackStartSequence: 1,
	}))

	select {
	case err := <-commitAck:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("commit should be delivered even when the transaction exceeds AckWindow")
	}
	assert.Equal(t, []pglogrepl.LSN{150}, stateStore.saved)
}

func TestReaderDeliveryWindowBackpressuresKeepalivePath(t *testing.T) {
	config := NewConfig("", "slot", "publication", "", "")
	config.StandbyMessageTimeout = time.Millisecond
	config.AckWindow = 1
	stateStore := &testStateStore{}
	config.StateStore = stateStore
	reader := NewReader(config, nil)
	reader.messageCH = make(chan *queuedMessage, 1)

	delivered := make(chan struct{}, 1)
	reader.listenerFunc = func(ctx *ListenerContext) {
		delivered <- struct{}{}
	}

	sent := make(chan struct{}, 1)
	reader.sendStandbyStatus = func(_ context.Context, _ pglogrepl.StandbyStatusUpdate) error {
		sent <- struct{}{}
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go reader.process(ctx)

	require.NoError(t, reader.enqueueMessage(ctx, queuedTestMessage(1, 110)))
	select {
	case <-delivered:
	case <-time.After(time.Second):
		t.Fatal("expected the first message to be delivered")
	}

	enqueueErr := make(chan error, 1)
	go func() {
		seq := uint64(2)
		for {
			err := reader.enqueueMessage(ctx, queuedTestMessage(seq, pglogrepl.LSN(100+seq*10)))
			if err != nil {
				enqueueErr <- err
				return
			}
			seq++
		}
	}()

	select {
	case <-sent:
	case <-time.After(time.Second):
		t.Fatal("expected a standby status update while the delivery window and queue were full")
	}

	cancel()
	require.ErrorIs(t, <-enqueueErr, context.Canceled)
}

func TestReaderWaitForDeliveryWindowHonorsContext(t *testing.T) {
	reader := NewReader(&Config{AckWindow: 1}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, reader.waitForDeliveryWindow(ctx, 2), context.Canceled)
}
