package walreader

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pglogrepl"
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

func TestReaderAcknowledgeAdvancesAppliedLSNOnlyAfterCheckpointSave(t *testing.T) {
	stateStore := &testStateStore{}
	reader := NewReader(&Config{StateStore: stateStore, LSNStateKey: "slot"}, nil)
	reader.appliedLSN.Store(100)

	var sent pglogrepl.StandbyStatusUpdate
	reader.sendStandbyStatus = func(_ context.Context, status pglogrepl.StandbyStatusUpdate) error {
		sent = status
		return nil
	}

	require.NoError(t, reader.acknowledge(context.Background(), 120))
	assert.Equal(t, []pglogrepl.LSN{120}, stateStore.saved)
	assert.Equal(t, pglogrepl.LSN(120), sent.WALFlushPosition)

	stateStore.saveErr = errors.New("checkpoint unavailable")
	require.Error(t, reader.acknowledge(context.Background(), 130))
	assert.Equal(t, uint64(120), reader.appliedLSN.Load())
	assert.Equal(t, []pglogrepl.LSN{120}, stateStore.saved)
	assert.Equal(t, pglogrepl.LSN(120), sent.WALFlushPosition)

	require.NoError(t, reader.acknowledge(context.Background(), 110))
	assert.Equal(t, uint64(120), reader.appliedLSN.Load())
	assert.Equal(t, []pglogrepl.LSN{120}, stateStore.saved)
}
