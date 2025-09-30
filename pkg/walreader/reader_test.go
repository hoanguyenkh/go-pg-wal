package walreader

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/hoanguyenkh/go-pg-wal/pkg/message/format"
)

// MockStateStore is a mock implementation of state.IStateStore
type MockStateStore struct {
	mock.Mock
}

func (m *MockStateStore) LoadLSN(ctx context.Context, key string) (pglogrepl.LSN, error) {
	args := m.Called(ctx, key)
	return args.Get(0).(pglogrepl.LSN), args.Error(1)
}

func (m *MockStateStore) SaveLSN(ctx context.Context, key string, lsn pglogrepl.LSN) error {
	args := m.Called(ctx, key, lsn)
	return args.Error(0)
}

func (m *MockStateStore) Close() error {
	args := m.Called()
	return args.Error(0)
}

// MockHandler is a mock implementation of MessageHandler
type MockHandler struct {
	mock.Mock
}

func (m *MockHandler) HandleInsert(msg *format.Insert) error {
	args := m.Called(msg)
	return args.Error(0)
}

func (m *MockHandler) HandleUpdate(msg *format.Update) error {
	args := m.Called(msg)
	return args.Error(0)
}

func (m *MockHandler) HandleDelete(msg *format.Delete) error {
	args := m.Called(msg)
	return args.Error(0)
}

func (m *MockHandler) HandleRelation(msg *format.Relation) error {
	args := m.Called(msg)
	return args.Error(0)
}

func (m *MockHandler) HandleBeginTransaction() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockHandler) HandleCommitTransaction() error {
	args := m.Called()
	return args.Error(0)
}

// MockBatchHandler is a mock implementation of BatchMessageHandler
type MockBatchHandler struct {
	mock.Mock
}

func (m *MockBatchHandler) HandleInsert(msg *format.Insert) error {
	args := m.Called(msg)
	return args.Error(0)
}

func (m *MockBatchHandler) HandleUpdate(msg *format.Update) error {
	args := m.Called(msg)
	return args.Error(0)
}

func (m *MockBatchHandler) HandleDelete(msg *format.Delete) error {
	args := m.Called(msg)
	return args.Error(0)
}

func (m *MockBatchHandler) HandleRelation(msg *format.Relation) error {
	args := m.Called(msg)
	return args.Error(0)
}

func (m *MockBatchHandler) HandleBeginTransaction() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockBatchHandler) HandleCommitTransaction() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockBatchHandler) HandleBatch(messages []BatchMessage) error {
	args := m.Called(messages)
	return args.Error(0)
}

// TestNewReader tests the NewReader function
func TestNewReader(t *testing.T) {
	tests := []struct {
		name              string
		config            *Config
		handler           MessageHandler
		expectedBatchMode bool
	}{
		{
			name: "with file store and no batch",
			config: &Config{
				ConnString:      "test-conn",
				SlotName:        "test_slot",
				PublicationName: "test_pub",
				BatchSize:       0,
				BatchTimeout:    1 * time.Second,
			},
			handler:           &MockHandler{},
			expectedBatchMode: false,
		},
		{
			name: "with batch handler and batch enabled",
			config: &Config{
				ConnString:      "test-conn",
				SlotName:        "test_slot",
				PublicationName: "test_pub",
				BatchSize:       100,
				BatchTimeout:    5 * time.Second,
			},
			handler:           &MockBatchHandler{},
			expectedBatchMode: true,
		},
		{
			name: "with custom state store",
			config: &Config{
				ConnString:      "test-conn",
				SlotName:        "test_slot",
				PublicationName: "test_pub",
				StateStore:      &MockStateStore{},
				BatchSize:       0,
			},
			handler:           &MockHandler{},
			expectedBatchMode: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := NewReader(tt.config, tt.handler)

			assert.NotNil(t, reader)
			assert.Equal(t, tt.config, reader.config)
			assert.Equal(t, tt.handler, reader.handler)
			assert.NotNil(t, reader.stateStore)
			assert.NotNil(t, reader.relations)
			assert.NotNil(t, reader.messageBatch)

			if tt.expectedBatchMode {
				assert.NotNil(t, reader.batchHandler)
			} else {
				if _, ok := tt.handler.(BatchMessageHandler); !ok {
					assert.Nil(t, reader.batchHandler)
				}
			}
		})
	}
}

// TestGetLastLSN tests the GetLastLSN method
func TestGetLastLSN(t *testing.T) {
	config := &Config{
		ConnString:      "test-conn",
		SlotName:        "test_slot",
		PublicationName: "test_pub",
	}
	handler := &MockHandler{}
	reader := NewReader(config, handler)

	expectedLSN := pglogrepl.LSN(12345)
	reader.lastLSN = expectedLSN

	assert.Equal(t, expectedLSN, reader.GetLastLSN())
}

// TestShouldFlushBatch tests the shouldFlushBatch method
func TestShouldFlushBatch(t *testing.T) {
	tests := []struct {
		name           string
		batchSize      int
		batchTimeout   time.Duration
		setupReader    func(*Reader)
		expectedResult bool
	}{
		{
			name:         "no batch handler",
			batchSize:    100,
			batchTimeout: 5 * time.Second,
			setupReader: func(r *Reader) {
				r.batchHandler = nil
			},
			expectedResult: false,
		},
		{
			name:         "batch size is 0",
			batchSize:    0,
			batchTimeout: 5 * time.Second,
			setupReader: func(r *Reader) {
				r.batchHandler = &MockBatchHandler{}
			},
			expectedResult: false,
		},
		{
			name:         "empty batch",
			batchSize:    100,
			batchTimeout: 5 * time.Second,
			setupReader: func(r *Reader) {
				r.batchHandler = &MockBatchHandler{}
				r.messageBatch = []BatchMessage{}
			},
			expectedResult: false,
		},
		{
			name:         "timeout reached",
			batchSize:    100,
			batchTimeout: 1 * time.Second,
			setupReader: func(r *Reader) {
				r.batchHandler = &MockBatchHandler{}
				r.messageBatch = []BatchMessage{{Type: "insert"}}
				r.lastBatchFlush = time.Now().Add(-2 * time.Second)
			},
			expectedResult: true,
		},
		{
			name:         "timeout not reached",
			batchSize:    100,
			batchTimeout: 5 * time.Second,
			setupReader: func(r *Reader) {
				r.batchHandler = &MockBatchHandler{}
				r.messageBatch = []BatchMessage{{Type: "insert"}}
				r.lastBatchFlush = time.Now()
			},
			expectedResult: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &Config{
				ConnString:      "test-conn",
				SlotName:        "test_slot",
				PublicationName: "test_pub",
				BatchSize:       tt.batchSize,
				BatchTimeout:    tt.batchTimeout,
			}
			handler := &MockHandler{}
			reader := NewReader(config, handler)
			tt.setupReader(reader)

			result := reader.shouldFlushBatch()
			assert.Equal(t, tt.expectedResult, result)
		})
	}
}

// TestFlushBatch tests the flushBatch method
func TestFlushBatch(t *testing.T) {
	tests := []struct {
		name          string
		setupReader   func(*Reader, *MockBatchHandler, *MockStateStore)
		expectedError bool
		errorContains string
	}{
		{
			name: "empty batch",
			setupReader: func(r *Reader, h *MockBatchHandler, s *MockStateStore) {
				r.messageBatch = []BatchMessage{}
			},
			expectedError: false,
		},
		{
			name: "successful batch flush",
			setupReader: func(r *Reader, h *MockBatchHandler, s *MockStateStore) {
				r.messageBatch = []BatchMessage{
					{Type: "insert"},
					{Type: "update"},
				}
				r.lastLSN = pglogrepl.LSN(100)
				h.On("HandleBatch", mock.Anything).Return(nil)
				s.On("SaveLSN", mock.Anything, "test_slot", pglogrepl.LSN(100)).Return(nil)
			},
			expectedError: false,
		},
		{
			name: "handler error",
			setupReader: func(r *Reader, h *MockBatchHandler, s *MockStateStore) {
				r.messageBatch = []BatchMessage{{Type: "insert"}}
				h.On("HandleBatch", mock.Anything).Return(errors.New("handler error"))
			},
			expectedError: true,
			errorContains: "error handling batch",
		},
		{
			name: "state store error",
			setupReader: func(r *Reader, h *MockBatchHandler, s *MockStateStore) {
				r.messageBatch = []BatchMessage{{Type: "insert"}}
				r.lastLSN = pglogrepl.LSN(200)
				h.On("HandleBatch", mock.Anything).Return(nil)
				s.On("SaveLSN", mock.Anything, "test_slot", pglogrepl.LSN(200)).Return(errors.New("save error"))
			},
			expectedError: true,
			errorContains: "CRITICAL: cannot save LSN state",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockHandler := &MockBatchHandler{}
			mockStateStore := &MockStateStore{}

			config := &Config{
				ConnString:      "test-conn",
				SlotName:        "test_slot",
				PublicationName: "test_pub",
				BatchSize:       100,
				BatchTimeout:    5 * time.Second,
				StateStore:      mockStateStore,
				LSNStateKey:     "test_slot",
			}

			reader := NewReader(config, mockHandler)
			reader.batchHandler = mockHandler
			tt.setupReader(reader, mockHandler, mockStateStore)

			ctx := context.Background()
			err := reader.flushBatch(ctx)

			if tt.expectedError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
				assert.Equal(t, 0, len(reader.messageBatch))
			}

			mockHandler.AssertExpectations(t)
			mockStateStore.AssertExpectations(t)
		})
	}
}

// TestHandleMessage tests the handleMessage method
func TestHandleMessage(t *testing.T) {
	tests := []struct {
		name          string
		batchSize     int
		msgType       string
		setupMocks    func(*MockHandler, *MockBatchHandler, *MockStateStore)
		expectedBatch bool
		expectedError bool
	}{
		{
			name:      "single mode - insert",
			batchSize: 0,
			msgType:   "insert",
			setupMocks: func(h *MockHandler, b *MockBatchHandler, s *MockStateStore) {
				h.On("HandleInsert", mock.Anything).Return(nil)
			},
			expectedBatch: false,
			expectedError: false,
		},
		{
			name:      "single mode - update",
			batchSize: 0,
			msgType:   "update",
			setupMocks: func(h *MockHandler, b *MockBatchHandler, s *MockStateStore) {
				h.On("HandleUpdate", mock.Anything).Return(nil)
			},
			expectedBatch: false,
			expectedError: false,
		},
		{
			name:      "single mode - delete",
			batchSize: 0,
			msgType:   "delete",
			setupMocks: func(h *MockHandler, b *MockBatchHandler, s *MockStateStore) {
				h.On("HandleDelete", mock.Anything).Return(nil)
			},
			expectedBatch: false,
			expectedError: false,
		},
		{
			name:      "single mode - relation",
			batchSize: 0,
			msgType:   "relation",
			setupMocks: func(h *MockHandler, b *MockBatchHandler, s *MockStateStore) {
				h.On("HandleRelation", mock.Anything).Return(nil)
			},
			expectedBatch: false,
			expectedError: false,
		},
		{
			name:      "batch mode - add to batch",
			batchSize: 100,
			msgType:   "insert",
			setupMocks: func(h *MockHandler, b *MockBatchHandler, s *MockStateStore) {
				// No handler calls expected when adding to batch
			},
			expectedBatch: true,
			expectedError: false,
		},
		{
			name:      "single mode - handler error",
			batchSize: 0,
			msgType:   "insert",
			setupMocks: func(h *MockHandler, b *MockBatchHandler, s *MockStateStore) {
				h.On("HandleInsert", mock.Anything).Return(errors.New("handler error"))
			},
			expectedBatch: false,
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockHandler := &MockHandler{}
			mockBatchHandler := &MockBatchHandler{}
			mockStateStore := &MockStateStore{}

			config := &Config{
				ConnString:      "test-conn",
				SlotName:        "test_slot",
				PublicationName: "test_pub",
				BatchSize:       tt.batchSize,
				BatchTimeout:    5 * time.Second,
				StateStore:      mockStateStore,
			}

			var handler MessageHandler
			if tt.batchSize > 0 {
				handler = mockBatchHandler
			} else {
				handler = mockHandler
			}

			reader := NewReader(config, handler)
			tt.setupMocks(mockHandler, mockBatchHandler, mockStateStore)

			// Create test messages
			insert := &format.Insert{TableName: "test_table"}
			update := &format.Update{TableName: "test_table"}
			delete := &format.Delete{TableName: "test_table"}
			relation := &format.Relation{Name: "test_table"}

			var saveLSN bool
			var err error

			switch tt.msgType {
			case "insert":
				saveLSN, err = reader.handleMessage("insert", insert, nil, nil, nil)
			case "update":
				saveLSN, err = reader.handleMessage("update", nil, update, nil, nil)
			case "delete":
				saveLSN, err = reader.handleMessage("delete", nil, nil, delete, nil)
			case "relation":
				saveLSN, err = reader.handleMessage("relation", nil, nil, nil, relation)
			}

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			if tt.expectedBatch {
				assert.False(t, saveLSN)
				assert.Equal(t, 1, len(reader.messageBatch))
				assert.Equal(t, tt.msgType, reader.messageBatch[0].Type)
			} else {
				assert.True(t, saveLSN)
			}

			mockHandler.AssertExpectations(t)
		})
	}
}

// TestHandleMessageBatchFull tests batch flush when batch is full
func TestHandleMessageBatchFull(t *testing.T) {
	mockBatchHandler := &MockBatchHandler{}
	mockStateStore := &MockStateStore{}

	config := &Config{
		ConnString:      "test-conn",
		SlotName:        "test_slot",
		PublicationName: "test_pub",
		BatchSize:       2, // Small batch size for testing
		BatchTimeout:    5 * time.Second,
		StateStore:      mockStateStore,
		LSNStateKey:     "test_slot",
	}

	reader := NewReader(config, mockBatchHandler)
	reader.lastLSN = pglogrepl.LSN(100)

	// Set up expectations
	mockBatchHandler.On("HandleBatch", mock.MatchedBy(func(msgs []BatchMessage) bool {
		return len(msgs) == 2
	})).Return(nil)
	mockStateStore.On("SaveLSN", mock.Anything, "test_slot", pglogrepl.LSN(100)).Return(nil)

	insert := &format.Insert{TableName: "test_table"}

	// Add first message - should not flush
	saveLSN, err := reader.handleMessage("insert", insert, nil, nil, nil)
	assert.NoError(t, err)
	assert.False(t, saveLSN)
	assert.Equal(t, 1, len(reader.messageBatch))

	// Add second message - should trigger flush
	saveLSN, err = reader.handleMessage("insert", insert, nil, nil, nil)
	assert.NoError(t, err)
	assert.True(t, saveLSN)
	assert.Equal(t, 0, len(reader.messageBatch)) // Batch should be cleared

	mockBatchHandler.AssertExpectations(t)
	mockStateStore.AssertExpectations(t)
}

// TestClose tests the Close method
func TestClose(t *testing.T) {
	tests := []struct {
		name          string
		setupReader   func(*Reader, *MockBatchHandler, *MockStateStore)
		expectedError bool
	}{
		{
			name: "successful close with empty batch",
			setupReader: func(r *Reader, h *MockBatchHandler, s *MockStateStore) {
				r.messageBatch = []BatchMessage{}
				s.On("Close").Return(nil)
			},
			expectedError: false,
		},
		{
			name: "close with pending batch",
			setupReader: func(r *Reader, h *MockBatchHandler, s *MockStateStore) {
				r.messageBatch = []BatchMessage{{Type: "insert"}}
				r.lastLSN = pglogrepl.LSN(100)
				h.On("HandleBatch", mock.Anything).Return(nil)
				s.On("SaveLSN", mock.Anything, "test_slot", pglogrepl.LSN(100)).Return(nil)
				s.On("Close").Return(nil)
			},
			expectedError: false,
		},
		{
			name: "state store close error",
			setupReader: func(r *Reader, h *MockBatchHandler, s *MockStateStore) {
				r.messageBatch = []BatchMessage{}
				s.On("Close").Return(errors.New("close error"))
			},
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockBatchHandler := &MockBatchHandler{}
			mockStateStore := &MockStateStore{}

			config := &Config{
				ConnString:      "test-conn",
				SlotName:        "test_slot",
				PublicationName: "test_pub",
				BatchSize:       100,
				BatchTimeout:    5 * time.Second,
				StateStore:      mockStateStore,
				LSNStateKey:     "test_slot",
			}

			reader := NewReader(config, mockBatchHandler)
			reader.batchHandler = mockBatchHandler
			tt.setupReader(reader, mockBatchHandler, mockStateStore)

			ctx := context.Background()
			err := reader.Close(ctx)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			mockBatchHandler.AssertExpectations(t)
			mockStateStore.AssertExpectations(t)
		})
	}
}

// TestBatchMessage tests the BatchMessage struct
func TestBatchMessage(t *testing.T) {
	insert := &format.Insert{TableName: "test_insert"}
	update := &format.Update{TableName: "test_update"}
	delete := &format.Delete{TableName: "test_delete"}
	relation := &format.Relation{Name: "test_relation"}

	tests := []struct {
		name     string
		msg      BatchMessage
		expected string
	}{
		{
			name:     "insert message",
			msg:      BatchMessage{Type: "insert", Insert: insert},
			expected: "insert",
		},
		{
			name:     "update message",
			msg:      BatchMessage{Type: "update", Update: update},
			expected: "update",
		},
		{
			name:     "delete message",
			msg:      BatchMessage{Type: "delete", Delete: delete},
			expected: "delete",
		},
		{
			name:     "relation message",
			msg:      BatchMessage{Type: "relation", Relation: relation},
			expected: "relation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.msg.Type)
			switch tt.msg.Type {
			case "insert":
				assert.NotNil(t, tt.msg.Insert)
			case "update":
				assert.NotNil(t, tt.msg.Update)
			case "delete":
				assert.NotNil(t, tt.msg.Delete)
			case "relation":
				assert.NotNil(t, tt.msg.Relation)
			}
		})
	}
}
