package walreader

import (
	"github.com/hoanguyenkh/go-pg-wal/pkg/message/format"
)

// MessageHandler defines the interface for handling WAL messages
type MessageHandler interface {
	// HandleInsert is called when an INSERT operation is detected
	HandleInsert(msg *format.Insert) error

	// HandleUpdate is called when an UPDATE operation is detected
	HandleUpdate(msg *format.Update) error

	// HandleDelete is called when a DELETE operation is detected
	HandleDelete(msg *format.Delete) error

	// HandleRelation is called when a relation (table schema) is received
	HandleRelation(msg *format.Relation) error

	// HandleBeginTransaction is called at the start of a transaction
	HandleBeginTransaction() error

	// HandleCommitTransaction is called when a transaction is committed
	HandleCommitTransaction() error
}

// BatchMessage represents a single message in a batch
type BatchMessage struct {
	Type     string // "insert", "update", "delete", "relation"
	Insert   *format.Insert
	Update   *format.Update
	Delete   *format.Delete
	Relation *format.Relation
}

// BatchMessageHandler defines the interface for handling WAL messages in batches
// If a handler implements this interface, the reader will use batch processing
type BatchMessageHandler interface {
	MessageHandler // Still supports single message handling as fallback

	// HandleBatch is called when a batch of messages is ready to be processed
	// Returns error if batch processing fails
	HandleBatch(messages []BatchMessage) error
}

// DefaultHandler provides a basic implementation that logs messages
type DefaultHandler struct{}

func (h *DefaultHandler) HandleInsert(msg *format.Insert) error {
	// Override this method in your implementation
	return nil
}

func (h *DefaultHandler) HandleUpdate(msg *format.Update) error {
	// Override this method in your implementation
	return nil
}

func (h *DefaultHandler) HandleDelete(msg *format.Delete) error {
	// Override this method in your implementation
	return nil
}

func (h *DefaultHandler) HandleRelation(msg *format.Relation) error {
	// Override this method in your implementation
	return nil
}

func (h *DefaultHandler) HandleBeginTransaction() error {
	// Override this method in your implementation
	return nil
}

func (h *DefaultHandler) HandleCommitTransaction() error {
	// Override this method in your implementation
	return nil
}
