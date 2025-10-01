package message

import (
	"fmt"
	"time"

	"github.com/go-playground/errors"
	"github.com/hoanguyenkh/go-pg-wal/pkg/message/format"
	"github.com/jackc/pglogrepl"
)

type Message struct {
	Message  any
	WalStart pglogrepl.LSN
}

const (
	StreamAbortByte  Type = 'A'
	BeginByte        Type = 'B'
	CommitByte       Type = 'C'
	DeleteByte       Type = 'D'
	StreamStopByte   Type = 'E'
	InsertByte       Type = 'I'
	LogicalByte      Type = 'M'
	OriginByte       Type = 'O'
	RelationByte     Type = 'R'
	StreamStartByte  Type = 'S'
	TruncateByte     Type = 'T'
	UpdateByte       Type = 'U'
	TypeByte         Type = 'Y'
	StreamCommitByte Type = 'c'
)

const (
	XLogDataByteID                = 'w'
	PrimaryKeepaliveMessageByteID = 'k'
)

var ErrorByteNotSupported = errors.New("message byte not supported")

type Type uint8

var streamedTransaction bool

func New(data []byte, serverTime time.Time, relation map[uint32]*format.Relation) (any, error) {
	switch Type(data[0]) {
	case InsertByte:
		return format.NewInsert(data, streamedTransaction, relation, serverTime)
	case UpdateByte:
		return format.NewUpdate(data, streamedTransaction, relation, serverTime)
	case DeleteByte:
		return format.NewDelete(data, streamedTransaction, relation, serverTime)
	case BeginByte:
		// Begin transaction - return a special marker or nil
		return "BEGIN_TRANSACTION", nil
	case CommitByte:
		// Commit transaction - return a special marker or nil
		return "COMMIT_TRANSACTION", nil
	case StreamStopByte, StreamAbortByte, StreamCommitByte:
		streamedTransaction = false
		return nil, nil
	case RelationByte:
		msg, err := format.NewRelation(data, streamedTransaction)
		if err == nil {
			relation[msg.OID] = msg
		}
		return msg, err
	case StreamStartByte:
		streamedTransaction = true
		return nil, nil
	default:
		// Log unsupported message types for debugging
		fmt.Printf("Unsupported message type: %c (0x%02x)\n", data[0], data[0])
		return nil, nil
	}
}
