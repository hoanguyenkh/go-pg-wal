package format

import (
	"encoding/binary"
	"fmt"
	"time"
)

type Commit struct {
	MessageTime time.Time // Server time khi nhận message
	Flags       uint8     // Commit flags
	CommitLSN   uint64    // LSN of the commit record
	EndLSN      uint64    // End LSN of the transaction
	CommitTime  time.Time // Transaction commit timestamp
}

func NewCommit(data []byte, serverTime time.Time) (*Commit, error) {
	msg := &Commit{
		MessageTime: serverTime,
	}
	if err := msg.decode(data); err != nil {
		return nil, err
	}
	return msg, nil
}

func (m *Commit) decode(data []byte) error {
	// Format: [Type 1][Flags 1][Commit LSN 8][End LSN 8][Commit Time 8] = 26 bytes
	if len(data) < 26 {
		return fmt.Errorf("commit message must be 26 bytes, got %d", len(data))
	}

	offset := 1 // Skip 'C' message type

	// Read Flags (1 byte)
	m.Flags = data[offset]
	offset += 1

	// Read Commit LSN (8 bytes)
	m.CommitLSN = binary.BigEndian.Uint64(data[offset:])
	offset += 8

	// Read End LSN (8 bytes)
	m.EndLSN = binary.BigEndian.Uint64(data[offset:])
	offset += 8

	// Read Commit Time (8 bytes)
	commitTimeMicros := int64(binary.BigEndian.Uint64(data[offset:]))
	m.CommitTime = pgTimeToGoTime(commitTimeMicros)

	return nil
}
