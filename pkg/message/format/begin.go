package format

import (
	"encoding/binary"
	"fmt"
	"time"
)

type Begin struct {
	MessageTime time.Time // Server time khi nhận message
	FinalLSN    uint64    // LSN của COMMIT (không phải begin!)
	CommitTime  time.Time // Transaction commit time
	XID         uint32
}

func NewBegin(data []byte, serverTime time.Time) (*Begin, error) {
	msg := &Begin{
		MessageTime: serverTime,
	}
	if err := msg.decode(data); err != nil {
		return nil, err
	}
	return msg, nil
}

func (m *Begin) decode(data []byte) error {
	// Format: [Type 1][Final LSN 8][Commit Time 8][XID 4] = 21 bytes
	if len(data) < 21 {
		return fmt.Errorf("begin message must be 21 bytes, got %d", len(data))
	}

	offset := 1 // Skip 'B' message type

	// Read Final LSN (8 bytes)
	m.FinalLSN = binary.BigEndian.Uint64(data[offset:])
	offset += 8

	// Read Commit Time (8 bytes) - PostgreSQL timestamp (microseconds since 2000-01-01)
	commitTimeMicros := int64(binary.BigEndian.Uint64(data[offset:]))
	m.CommitTime = pgTimeToGoTime(commitTimeMicros)
	offset += 8

	// Read XID (4 bytes)
	m.XID = binary.BigEndian.Uint32(data[offset:])

	return nil
}

// Convert PostgreSQL timestamp to Go time
func pgTimeToGoTime(microseconds int64) time.Time {
	// PostgreSQL epoch: 2000-01-01 00:00:00 UTC
	pgEpoch := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	return pgEpoch.Add(time.Duration(microseconds) * time.Microsecond)
}
