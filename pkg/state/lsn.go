package state

import (
	"os"

	"github.com/jackc/pglogrepl"
)

// SaveLSNState writes the current LSN to a file
func SaveLSNState(filename string, lsn pglogrepl.LSN) error {
	return os.WriteFile(filename, []byte(lsn.String()), 0644)
}

// LoadLSNState reads the LSN from a file
func LoadLSNState(filename string) (pglogrepl.LSN, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return 0, err
	}
	return pglogrepl.ParseLSN(string(data))
}
