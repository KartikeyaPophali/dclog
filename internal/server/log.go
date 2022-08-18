package server

import (
	"fmt"
	"sync"
)

// Log represents an append-only log of records.
type Log struct {
	mu      sync.Mutex
	records []Record
}

// New is used to instantiate a new Log.
func NewLog() *Log {
	return &Log{}
}

// Append appends a new record to the Log.
func (l *Log) Append(record Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	record.Offset = uint64(len(l.records))
	l.records = append(l.records, record)
	return record.Offset, nil
}

// Read returns a record from the Log given an offset.
// The offset should be less than the number of records in the Log.
func (l *Log) Read(offset uint64) (Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if offset >= uint64(len(l.records)) {
		return Record{}, ErrOffsetNotFound
	}
	return l.records[offset], nil
}

// Record represents the base unit of data in the Log.
type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}

// ErrOffsetNotFound is used to return an error if a Read is requested with an invalid offset.
var ErrOffsetNotFound = fmt.Errorf("offset not found")
