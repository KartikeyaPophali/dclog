package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
	//"golang.org/x/tools/go/analysis/passes/nilfunc"
)

var (
	enc = binary.BigEndian
)

const (
	lenWidth = 8
)

// store represents the file which stores the records. It has a buffered writer to reduce system calls.
// The size indicates the size of the file and the position of the next entry.
//
// Illustrative example:
// 		Index: 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16
//	 Raw file: 5 a p p l e 3 c a t  4  b  a  l  l
// 3 records:
// 		Offset: 0, Position: 0, Record: len=5, data='apple'
// 		Offset: 1, Position: 6, Record: len=3, data='cat'
// 		Offset: 2, Position: 10, Record: len=4, data='ball'
// Offset and Position for a record form an index entry stored in the index struct.
type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// Append persists the given bytes to the store.
// Returns number of bytes written, position of data in store, and error if any.
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size
	// First write the length of the data to buffer
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	// Next write data to the buffer, not directly to file - reduce system calls
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	w += lenWidth // number of bytes of data is of type uint64 => 8 bytes == lenWidth
	s.size += uint64(w)
	return uint64(w), pos, nil
}

// Read returns the record stored at the given position.
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}
	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	return b, nil
}

// ReadAt implements the io.ReaderAt interface on the store type.
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

// Close safely closes the store's file. It persists any buffered data before closing.
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return err
	}
	return s.File.Close()
}
