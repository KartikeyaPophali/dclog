package log

import (
	"fmt"
	"os"
	"path"

	api "github.com/KartikeyaPophali/dclog/api/v1"
	"google.golang.org/protobuf/proto"
)

// segment wraps the store and the index to coordinate operations across the two.
type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}

func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}
	storeFile, err := os.OpenFile(path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}
	indexFile, err := os.OpenFile(path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}
	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}
	return s, nil
}

// Append appends the given record in the segment's store and saves its offset and position in the index.
func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	currentOffset := s.nextOffset
	record.Offset = currentOffset
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}
	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}
	if err = s.index.Write(uint32(s.nextOffset-s.baseOffset), pos); err != nil { // offset entries in index are relative to base offset for segment
		return 0, err
	}
	s.nextOffset += 1
	return currentOffset, nil
}

// Read returns the record stored in the segment at the specified offset.
func (s *segment) Read(offset uint64) (*api.Record, error) {
	relativeOffset := offset - s.baseOffset
	_, recordPosition, err := s.index.Read(int64(relativeOffset))
	if err != nil {
		return nil, err
	}
	b, err := s.store.Read(recordPosition)
	if err != nil {
		return nil, err
	}
	record := &api.Record{}
	err = proto.Unmarshal(b, record)
	return record, err
}

// IsMaxed returns whether the segment has reached its max size.
func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes || s.index.size >= s.config.Segment.MaxIndexBytes
}

// Remove removes the segment and its associated store and index.
func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

// Close closes the segment's store and index.
func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

func nearestMultiple(j, k uint64) uint64 {
	return (j / k) * k
}
