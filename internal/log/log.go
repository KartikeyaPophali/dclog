package log

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/KartikeyaPophali/dclog/api/v1"
)

// Log encapsulates the slice of all segments and a pointer to the active segment.
type Log struct {
	mu sync.RWMutex

	Dir           string
	Config        Config
	activeSegment *segment
	segments      []*segment
}

// NewLog creates and sets up the Log datastructure.
func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}
	l := &Log{
		Dir:    dir,
		Config: c,
	}
	return l, l.setup()
}

// setup reads the log's segment files from persistent storage and sets up the log for use.
// If this is a new log with no segments, then one is created and set as active.
func (l *Log) setup() error {
	files, err := ioutil.ReadDir(l.Dir)
	if err != nil {
		return err
	}
	var baseOffsets []uint64
	for _, file := range files {
		offStr := strings.TrimSuffix(file.Name(), path.Ext(file.Name()))
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}
	sort.Slice(baseOffsets, func(i, j int) bool { // so that []segments is sorted old to new
		return baseOffsets[i] < baseOffsets[j]
	})
	for i := 0; i < len(baseOffsets); i++ {
		if err = l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
		i++ // baseOffsets has double entries corresponding to store and index files for each segment
	}
	if l.segments == nil {
		if err = l.newSegment(l.Config.Segment.InitialOffset); err != nil {
			return err
		}
	}
	return nil
}

// newSegment loads an existing segment or creates a new one given the base offset.
func (l *Log) newSegment(baseOffset uint64) error {
	seg, err := newSegment(l.Dir, baseOffset, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, seg)
	l.activeSegment = seg
	return nil
}

// Append appends a record to the log and returns the offset.
//
// It checks whether the segment has reached its maximum size after the operation,
// therefore it is possible for a segment to cross its MaxStoreBytes or MaxIndexBytes limit.
// Example - if MaxStoreBytes=16 and one appends "hello world" as record value to a segment,
// its store size would be 8+11=19 before a new segment is created
func (l *Log) Append(record *api.Record) (offset uint64, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	offset, err = l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}
	if l.activeSegment.IsMaxed() {
		err = l.newSegment(offset + 1)
	}
	return offset, err
}

// Read reads a record from the log given its offset.
func (l *Log) Read(offset uint64) (*api.Record, error) {
	// TODO: make lock per segment instead of entire log (for all read related methods)
	l.mu.RLock()
	defer l.mu.RUnlock()
	var readSeg *segment
	// TODO: use binary search instead of linear search to find read segment - can use sort search()
	for _, seg := range l.segments {
		if seg.baseOffset <= offset && offset < seg.nextOffset {
			readSeg = seg
			break
		}
	}
	if readSeg == nil {
		return nil, fmt.Errorf("offset out of range: %d", offset)
	}
	return readSeg.Read(offset)
}

// Close closes the log safely by closing all segments.
func (l *Log) Close() error {
	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Remove erases the log contents by removing all its segment files.
func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.Dir)
}

// Reset removes the current log contents and sets up a new log.
func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}
	return l.setup()
}

// LowestOffset returns the lowest offset for the records stored in the log.
func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].baseOffset, nil
}

// HighestOffset returns highest offset for the records stored in the log.
func (l *Log) HighestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	off := l.segments[len(l.segments)-1].nextOffset
	if off == 0 {
		return off, nil
	}
	return off - 1, nil
}

// Truncate removes all segments whose highest offset is lower than or equal to the lowest.
// It will be called periodically to clear disc space by removing old segments.
func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var segments []*segment
	for _, segment := range l.segments {
		if segment.nextOffset <= lowest+1 {
			if err := segment.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, segment)
	}
	l.segments = segments
	return nil
}

// Reader returns an io.Reader to read the whole log.
func (l *Log) Reader() io.Reader {
	l.mu.RLock()
	defer l.mu.RUnlock()
	readers := make([]io.Reader, len(l.segments))
	for i, seg := range l.segments {
		o := &originReader{
			seg.store,
			0,
		}
		readers[i] = o
	}
	return io.MultiReader(readers...)
}

type originReader struct {
	*store
	off int64
}

func (o *originReader) Read(p []byte) (int, error) {
	n, err := o.ReadAt(p, o.off)
	o.off += int64(n)
	return n, err
}
