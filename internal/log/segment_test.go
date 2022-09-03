package log

import (
	//"io"
	"io"
	"io/ioutil"
	"os"
	"testing"

	api "github.com/KartikeyaPophali/dclog/api/v1"
	"github.com/stretchr/testify/require"
)

func TestSegment(t *testing.T) {
	dir, _ := ioutil.TempDir("", "segment-test")
	defer os.RemoveAll(dir)

	record := &api.Record{Value: []byte("hello world")}

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entWidth * 3

	// test newSegment()
	seg, err := newSegment(dir, 15, c)
	require.NoError(t, err)
	require.Equal(t, uint64(15), seg.nextOffset)
	require.False(t, seg.IsMaxed())

	// test Append()
	for i := 0; i < 3; i++ {
		off, err := seg.Append(record)
		require.NoError(t, err)
		require.Equal(t, seg.baseOffset+uint64(i), off)

		returnedRecord, err := seg.Read(seg.baseOffset+uint64(i))
		require.NoError(t, err)
		require.Equal(t, record.Value, returnedRecord.Value)
	}

	// check IsMaxed()
	_, err = seg.Append(record)
	require.Error(t, io.EOF, err)
	require.True(t, seg.IsMaxed())

	// check persistence
	c.Segment.MaxIndexBytes = 1024
	c.Segment.MaxStoreBytes = uint64((lenWidth + len(record.Value)) * 3) 
	seg, err = newSegment(dir, 15, c)	// should load same segment as above given same parameters
	require.NoError(t, err)
	require.True(t, seg.IsMaxed())	
	
	// check Remove()
	err = seg.Remove()
	require.NoError(t, err)
	seg, err = newSegment(dir, 15, c)
	require.NoError(t, err)
	require.False(t, seg.IsMaxed())
}