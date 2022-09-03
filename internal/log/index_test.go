package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	f, err := ioutil.TempFile(os.TempDir(), "index_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	c := Config{}
	c.Segment.MaxIndexBytes = 1024
	idx, err := newIndex(f, c)
	require.NoError(t, err)
	require.Equal(t, f.Name(), idx.Name())

	_, _, err = idx.Read(-1)
	require.Error(t, err)

	entries := []struct {
		Off uint32
		Pos uint64
	}{
		{Off: 0, Pos: 0},
		{Off: 1, Pos: 10},
	}
	for _, entry := range entries {
		err = idx.Write(entry.Off, entry.Pos)
		require.NoError(t, err)
		_, pos, err := idx.Read(int64(entry.Off))
		require.NoError(t, err)
		require.Equal(t, entry.Pos, pos)
	}

	// index Read should return error when reading past existing entries
	_, _, err = idx.Read(int64(len(entries)))
	require.Equal(t, io.EOF, err)
	_ = idx.Close()

	// TODO: index should build its state from existing file
}
