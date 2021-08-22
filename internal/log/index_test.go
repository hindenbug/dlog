package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	f, err := ioutil.TempFile("", "index_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	c := Config{}
	c.Segment.MaxIndexBytes = 1024
	idx, err := newIndex(f, c)
	require.NoError(t, err)

	// Reading an empty index file
	_, _, err = idx.Read(-1)
	require.Error(t, err)
	require.Equal(t, f.Name(), idx.Name())

	entries := []struct {
		Offset   uint32
		Position uint64
	}{
		{Offset: 0, Position: 0},
		{Offset: 1, Position: 10},
		{Offset: 2, Position: 20},
	}

	// Each entry is iterated over and written to the index.
	// Reading the entry is then checked with Read() method.
	for _, entry := range entries {
		err = idx.Write(entry.Offset, entry.Position)
		require.NoError(t, err)

		_, pos, err := idx.Read(int64(entry.Offset))
		require.NoError(t, err)
		require.Equal(t, entry.Position, pos)
	}

	// index and scanner should error when reading past existing entries
	_, _, err = idx.Read(int64(len(entries)))
	require.Equal(t, io.EOF, err)
	_ = idx.Close()

	// index should build its state from the existing file
	f, _ = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	idx, err = newIndex(f, c)
	require.NoError(t, err)

	// Reading the last offset/position
	off, pos, err := idx.Read(-1)
	require.NoError(t, err)
	require.Equal(t, uint32(2), off)
	require.Equal(t, entries[2].Position, pos)
}
