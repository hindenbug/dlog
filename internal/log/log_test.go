package log

import (
	"io/ioutil"
	"os"
	"testing"

	api "github.com/hindenbug/dlog/api/log/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, log *Log){
		"append and read a record succeeds": testAppendRead,
		"init with existing segments":       testInitExisting,
		"reader":                            testReader,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := ioutil.TempDir("", "store-test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)

			c := Config{}
			c.Segment.MaxStoreBytes = 32
			log, err := NewLog(dir, c)
			require.NoError(t, err)

			fn(t, log)
		})
	}
}

func testAppendRead(t *testing.T, log *Log) {
	apnd := &api.Record{
		Value: []byte("hello world"),
	}
	off, err := log.Append(apnd)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	read, err := log.Read(off)
	require.NoError(t, err)
	require.Equal(t, apnd.Value, read.Value)
}

func testInitExisting(t *testing.T, log *Log) {
	apnd := &api.Record{
		Value: []byte("hello world"),
	}

	for i := 0; i < 3; i++ {
		_, err := log.Append(apnd)
		require.NoError(t, err)
	}
	require.NoError(t, log.Close())

	// Create a new log from the directory and config of the old one.
	_, err := NewLog(log.Dir, log.Config)
	require.NoError(t, err)
}

func testReader(t *testing.T, log *Log) {
	apnd := &api.Record{
		Value: []byte("hello world"),
	}

	off, err := log.Append(apnd)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	reader := log.Reader()
	b, err := ioutil.ReadAll(reader)
	require.NoError(t, err)

	read := &api.Record{}
	// Store writes the length as a prefix to the binary content so we have to skip it.
	err = proto.Unmarshal(b[limit:], read)
	require.NoError(t, err)
	require.Equal(t, apnd.Value, read.Value)
}
