package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

const (
	// lenWidth determines how many bytes will be used to store the length of the record.
	limit = 8
)

type store struct {
	// type embedding of an os file.
	*os.File

	mu     sync.Mutex
	buffer *bufio.Writer
	size   uint64
}

func newStore(f *os.File) (*store, error) {
	file, err := os.Stat(f.Name())

	if err != nil {
		return nil, err
	}

	size := uint64(file.Size())
	return &store{File: f, size: size, buffer: bufio.NewWriter(f)}, nil

}

// Append writes the provided bytes as a record to the end of the store.
// Returns the size fo the record and the position of the record within the store.
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size

	// write the binary representation of the data into the bufio.Writer
	// we first tell how much data we're going to write, with what encoding format
	if err := binary.Write(s.buffer, binary.BigEndian, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	// Write the Record data
	w, err := s.buffer.Write(p)
	if err != nil {
		return 0, 0, err
	}

	// The number of written bytes is the Record length + the written Record data size
	w += limit
	s.size += uint64(w)

	return uint64(w), pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// flush the buffered writer so we don't read a record that hasn't been flushed to disk yet
	if err := s.buffer.Flush(); err != nil {
		return nil, err
	}
	// The amount of bytes needed to read the whole record are determined
	size := make([]byte, limit)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	// Read the actual record data given its offset and size.
	b := make([]byte, binary.BigEndian.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos+limit)); err != nil {
		return nil, err
	}

	return b, nil
}

func (s *store) ReadAt(p []byte, offset int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buffer.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(p, offset)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.buffer.Flush()
	if err != nil {
		return err
	}

	return s.File.Close()
}
