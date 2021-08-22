package log

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/tysontate/gommap"
)

// Index entries contain two fields: the record's offset
// and its position in the store file.
var (
	offsetWidth = 4
	posWidth    = 8
	entryWidth  = offsetWidth + posWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

// newIndex creates an index for the given file. It immediately
// saves the file's current size to track the amount of data
// in the file as more entries are added. The file grows to the max
// index size before memory-mapping the file and returning
// the created index to the caller.
func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}

	file, err := os.Stat(f.Name())

	if err != nil {
		return nil, err
	}

	idx.size = uint64(file.Size())

	// Grow the file to max index size for mmap.
	// We can't resize it after we mmap the file.
	// This will add some unknown amount of space
	// between the last entry and the file's end.
	if err := os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}

	if idx.mmap, err = gommap.Map(
		idx.file.Fd(), gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED); err != nil {
		return nil, err
	}

	return idx, nil
}

func (i *index) Close() error {
	// Syncs the changes made to this memory-mapped region to the persisted file
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	// This calls the fsync syscall which will force the file system
	// to flush it's buffers to disk. This guarantees that the data is on disk
	// even if the system is powered down or the OS crashes.
	if err := i.file.Sync(); err != nil {
		return err
	}

	// Truncate the file that we grew in newIndex.
	// This means removing the empty space between
	// the max file size and the last entry. This will let
	// the service find the last entry of the index when restarting.
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}

	return i.file.Close()
}

// Takes in an offset, and returns the associated record's position in the store
// The offset is relative to the segment's base offset
// The offset is 4B because it saves space - significant once there are a lot of records.
func (i *index) Read(offset int64) (output uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	if offset == -1 {
		// this is called in newSegment to get the next offset
		// jump to the last offset
		output = uint32((i.size / uint64(entryWidth)) - 1)
	} else {
		output = uint32(offset)
	}

	pos = uint64(output) * uint64(entryWidth)

	if i.size < pos+uint64(entryWidth) {
		return 0, 0, io.EOF
	}

	output = binary.BigEndian.Uint32(i.mmap[pos : pos+uint64(offsetWidth)])
	pos = binary.BigEndian.Uint64(i.mmap[pos+uint64(offsetWidth) : pos+uint64(entryWidth)])

	return output, pos, nil
}

// Write appends the given offset and position to the index.
// first validates that there is enough space to the write
// the entry. If so, the offset and position are encoded and
// written to the memory-mapped file. Lastly, it increments
// the position where the next write will go.
func (i *index) Write(offset uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.size+uint64(entryWidth) {
		return io.EOF
	}

	binary.BigEndian.PutUint32(i.mmap[i.size:i.size+uint64(offsetWidth)], offset)
	binary.BigEndian.PutUint64(i.mmap[i.size+uint64(offsetWidth):i.size+uint64(entryWidth)], pos)

	i.size += uint64(entryWidth)

	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}
