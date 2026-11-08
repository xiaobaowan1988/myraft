//go:build linux

package shm

import (
	"fmt"
	"os"
	"syscall"
)

// Mapping is a file mapped into memory with MAP_SHARED: every process that maps the
// same file sees the same bytes.
type Mapping struct {
	f    *os.File
	data []byte
}

// Create makes a new file of exactly size bytes (zero-filled) and maps it. It fails
// if the file already exists, so a stale file is never silently reused.
func Create(path string, size int) (*Mapping, error) {
	if size <= 0 {
		return nil, fmt.Errorf("shm: bad size %d", size)
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}
	if err := f.Truncate(int64(size)); err != nil {
		f.Close()
		os.Remove(path)
		return nil, err
	}
	return mapFile(f, size)
}

// Open maps an existing file at its current size.
func Open(path string) (*Mapping, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if st.Size() == 0 {
		f.Close()
		return nil, fmt.Errorf("shm: %s is empty", path)
	}
	return mapFile(f, int(st.Size()))
}

func mapFile(f *os.File, size int) (*Mapping, error) {
	b, err := syscall.Mmap(int(f.Fd()), 0, size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("shm: mmap: %w", err)
	}
	return &Mapping{f: f, data: b}, nil
}

// Buffer returns the mapped memory.
func (m *Mapping) Buffer() Buffer { return Wrap(m.data) }

// Close unmaps the file. Every Buffer taken from it becomes invalid: touching one
// afterwards faults the process, so owners must stop using them first.
func (m *Mapping) Close() error {
	if m.data == nil {
		return nil
	}
	err := syscall.Munmap(m.data)
	m.data = nil
	if cerr := m.f.Close(); err == nil {
		err = cerr
	}
	return err
}
