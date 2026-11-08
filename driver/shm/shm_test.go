package shm

import (
	"path/filepath"
	"testing"
)

func mustPanic(t *testing.T, what string, f func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Errorf("%s did not panic", what)
		}
	}()
	f()
}

func TestBufferChecksBoundsAndAlignment(t *testing.T) {
	b := Wrap(Heap(64))
	b.PutInt64(56, 7) // last valid slot
	mustPanic(t, "int64 past the end", func() { b.PutInt64(64, 1) })
	mustPanic(t, "int64 straddling the end", func() { b.Int64(60) })
	mustPanic(t, "negative offset", func() { b.Int32(-4) })
	mustPanic(t, "misaligned int64", func() { b.Int64(4) })
	mustPanic(t, "misaligned int32", func() { b.PutInt32(2, 1) })
	mustPanic(t, "copy past the end", func() { b.PutBytes(60, make([]byte, 8)) })
	mustPanic(t, "slice past the end", func() { b.Slice(32, 40) })
	mustPanic(t, "misaligned slice", func() { b.Slice(4, 8) })
	mustPanic(t, "unaligned wrap", func() { Wrap(Heap(32)[1:]) })
}

func TestBufferAtomics(t *testing.T) {
	b := Wrap(Heap(64))
	b.PutInt64Ordered(0, 5)
	if got := b.AddInt64(0, 3); got != 5 || b.Int64Volatile(0) != 8 {
		t.Fatalf("AddInt64 returned %d, value now %d; want it to return the old value 5 and leave 8", got, b.Int64Volatile(0))
	}
	if b.CompareAndSetInt64(0, 7, 1) || !b.CompareAndSetInt64(0, 8, 1) || b.Int64(0) != 1 {
		t.Fatal("CompareAndSetInt64 is wrong")
	}
	if got := b.AddInt32(8, 2); got != 0 || b.Int32Volatile(8) != 2 {
		t.Fatal("AddInt32 is wrong")
	}
	b.PutBytes(16, []byte("hello"))
	got := make([]byte, 5)
	b.GetBytes(16, got)
	if string(got) != "hello" {
		t.Fatal("bytes round trip failed")
	}
	b.Zero(16, 5)
	b.GetBytes(16, got)
	if string(got) != "\x00\x00\x00\x00\x00" {
		t.Fatal("Zero left data behind")
	}
}

func TestMappingSharedBetweenTwoMappings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f")
	a, err := Create(path, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if _, err := Create(path, 4096); err == nil {
		t.Fatal("Create over an existing file succeeded")
	}
	b, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	a.Buffer().PutInt64Ordered(128, 42)
	if got := b.Buffer().Int64Volatile(128); got != 42 {
		t.Fatalf("second mapping sees %d, want 42", got)
	}
	if a.Buffer().Len() != 4096 || b.Buffer().Len() != 4096 {
		t.Fatal("wrong mapping size")
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := Open(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("Open of a missing file succeeded")
	}
}
