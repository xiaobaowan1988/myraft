package raft

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestFileWALRoundTripAndTornTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.wal")
	w, err := OpenFileWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	recs := []Record{
		{Kind: recMeta, Term: 2, VotedFor: 1},
		{Kind: recAppend, Index: 1, Entries: []LogEntry{{Term: 1, Command: "a"}, {Term: 1, Command: 7}, {Term: 2}}},
		{Kind: recAppend, Index: 2, Entries: []LogEntry{{Term: 2, Command: "b"}}}, // overwrites 2..3
	}
	if err := w.Append(recs); err != nil {
		t.Fatal(err)
	}
	w.Close()

	// Simulate a torn write: append half a frame of garbage.
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	f.Write([]byte{0, 0, 0, 50, 1, 2, 3, 4, 9, 9})
	f.Close()

	w, err = OpenFileWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := w.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(recs) {
		t.Fatalf("loaded %d records, want %d", len(got), len(recs))
	}
	term, voted, log, err := replay(got)
	if err != nil {
		t.Fatal(err)
	}
	want := []LogEntry{{}, {Term: 1, Command: "a"}, {Term: 2, Command: "b"}}
	if term != 2 || voted != 1 || !reflect.DeepEqual(log, want) {
		t.Fatalf("replay: term=%d voted=%d log=%v", term, voted, log)
	}

	// The torn tail was truncated, so a new append must be readable.
	if err := w.Append([]Record{{Kind: recMeta, Term: 3, VotedFor: -1}}); err != nil {
		t.Fatal(err)
	}
	w.Close()
	w, _ = OpenFileWAL(path)
	defer w.Close()
	got, _ = w.Load()
	if len(got) != 4 || got[3].Term != 3 {
		t.Fatalf("after re-append: %v", got)
	}
}
