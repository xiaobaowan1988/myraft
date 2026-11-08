package raft

import (
	"bytes"
	"errors"
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

// sampleRecords is a WAL with several frames, the last one distinguishable.
func sampleRecords() []Record {
	return []Record{
		{Kind: recMeta, Term: 1, VotedFor: 2},
		{Kind: recAppend, Index: 1, Entries: []LogEntry{{Term: 1, Command: "alpha"}, {Term: 1, Command: 7}}},
		{Kind: recMeta, Term: 2, VotedFor: -1},
		{Kind: recAppend, Index: 3, Entries: []LogEntry{{Term: 2, Command: "omega"}}},
	}
}

func writeWAL(t *testing.T, path string, recs []Record) (sizes []int) {
	t.Helper()
	var all []byte
	for _, r := range recs {
		b, err := encodeFrames([]Record{r})
		if err != nil {
			t.Fatal(err)
		}
		sizes = append(sizes, len(b))
		all = append(all, b...)
	}
	if err := os.WriteFile(path, all, 0o644); err != nil {
		t.Fatal(err)
	}
	return sizes
}

func loadWAL(t *testing.T, path string) ([]Record, error) {
	t.Helper()
	w, err := OpenFileWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	return w.Load()
}

// A crash in the middle of the last Append leaves a prefix of its frame. Whatever
// the prefix length, everything before it must come back, the torn bytes must be
// gone, and the log must be appendable again.
func TestFileWALTornTailAtEveryOffset(t *testing.T) {
	recs := sampleRecords()
	dir := t.TempDir()
	path := filepath.Join(dir, "wal")
	sizes := writeWAL(t, path, recs)
	full, _ := os.ReadFile(path)
	lastStart := len(full) - sizes[len(sizes)-1]

	for cut := lastStart; cut < len(full); cut++ { // from "last frame not written at all" to "one byte short"
		if err := os.WriteFile(path, full[:cut], 0o644); err != nil {
			t.Fatal(err)
		}
		w, err := OpenFileWAL(path)
		if err != nil {
			t.Fatal(err)
		}
		got, err := w.Load()
		if err != nil {
			t.Fatalf("cut at %d of %d: %v", cut, len(full), err)
		}
		if !reflect.DeepEqual(got, recs[:len(recs)-1]) {
			t.Fatalf("cut at %d: got %d records, want the %d before the torn one", cut, len(got), len(recs)-1)
		}
		if st, _ := os.Stat(path); int(st.Size()) != lastStart {
			t.Fatalf("cut at %d: file is %d bytes after Load, want it truncated to %d", cut, st.Size(), lastStart)
		}
		extra := Record{Kind: recMeta, Term: 9, VotedFor: 1}
		if err := w.Append([]Record{extra}); err != nil {
			t.Fatal(err)
		}
		w.Close()
		again, err := loadWAL(t, path)
		if err != nil || len(again) != len(recs) || !reflect.DeepEqual(again[len(again)-1], extra) {
			t.Fatalf("cut at %d: after appending to the repaired log, reload gave %d records, err=%v", cut, len(again), err)
		}
	}
}

// Damage anywhere before the last frame, with valid data after it, must be refused,
// not "repaired" by throwing away everything after it.
func TestFileWALMidFileCorruptionIsRefused(t *testing.T) {
	recs := sampleRecords()
	dir := t.TempDir()
	path := filepath.Join(dir, "wal")
	sizes := writeWAL(t, path, recs)
	orig, _ := os.ReadFile(path)
	start1 := sizes[0]
	start2 := start1 + sizes[1]
	// every byte of the 2nd frame (magic, length, checksum, payload)
	for pos := start1; pos < start2; pos++ {
		bad := append([]byte(nil), orig...)
		bad[pos] ^= 0x5a
		if err := os.WriteFile(path, bad, 0o644); err != nil {
			t.Fatal(err)
		}
		w, err := OpenFileWAL(path)
		if err != nil {
			t.Fatal(err)
		}
		_, err = w.Load()
		w.Close()
		var ce *CorruptError
		if !errors.As(err, &ce) {
			t.Fatalf("a flipped byte at %d (inside frame 2 of 4) gave err=%v, want *CorruptError", pos, err)
		}
		if ce.Offset != start1 {
			t.Errorf("flip at %d: corruption reported at %d, want the start of frame 2 (%d)", pos, ce.Offset, start1)
		}
		after, _ := os.ReadFile(path)
		if !bytes.Equal(after, bad) {
			t.Fatalf("flip at %d: Load modified the file although it refused it", pos)
		}
	}
}

// Damage inside the LAST frame cannot be told from a torn write, so it is treated as
// one: the frame is dropped. This is the documented limit of what a log can detect.
func TestFileWALDamageInTheLastFrameCountsAsTorn(t *testing.T) {
	recs := sampleRecords()
	path := filepath.Join(t.TempDir(), "wal")
	sizes := writeWAL(t, path, recs)
	orig, _ := os.ReadFile(path)
	bad := append([]byte(nil), orig...)
	bad[len(bad)-3] ^= 0xff
	os.WriteFile(path, bad, 0o644)
	got, err := loadWAL(t, path)
	if err != nil || len(got) != len(recs)-1 {
		t.Fatalf("got %d records, err=%v; want the last frame dropped as torn", len(got), err)
	}
	if st, _ := os.Stat(path); int(st.Size()) != len(orig)-sizes[len(sizes)-1] {
		t.Errorf("file is %d bytes, want it truncated before the damaged frame", st.Size())
	}
}

// After a failed write the file may end in half a frame; appending behind it would
// turn a torn tail into mid-file damage. So every later Append must fail too.
func TestFileWALRefusesToAppendAfterAFailedWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	w, err := OpenFileWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append([]Record{{Kind: recMeta, Term: 1, VotedFor: 0}}); err != nil {
		t.Fatal(err)
	}
	w.f.Close() // makes the next write fail
	first := w.Append([]Record{{Kind: recMeta, Term: 2, VotedFor: 0}})
	if first == nil {
		t.Fatal("a write to a closed file succeeded")
	}
	if second := w.Append([]Record{{Kind: recMeta, Term: 3, VotedFor: 0}}); second != first {
		t.Fatalf("second Append returned %v, want the same sticky error %v", second, first)
	}
}

// An encoding failure writes nothing, so it must not poison the log.
func TestFileWALEncodingFailureDoesNotPoisonIt(t *testing.T) {
	type unregistered struct{ X int }
	path := filepath.Join(t.TempDir(), "wal")
	w, _ := OpenFileWAL(path)
	defer w.Close()
	if err := w.Append([]Record{{Kind: recAppend, Index: 1, Entries: []LogEntry{{Term: 1, Command: unregistered{1}}}}}); err == nil {
		t.Fatal("an unregistered command type was encoded")
	}
	if err := w.Append([]Record{{Kind: recMeta, Term: 1, VotedFor: 0}}); err != nil {
		t.Fatalf("Append after an encoding failure: %v", err)
	}
	got, err := w.Load()
	if err != nil || len(got) != 1 {
		t.Fatalf("Load gave %d records, err=%v", len(got), err)
	}
}

func TestFileWALCreationAndReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal")
	w, err := OpenFileWAL(path)
	if err != nil {
		t.Fatalf("creating a WAL (including syncing its directory): %v", err)
	}
	w.Append(sampleRecords())
	w.Close()
	got, err := loadWAL(t, path)
	if err != nil || !reflect.DeepEqual(got, sampleRecords()) {
		t.Fatalf("reopen gave %d records, err=%v", len(got), err)
	}
}
