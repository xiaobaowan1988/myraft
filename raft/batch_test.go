package raft

import (
	"strings"
	"testing"
	"time"
)

// leaderWithLog builds a leader core of a 3-node cluster whose log holds n
// entries, each carrying a command of cmdSize bytes, and whose follower 1 is
// already replicating (so every sendBatch is allowed).
func leaderWithLog(t *testing.T, n, cmdSize int) *core {
	t.Helper()
	now := time.Unix(1_700_000_000, 0)
	c := newCore(0, 3, 1, 0, []LogEntry{{}}, 1, func() time.Time { return now })
	c.becomeLeader()
	c.takeReady()
	cmd := strings.Repeat("x", cmdSize)
	for i := 0; i < n; i++ {
		c.appendLocal(LogEntry{Term: 1, Command: cmd})
	}
	c.takeReady()
	c.probing[1] = false
	c.nextIndex[1] = 1
	c.pending[1] = nil
	return c
}

// batches drives sendBatch until follower 1 has everything and returns the
// batches' entries.
func batches(c *core) [][]LogEntry {
	var out [][]LogEntry
	for c.nextIndex[1] < len(c.log) {
		before := len(c.rd.Messages)
		c.sendBatch(1)
		out = append(out, c.rd.Messages[before].Entries)
	}
	return out
}

func estimatedBytes(es []LogEntry) int {
	n := 0
	for _, e := range es {
		n += entryOverhead + EntrySize(e.Command)
	}
	return n
}

func withBatchLimits(t *testing.T, entries, bytes int) {
	oldE, oldB := MaxAppendEntries, MaxAppendBytes
	MaxAppendEntries, MaxAppendBytes = entries, bytes
	t.Cleanup(func() { MaxAppendEntries, MaxAppendBytes = oldE, oldB })
}

func TestBatchesAreCappedByBytes(t *testing.T) {
	withBatchLimits(t, 0, 2500) // no count cap
	c := leaderWithLog(t, 9, 1000)
	total := 0
	for i, es := range batches(c) {
		total += len(es)
		if len(es) > 1 && estimatedBytes(es) > 2500 {
			t.Errorf("batch %d: %d entries, %d bytes estimated, over the 2500-byte cap", i, len(es), estimatedBytes(es))
		}
	}
	if total != 10 { // the leader's no-op plus the 9 entries
		t.Errorf("batches carried %d entries in total, want 10", total)
	}
}

func TestBatchAlwaysCarriesAtLeastOneEntry(t *testing.T) {
	withBatchLimits(t, 0, 100) // far smaller than one entry
	c := leaderWithLog(t, 3, 5000)
	got := batches(c)
	for i, es := range got {
		if len(es) != 1 {
			t.Errorf("batch %d carries %d entries, want exactly 1 (an oversized entry must still go out alone)", i, len(es))
		}
	}
	if len(got) != 4 { // no-op + 3 entries
		t.Errorf("%d batches, want 4", len(got))
	}
}

func TestEntryCountCapStillApplies(t *testing.T) {
	withBatchLimits(t, 3, 1<<20) // tiny commands, generous byte cap, 3 entries per batch
	c := leaderWithLog(t, 9, 1)
	for i, es := range batches(c) {
		if len(es) > 3 {
			t.Errorf("batch %d carries %d entries, over the 3-entry cap", i, len(es))
		}
	}
}
