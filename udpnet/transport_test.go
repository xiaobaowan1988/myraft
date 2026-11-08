package udpnet

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xiaobaowan1988/myraft/raft"
)

func TestMessagesRoundTripThroughTheStream(t *testing.T) {
	tn := newTestNet(t, 2, netOpts{})
	want := sampleMessages()
	for i := range want {
		want[i].From, want[i].To = 0, 1
		tn.tr[0].Send(want[i])
	}
	waitFor(t, 5*time.Second, "all messages", func() bool { return tn.rec[1].count() >= len(want) })
	got := tn.rec[1].snapshot()
	if len(got) != len(want) {
		t.Fatalf("%d messages arrived, want %d", len(got), len(want))
	}
	// Order is kept within each stream (control and bulk), not between them.
	for _, c := range []int{streamControl, streamBulk} {
		var w, g []raft.Message
		for _, m := range want {
			if class(m) == c {
				w = append(w, m)
			}
		}
		for _, m := range got {
			if class(m) == c {
				g = append(g, m)
			}
		}
		if len(w) == 0 || !reflect.DeepEqual(w, g) {
			t.Errorf("stream %d: got %d messages %+v\n want %d messages %+v", c, len(g), g, len(w), w)
		}
	}
}

// With a reliable stream under it, there is no per-message size limit: a command
// of hundreds of KiB goes through intact, even over a lossy link.
func TestHugeCommandsAreCarried(t *testing.T) {
	tn := newTestNet(t, 2, netOpts{drop: 0.05, dup: 0.05, maxDelay: 2 * time.Millisecond})
	big := strings.Repeat("0123456789", 40_000) // 400 KB
	m := raft.Message{Type: raft.MsgApp, From: 0, To: 1, Term: 3, Seq: 7, PrevLogIndex: 4, PrevLogTerm: 2,
		Entries: []raft.LogEntry{{Term: 3, Command: big}, {Term: 3, Command: "small"}}}
	tn.tr[0].Send(m)
	waitFor(t, 30*time.Second, "the huge message", func() bool { return tn.rec[1].count() >= 1 })
	if got := tn.rec[1].snapshot()[0]; !reflect.DeepEqual(got, m) {
		t.Error("a 400 KB message arrived altered")
	}
}

func TestSpoofedSenderInsideAStreamIsRefused(t *testing.T) {
	tn := newTestNet(t, 3, netOpts{})
	// Node 0 sends a message that claims to come from node 2.
	tn.tr[0].Send(raft.Message{Type: raft.MsgVoteResp, From: 2, To: 1, Term: 9, Granted: true})
	waitFor(t, 3*time.Second, "the spoof to be refused", func() bool { return tn.tr[1].Stats().DroppedBadMessage >= 1 })
	if n := tn.rec[1].count(); n != 0 {
		t.Fatalf("%d spoofed messages reached the endpoint", n)
	}
	tn.tr[0].Send(small(0, 1, 5))
	waitFor(t, 3*time.Second, "a genuine message", func() bool { return tn.rec[1].count() == 1 })
}

func TestUnencodableCommandIsDroppedAndCounted(t *testing.T) {
	tn := newTestNet(t, 2, netOpts{})
	type unregistered struct{ X int }
	tn.tr[0].Send(raft.Message{Type: raft.MsgApp, From: 0, To: 1, Term: 1, Seq: 1, Entries: []raft.LogEntry{{Term: 1, Command: unregistered{1}}}})
	tn.tr[0].Send(small(0, 1, 2))
	waitFor(t, 3*time.Second, "the next message", func() bool { return tn.rec[1].count() == 1 })
	if st := tn.tr[0].Stats(); st.DroppedEncode != 1 {
		t.Errorf("DroppedEncode = %d, want 1", st.DroppedEncode)
	}
}

// A peer that is down: Raft is told it is unreachable, both when the stream
// notices the silence and when a Send has to be dropped.
func TestDownPeerIsReportedToRaft(t *testing.T) {
	tn := newTestNet(t, 2, netOpts{})
	tn.tr[1].Close()
	waitFor(t, 3*time.Second, "a report from the silence", func() bool { return tn.rec[0].unreachableCount() > 0 })
	if tn.rec[0].unreachable[0] != 1 {
		t.Errorf("reported peer %d, want 1", tn.rec[0].unreachable[0])
	}
	before := tn.rec[0].unreachableCount()
	time.Sleep(300 * time.Millisecond) // quiet: any new report must come from the next step
	tn.tr[0].Send(small(0, 1, 1))      // refused (NotConnected): dropped, and reported at once
	waitFor(t, 2*time.Second, "a report caused by the dropped Send", func() bool { return tn.rec[0].unreachableCount() > before })
	if st := tn.tr[0].Stats(); st.DroppedOffer == 0 {
		t.Error("DroppedOffer is 0: the send was not counted as dropped")
	}
}

func small(from, to int, i uint64) raft.Message {
	return raft.Message{Type: raft.MsgAppResp, From: from, To: to, Term: 1, Seq: i, Success: true, Match: int(i)}
}

// The peer is alive but cannot keep up: the stream back-pressures and Send must drop
// the message. Raft has to be told, at once, even though the peer is not silent;
// that report is the only thing that tells Raft its message did not go out.
func TestDroppedSendBecauseOfBackPressureIsReported(t *testing.T) {
	tn := newTestNet(t, 2, netOpts{tweak: func(i int, c *Config) {
		c.SendBuffer, c.MaxMessage, c.RecvWindow = 8<<10, 2<<10, 8<<10
	}})
	block := make(chan struct{})
	tn.rec[1].setBlock(block) // node 1's Raft is stuck handling its first message
	defer close(block)

	cmd := strings.Repeat("x", 1000)
	for i := 0; i < 50; i++ {
		tn.tr[0].Send(raft.Message{Type: raft.MsgApp, From: 0, To: 1, Term: 1, Seq: uint64(i + 1),
			Entries: []raft.LogEntry{{Term: 1, Command: cmd}}})
	}
	waitFor(t, 3*time.Second, "a report caused by back-pressure", func() bool { return tn.rec[0].unreachableCount() > 0 })
	st := tn.tr[0].Stats()
	if st.DroppedOffer == 0 {
		t.Error("nothing was dropped, so the report cannot have come from a dropped Send")
	}
	if st.Stream.Resets != 0 {
		t.Errorf("the stream was reset %d times: the peer was reported because it was silent, not because of the dropped Send", st.Stream.Resets)
	}
}
