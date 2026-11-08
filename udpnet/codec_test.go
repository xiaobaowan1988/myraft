package udpnet

import (
	"math/rand"
	"reflect"
	"testing"

	"github.com/xiaobaowan1988/myraft/raft"
)

func sampleMessages() []raft.Message {
	return []raft.Message{
		{Type: raft.MsgVote, From: 1, To: 2, Term: 7, LastLogIndex: 100, LastLogTerm: 6},
		{Type: raft.MsgVoteResp, From: 2, To: 1, Term: 7, Granted: true},
		{Type: raft.MsgVoteResp, From: 2, To: 1, Term: 8, Granted: false},
		{Type: raft.MsgApp, From: 0, To: 1, Term: 3, Seq: 99, PrevLogIndex: 5, PrevLogTerm: 2, LeaderCommit: 4},
		{Type: raft.MsgApp, From: 0, To: 1, Term: 3, Seq: 1 << 40, PrevLogIndex: 0, LeaderCommit: 0, Entries: []raft.LogEntry{
			{Term: 3},
			{Term: 3, Command: "set x=1"},
			{Term: 3, Command: []byte{0, 1, 2, 255}},
			{Term: 2, Command: 42},
			{Term: 2, Command: -7},
			{Term: 3, Command: true},
			{Term: 3, Command: 3.5},
		}},
		{Type: raft.MsgAppResp, From: 1, To: 0, Term: 3, Seq: 5, Success: true, Match: 12},
		{Type: raft.MsgAppResp, From: 1, To: 0, Term: 3, Seq: 6, Success: false, ConflictIndex: 4, ConflictTerm: -1},
	}
}

func TestMessageRoundTrip(t *testing.T) {
	c := DefaultCodec{}
	for i, m := range sampleMessages() {
		b, err := encodeMessage(m, c)
		if err != nil {
			t.Fatalf("message %d: encode: %v", i, err)
		}
		got, err := decodeMessage(b, c)
		if err != nil {
			t.Fatalf("message %d: decode: %v", i, err)
		}
		if !reflect.DeepEqual(got, m) {
			t.Errorf("message %d:\n got  %+v\n want %+v", i, got, m)
		}
	}
}

type customCmd struct{ A, B int }

func TestGobFallbackNeedsRegistration(t *testing.T) {
	c := DefaultCodec{}
	if _, err := c.Encode(customCmd{1, 2}); err == nil {
		t.Fatal("encoding an unregistered type should fail, not silently succeed")
	}
}

// Truncating a valid message anywhere, appending junk, or flipping bytes must
// yield an error or a different message, and never a panic.
func TestDecodeNeverPanicsOnBadInput(t *testing.T) {
	c := DefaultCodec{}
	rng := rand.New(rand.NewSource(1))
	for _, m := range sampleMessages() {
		b, _ := encodeMessage(m, c)
		for cut := 0; cut < len(b); cut++ {
			if _, err := decodeMessage(b[:cut], c); err == nil {
				t.Errorf("a %d-byte prefix of a %d-byte message decoded without error", cut, len(b))
			}
		}
		if _, err := decodeMessage(append(append([]byte(nil), b...), 0), c); err == nil {
			t.Error("trailing byte accepted")
		}
		for i := 0; i < 300; i++ {
			x := append([]byte(nil), b...)
			x[rng.Intn(len(x))] ^= byte(1 + rng.Intn(255))
			decodeMessage(x, c) // must not panic
		}
	}
	for i := 0; i < 20000; i++ {
		x := make([]byte, rng.Intn(64))
		rng.Read(x)
		decodeMessage(x, c)
	}
}

// A declared count or length far larger than the data must be rejected without
// allocating for it.
func TestDecodeRejectsHugeDeclaredSizes(t *testing.T) {
	c := DefaultCodec{}
	// MsgApp with 2^60 entries declared and no data after the count.
	b := []byte{byte(raft.MsgApp), 0, 2, 2, 1, 0, 0, 0}
	b = append(b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x0f)
	if _, err := decodeMessage(b, c); err == nil {
		t.Fatal("huge entry count accepted")
	}
	// One entry whose command length is 2^60.
	b = []byte{byte(raft.MsgApp), 0, 2, 2, 1, 0, 0, 0, 1, 2}
	b = append(b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x0f)
	if _, err := decodeMessage(b, c); err == nil {
		t.Fatal("huge command length accepted")
	}
}
