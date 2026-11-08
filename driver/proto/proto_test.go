package proto

import (
	"errors"
	"math/rand"
	"testing"

	"github.com/xiaobaowan1988/myraft/driver/logbuf"
	"github.com/xiaobaowan1988/myraft/driver/shm"
)

func TestRoundTrips(t *testing.T) {
	s := Setup{SessionID: -1, StreamID: 2, TermID: 3, TermOffset: 64, InitialTermID: -9, TermLength: 1 << 16, MTU: 1408}
	if got, err := ParseSetup(EncodeSetup(s)); err != nil || got != s {
		t.Errorf("Setup: %+v, %v", got, err)
	}
	st := Status{SessionID: 1, StreamID: 2, TermID: 3, TermOffset: 128, Window: 1 << 17}
	if got, err := ParseStatus(EncodeStatus(st)); err != nil || got != st {
		t.Errorf("Status: %+v, %v", got, err)
	}
	n := Nak{SessionID: 1, StreamID: 2, TermID: 3, TermOffset: 256, Length: 1408}
	if got, err := ParseNak(EncodeNak(n)); err != nil || got != n {
		t.Errorf("Nak: %+v, %v", got, err)
	}
	h := Heartbeat{SessionID: 1, StreamID: 2, TermID: 3, TermOffset: 32}
	if got, err := ParseHeartbeat(EncodeHeartbeat(h)); err != nil || got != h {
		t.Errorf("Heartbeat: %+v, %v", got, err)
	}
}

func TestControlFramesAreStrict(t *testing.T) {
	good := map[string][]byte{
		"setup":     EncodeSetup(Setup{TermLength: 4096, MTU: 256}),
		"status":    EncodeStatus(Status{Window: 100}),
		"nak":       EncodeNak(Nak{Length: 32}),
		"heartbeat": EncodeHeartbeat(Heartbeat{}),
	}
	parse := map[string]func([]byte) error{
		"setup":     func(b []byte) error { _, err := ParseSetup(b); return err },
		"status":    func(b []byte) error { _, err := ParseStatus(b); return err },
		"nak":       func(b []byte) error { _, err := ParseNak(b); return err },
		"heartbeat": func(b []byte) error { _, err := ParseHeartbeat(b); return err },
	}
	for name, b := range good {
		if err := parse[name](b); err != nil {
			t.Errorf("%s: %v", name, err)
		}
		for n := 0; n < len(b); n++ {
			if err := parse[name](b[:n]); !errors.Is(err, ErrMalformed) {
				t.Errorf("%s truncated to %d bytes: %v", name, n, err)
			}
		}
		if err := parse[name](append(append([]byte(nil), b...), 0)); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s with a trailing byte: %v", name, err)
		}
		bad := append([]byte(nil), b...)
		bad[4] = 7 // version
		if err := parse[name](bad); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s with a bad version: %v", name, err)
		}
		bad = append([]byte(nil), b...)
		bad[0]++ // length field disagrees with the size
		if err := parse[name](bad); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s with a wrong length field: %v", name, err)
		}
		// a frame of one type is not accepted as another
		for other, f := range parse {
			if other != name {
				if err := f(b); err == nil {
					t.Errorf("a %s was accepted as a %s", name, other)
				}
			}
		}
	}
	if _, err := ParseNak(EncodeNak(Nak{Length: 0})); err == nil {
		t.Error("a NAK for 0 bytes was accepted")
	}
	if _, err := ParseStatus(EncodeStatus(Status{Window: -1})); err == nil {
		t.Error("a negative window was accepted")
	}
}

// A frame straight out of a log buffer parses with this package's header decoder:
// the two layouts are the same.
func TestLogFramesParseAsProtocolFrames(t *testing.T) {
	l, err := logbuf.Create(shm.Wrap(shm.Heap(logbuf.FileLength(4096))), logbuf.Params{TermLength: 4096, MTU: 256, InitialTermID: 5, SessionID: 6, StreamID: 7})
	if err != nil {
		t.Fatal(err)
	}
	l.Offer([]byte("hello"), 1<<40)
	frames, _ := l.Scan(0, 256)
	h, err := CheckData(frames)
	if err != nil {
		t.Fatal(err)
	}
	if h.Type != TypeData || h.SessionID != 6 || h.StreamID != 7 || h.TermID != 5 || h.TermOffset != 0 || int(h.Length) != logbuf.HeaderLength+5 || h.Flags != logbuf.FlagUnfragmented {
		t.Fatalf("header %+v", h)
	}
}

func TestCheckData(t *testing.T) {
	l, _ := logbuf.Create(shm.Wrap(shm.Heap(logbuf.FileLength(4096))), logbuf.Params{TermLength: 4096, MTU: 256, SessionID: 1, StreamID: 2})
	for i := 0; i < 3; i++ {
		l.Offer(make([]byte, 40), 1<<40)
	}
	frames, _ := l.Scan(0, 256)
	if _, err := CheckData(frames); err != nil {
		t.Fatalf("three frames in one datagram: %v", err)
	}
	for n := 1; n < len(frames); n++ {
		// truncations that cut a frame short must be rejected; truncating at a frame
		// boundary, or inside the alignment padding after a complete frame, still
		// leaves whole frames (each is 72 bytes, padded to 96)
		_, err := CheckData(frames[:n])
		boundary := n%96 == 0 || n%96 >= 72
		if (err == nil) != boundary {
			t.Errorf("truncation to %d bytes: err=%v, boundary=%v", n, err, boundary)
		}
	}
	bad := append([]byte(nil), frames...)
	bad[96+16]++ // second frame: another stream
	if _, err := CheckData(bad); err == nil {
		t.Error("frames of different streams in one datagram were accepted")
	}
	bad = append([]byte(nil), frames...)
	bad[6] = TypeSetup // a control type inside a data datagram
	if _, err := CheckData(bad); err == nil {
		t.Error("a control frame inside a data datagram was accepted")
	}
	bad = append([]byte(nil), frames...)
	bad[0], bad[1], bad[2], bad[3] = 0, 0, 0, 0
	if _, err := CheckData(bad); err == nil {
		t.Error("a zero-length frame was accepted")
	}
}

func TestParsersNeverPanic(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for i := 0; i < 50000; i++ {
		b := make([]byte, rng.Intn(100))
		rng.Read(b)
		if len(b) >= 8 && rng.Intn(2) == 0 {
			b[4] = Version
		}
		ParseHeader(b)
		ParseSetup(b)
		ParseStatus(b)
		ParseNak(b)
		ParseHeartbeat(b)
		CheckData(b)
		Kind(b)
	}
}

func TestDataSpan(t *testing.T) {
	l, _ := logbuf.Create(shm.Wrap(shm.Heap(logbuf.FileLength(4096))), logbuf.Params{TermLength: 4096, MTU: 256})
	for i := 0; i < 15; i++ {
		l.Offer(make([]byte, 200), 1<<40) // 256 bytes each
	}
	l.Offer(make([]byte, 100), 1<<40) // 160; 96 bytes left in the term
	l.Offer(make([]byte, 150), 1<<40) // does not fit: pads 96
	frames, adv := l.Scan(15*256+160, 256)
	if len(frames) != logbuf.HeaderLength || adv != 96 {
		t.Fatalf("setup: scan returned %d bytes, advance %d", len(frames), adv)
	}
	if _, err := CheckData(frames); err != nil {
		t.Fatal(err)
	}
	if DataSpan(frames) != 96 {
		t.Fatalf("a padding header spans %d bytes, want 96", DataSpan(frames))
	}
	two, adv2 := l.Scan(0, 512)
	if DataSpan(two) != adv2 || adv2 != 512 {
		t.Fatalf("two frames: span %d, advance %d", DataSpan(two), adv2)
	}
}
