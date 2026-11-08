package cnc

import (
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestCommandRoundTrip(t *testing.T) {
	for _, c := range []Command{
		{Type: CmdAddPublication, ClientID: 1, CorrelationID: 2, StreamID: -7, Channel: "ipc"},
		{Type: CmdAddSubscription, ClientID: 3, CorrelationID: 4, StreamID: 99, Channel: ""},
		{Type: CmdRemovePublication, ClientID: 5, CorrelationID: 6, RegistrationID: 1 << 40},
		{Type: CmdRemoveSubscription, ClientID: 7, CorrelationID: 8, RegistrationID: 9},
		{Type: CmdClientKeepalive, ClientID: 10},
		{Type: CmdClientClose, ClientID: 11},
	} {
		got, err := DecodeCommand(c.Type, EncodeCommand(c))
		if err != nil || !reflect.DeepEqual(got, c) {
			t.Errorf("round trip of %+v gave %+v, err=%v", c, got, err)
		}
	}
}

func TestEventRoundTrip(t *testing.T) {
	for _, e := range []Event{
		{Type: EvPublicationReady, CorrelationID: 1, RegistrationID: 1, SessionID: -5, StreamID: 6, CounterID: 7, Name: "pub-1.log"},
		{Type: EvAvailableImage, CorrelationID: 2, RegistrationID: 3, SessionID: 4, StreamID: 5, CounterID: 6, Name: "x"},
		{Type: EvUnavailableImage, CorrelationID: 8, RegistrationID: 9, StreamID: 10},
		{Type: EvOperationSuccess, CorrelationID: 11},
		{Type: EvError, CorrelationID: 12, Code: ErrCodeInvalidChannel, Message: "no such channel"},
	} {
		got, err := DecodeEvent(e.Type, EncodeEvent(e))
		if err != nil || !reflect.DeepEqual(got, e) {
			t.Errorf("round trip of %+v gave %+v, err=%v", e, got, err)
		}
	}
}

// Messages come from other processes, so decoding must reject anything malformed
// without panicking: every truncation of a valid message, and random garbage.
func TestDecodeNeverPanicsOnBadInput(t *testing.T) {
	cmds := []int32{CmdAddPublication, CmdRemovePublication, CmdAddSubscription, CmdRemoveSubscription, CmdClientKeepalive, CmdClientClose, 0, 99, -1}
	evs := []int32{EvPublicationReady, EvOperationSuccess, EvError, EvAvailableImage, EvUnavailableImage, 0, 999}
	valid := EncodeCommand(Command{Type: CmdAddPublication, ClientID: 1, CorrelationID: 2, StreamID: 3, Channel: "ipc"})
	for n := 0; n < len(valid); n++ {
		if _, err := DecodeCommand(CmdAddPublication, valid[:n]); err == nil {
			t.Errorf("a command truncated to %d of %d bytes was accepted", n, len(valid))
		}
	}
	validEv := EncodeEvent(Event{Type: EvAvailableImage, CorrelationID: 1, Name: "abc"})
	for n := 0; n < len(validEv); n++ {
		if _, err := DecodeEvent(EvAvailableImage, validEv[:n]); err == nil {
			t.Errorf("an event truncated to %d of %d bytes was accepted", n, len(validEv))
		}
	}
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 20000; i++ {
		b := make([]byte, rng.Intn(80))
		rng.Read(b)
		if len(b) >= 28 && rng.Intn(2) == 0 {
			b[24], b[25], b[26], b[27] = 0xff, 0xff, 0xff, 0xff // a huge string length
		}
		DecodeCommand(cmds[rng.Intn(len(cmds))], b)
		DecodeEvent(evs[rng.Intn(len(evs))], b)
	}
}

func TestCreateOpenAndHeartbeat(t *testing.T) {
	dir := t.TempDir()
	l := Layout{ToDriverCapacity: 4096, ToClientsCapacity: 4096, MaxCounters: 8, ClientTimeoutNs: 123, StartNs: 456, PID: 789}
	f, err := Create(dir, l)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := Open(dir); err != ErrNotReady {
		t.Fatalf("Open before Publish gave %v, want ErrNotReady", err)
	}
	ring, _ := f.ToDriver()
	ring.SetConsumerHeartbeat(42)
	f.Publish()
	g, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if g.ClientTimeoutNs() != 123 || g.StartNs() != 456 || g.PID() != 789 {
		t.Fatal("header fields are wrong")
	}
	gr, _ := g.ToDriver()
	if gr.ConsumerHeartbeat() != 42 {
		t.Fatal("the heartbeat is not shared")
	}
	// the sections are really shared
	gr.Write(1, []byte("cmd"))
	var got string
	ring.Read(func(_ int32, p []byte) { got = string(p) }, 1)
	if got != "cmd" {
		t.Fatalf("a command written through one mapping was read as %q", got)
	}
	cm, _ := f.CountersManager()
	id, _ := cm.Allocate(1, 1, "x")
	cm.Counter(id).Set(5)
	cr, _ := g.CountersReader()
	if cr.Counter(id).Get() != 5 {
		t.Fatal("counters are not shared")
	}
	tx, _ := f.NewTransmitter()
	rx, _ := g.NewReceiver()
	tx.Transmit(1, []byte("ev"))
	if _, p, ok := rx.Receive(); !ok || string(p) != "ev" {
		t.Fatal("events are not shared")
	}
}

func TestOpenRejectsDamagedFiles(t *testing.T) {
	dir := t.TempDir()
	if _, err := Open(dir); err == nil {
		t.Fatal("Open of a missing file succeeded")
	}
	f, err := Create(dir, Layout{ToDriverCapacity: 4096, ToClientsCapacity: 4096, MaxCounters: 4})
	if err != nil {
		t.Fatal(err)
	}
	f.Publish()
	f.Close()
	path := filepath.Join(dir, FileName)
	good, _ := os.ReadFile(path)
	cases := map[string]func([]byte){
		"wrong version":      func(b []byte) { b[offVersion] = 9 },
		"wrong section size": func(b []byte) { b[offToDriverLen+1] ^= 1 },
	}
	for name, damage := range cases {
		b := append([]byte(nil), good...)
		damage(b)
		os.WriteFile(path, b, 0o644)
		if g, err := Open(dir); err == nil {
			g.Close()
			t.Errorf("%s: Open succeeded", name)
		}
	}
	os.WriteFile(path, good[:100], 0o644)
	if g, err := Open(dir); err == nil {
		g.Close()
		t.Error("a truncated file was accepted")
	}
}
