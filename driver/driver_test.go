package driver

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xiaobaowan1988/myraft/driver/cnc"
	"github.com/xiaobaowan1988/myraft/driver/logbuf"
)

func TestConfigValidation(t *testing.T) {
	dir := t.TempDir()
	for name, c := range map[string]Config{
		"no directory":             {},
		"window above half a term": {Dir: dir, TermLength: 1 << 16, PublicationWindow: 1<<15 + 32},
		"bad term length":          {Dir: dir, TermLength: 5000},
		"bad MTU":                  {Dir: dir, TermLength: 1 << 16, MTU: 100},
	} {
		if _, err := Open(c); err == nil {
			t.Errorf("%s: Open succeeded", name)
		}
	}
}

// The conductor is a plain duty cycle: with a clock and a command in the ring, one
// DoWork call produces the event, no goroutines involved.
func TestConductorCanBeSteppedByHand(t *testing.T) {
	now := int64(1_000_000_000)
	d, err := Open(Config{Dir: filepath.Join(t.TempDir(), "d"), TermLength: 1 << 16, MTU: 512, Clock: func() int64 { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	f, err := cnc.Open(d.Dir())
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	ring, _ := f.ToDriver()
	rx, _ := f.NewReceiver()
	if ring.ConsumerHeartbeat() != now {
		t.Fatalf("heartbeat %d at start, want %d", ring.ConsumerHeartbeat(), now)
	}
	ring.Write(cnc.CmdAddPublication, cnc.EncodeCommand(cnc.Command{Type: cnc.CmdAddPublication, ClientID: 1, CorrelationID: 10, StreamID: 3, Channel: "ipc"}))
	if d.Conductor().DoWork(now) == 0 {
		t.Fatal("a duty cycle that handled a command reported no work")
	}
	typ, payload, ok := rx.Receive()
	if !ok || typ != cnc.EvPublicationReady {
		t.Fatalf("got event %d ok=%v", typ, ok)
	}
	ev, _ := cnc.DecodeEvent(typ, payload)
	if ev.CorrelationID != 10 || ev.StreamID != 3 {
		t.Fatalf("event %+v", ev)
	}
	if _, err := os.Stat(filepath.Join(d.Dir(), ev.Name)); err != nil {
		t.Fatalf("the log file named in the event does not exist: %v", err)
	}
	if st := d.Conductor().Stats(); st.Publications != 1 || st.Clients != 1 {
		t.Fatalf("stats %+v", st)
	}
	now += int64(2 * time.Millisecond)
	d.Conductor().DoWork(now)
	if ring.ConsumerHeartbeat() != now {
		t.Fatal("the heartbeat was not refreshed")
	}
}

func TestBackoffIdleResetsOnWork(t *testing.T) {
	b := NewBackoffIdle(2, 2, time.Microsecond, 4*time.Microsecond)
	for i := 0; i < 20; i++ {
		b.Idle(0)
	}
	if b.park != 4*time.Microsecond {
		t.Fatalf("park time %v after a long idle, want the 4µs maximum", b.park)
	}
	b.Idle(1)
	if b.spins != 0 || b.yields != 0 || b.park != time.Microsecond {
		t.Fatalf("work did not reset the strategy: %+v", b)
	}
}

func TestLogbufParamsMatchTheConfig(t *testing.T) {
	c := Config{Dir: t.TempDir()}
	if err := c.setDefaults(); err != nil {
		t.Fatal(err)
	}
	if err := (logbuf.Params{TermLength: c.TermLength, MTU: c.MTU}).Validate(); err != nil {
		t.Fatalf("the default configuration does not make a valid log buffer: %v", err)
	}
	if c.PublicationWindow != c.TermLength/4 {
		t.Fatalf("default window %d, want a quarter of the term", c.PublicationWindow)
	}
}
