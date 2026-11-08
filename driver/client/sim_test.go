package client

import (
	"flag"
	"fmt"
	"hash/fnv"
	"net/netip"
	"path/filepath"
	"testing"
	"time"

	"github.com/xiaobaowan1988/myraft/driver"
	"github.com/xiaobaowan1988/myraft/driver/counters"
	"github.com/xiaobaowan1988/myraft/driver/logbuf"
	"github.com/xiaobaowan1988/myraft/driver/proto"
)

var simSeeds = flag.Int("sim_seeds", 60, "number of seeds in the lossy-network simulation sweep")

// sim is a deterministic world: any number of drivers and clients on an in-memory
// network, all driven from one goroutine in a fixed order under a fake clock.
type sim struct {
	t       *testing.T
	seed    int64
	clock   *fakeClock
	net     *memNet
	step    time.Duration
	drivers []*simDriver
	cfgMod  func(*driver.Config)
	steps   int64
}

type simDriver struct {
	d *driver.MediaDriver
	c *Client
}

func simConfig(c *driver.Config) {
	c.TermLength = 1 << 16
	c.MTU = 1408
	c.PublicationWindow = 1 << 15
	c.ReceiverWindow = 1 << 14
	c.SetupInterval = 5 * time.Millisecond
	c.HeartbeatInterval = 20 * time.Millisecond
	c.ReceiverTimeout = time.Second
	c.ImageLivenessTimeout = 2 * time.Second
}

func newSim(t *testing.T, seed int64, mod func(*driver.Config)) *sim {
	t.Helper()
	return &sim{t: t, seed: seed, clock: newFakeClock(), net: newMemNet(seed), step: 500 * time.Microsecond, cfgMod: mod}
}

func (s *sim) addDriver() *simDriver {
	s.t.Helper()
	cfg := driver.Config{Dir: filepath.Join(s.t.TempDir(), fmt.Sprintf("d%d", len(s.drivers))), DriverTimeout: time.Hour,
		Clock: s.clock.Now, ListenPacket: s.net.Listen, Seed: s.seed*100 + int64(len(s.drivers)) + 1}
	simConfig(&cfg)
	if s.cfgMod != nil {
		s.cfgMod(&cfg)
	}
	d, err := driver.Open(cfg) // not started: the sim runs it
	if err != nil {
		s.t.Fatal(err)
	}
	c, err := Connect(Options{Dir: d.Dir(), Clock: s.clock.Now, ManualConductor: true, Pump: s.advance, DriverTimeout: time.Hour})
	if err != nil {
		s.t.Fatal(err)
	}
	sd := &simDriver{d: d, c: c}
	s.drivers = append(s.drivers, sd)
	s.t.Cleanup(func() { c.Close(); d.Close() })
	return sd
}

// advance moves time forward one step and runs everything once.
func (s *sim) advance() {
	s.steps++
	s.clock.Advance(s.step)
	now := s.clock.Now()
	s.net.advance(now)
	for _, sd := range s.drivers {
		sd.d.Conductor().DoWork(now)
		sd.d.Sender().DoWork(now)
		sd.d.Receiver().DoWork(now)
		sd.c.DoWork()
	}
}

// runUntil advances until cond holds or limit of simulated time passes.
func (s *sim) runUntil(limit time.Duration, cond func() bool) bool {
	end := s.clock.Now() + int64(limit)
	for s.clock.Now() < end {
		if cond() {
			return true
		}
		s.advance()
	}
	return cond()
}

// ctr returns the value of the first counter of the given type, or -1.
func ctr(c *Client, typ int32) int64 {
	v := int64(-1)
	c.Counters().ForEach(func(i counters.Info) {
		if i.TypeID == typ && v == -1 {
			v = i.Value
		}
	})
	return v
}

// invariants checks, at every call, the properties that must hold at every instant
// of a single-stream transfer from pubD to subD. prev carries the last values seen.
type invariants struct {
	t                *testing.T
	pubD, subD       *simDriver
	window, slack    int64
	rcvPos, hwm, sub int64
	sndPos, sndCons  int64
	violations       int
}

func (v *invariants) check(where string) {
	t := v.t
	hwm, rcv := ctr(v.subD.c, driver.CounterRcvHWM), ctr(v.subD.c, driver.CounterRcvPos)
	sub, cons := ctr(v.subD.c, driver.CounterSubPos), ctr(v.subD.c, driver.CounterImageCons)
	sndPos, sndCons, sndLim := ctr(v.pubD.c, driver.CounterSenderPos), ctr(v.pubD.c, driver.CounterSenderCons), ctr(v.pubD.c, driver.CounterSenderLimit)
	fail := func(format string, a ...any) {
		v.violations++
		if v.violations <= 3 {
			t.Errorf("invariant at %s: "+format, append([]any{where}, a...)...)
		}
	}
	if hwm >= 0 {
		if hwm < v.hwm || rcv < v.rcvPos || sub < v.sub {
			fail("a position went backwards: hwm %d->%d rcv %d->%d sub %d->%d", v.hwm, hwm, v.rcvPos, rcv, v.sub, sub)
		}
		if !(sub <= rcv && rcv <= hwm) {
			fail("sub %d <= rcv %d <= hwm %d does not hold", sub, rcv, hwm)
		}
		if cons >= 0 && hwm-cons > v.window+v.slack {
			fail("the receiver holds %d bytes beyond its consumption %d; window %d (+%d for padding)", hwm-cons, cons, v.window, v.slack)
		}
		v.hwm, v.rcvPos, v.sub = max(v.hwm, hwm), max(v.rcvPos, rcv), max(v.sub, sub)
	}
	if sndPos >= 0 && sndLim >= 0 {
		if sndPos < v.sndPos {
			fail("sender position went backwards: %d -> %d", v.sndPos, sndPos)
		}
		if sndLim > 0 && sndPos > sndLim+v.slack {
			fail("sender position %d is beyond its limit %d (+%d for padding)", sndPos, sndLim, v.slack)
		}
		if sndCons > sndPos {
			fail("the receiver is reported to have consumed %d, past what was sent (%d)", sndCons, sndPos)
		}
		v.sndPos = max(v.sndPos, sndPos)
	}
}

// transfer publishes n self-describing messages from a to b over the network and
// returns when all are delivered (or the simulated time limit passes). It checks the
// invariants throughout and that delivery is complete, ordered and exactly once.
type transferResult struct {
	ok         bool
	simTime    time.Duration
	sender     driver.SenderStats
	receiver   driver.ReceiverStats
	net        memNet
	traceHash  uint64
	violations int
}

func runTransfer(t *testing.T, seed int64, drop, dup float64, maxDelay time.Duration, n int, mod func(*driver.Config)) transferResult {
	t.Helper()
	s := newSim(t, seed, mod)
	s.net.drop, s.net.dup, s.net.maxDelay = drop, dup, int64(maxDelay)
	h := fnv.New64a()
	s.net.trace = func(now int64, from, to netip.AddrPort, b []byte) {
		fmt.Fprintf(h, "%d %v %v %x|", now, from, to, b[:min(len(b), 40)])
	}
	pubD, subD := s.addDriver(), s.addDriver()
	sub, err := subD.c.AddSubscription("udp://127.0.0.1:0", 1)
	if err != nil {
		t.Fatal(err)
	}
	pub, err := pubD.c.AddPublication("udp://"+sub.LocalAddr(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if !s.runUntil(10*time.Second, func() bool { return pub.IsConnected() && sub.ImageCount() == 1 }) {
		t.Fatalf("seed %d: no connection after 10s of simulated time (drop %.2f)", seed, drop)
	}
	inv := &invariants{t: t, pubD: pubD, subD: subD, window: int64(subD.d.Config().ReceiverWindow), slack: int64(pub.MaxMessageLength()) + 1408}
	v := newVerifier(t, 1)
	sent := 0
	start := s.clock.Now()
	deadline := start + int64(300*time.Second)
	for v.total < n && s.clock.Now() < deadline {
		for i := 0; i < 8 && sent < n; i++ {
			if _, res := pub.Offer(makeMsg(0, sent)); res != Offered {
				break
			}
			sent++
		}
		if _, err := sub.Poll(v.handle, 64); err != nil {
			t.Fatal(err)
		}
		s.advance()
		if s.steps%4 == 0 {
			inv.check(fmt.Sprintf("seed %d step %d", seed, s.steps))
		}
	}
	r := transferResult{ok: v.total == n, simTime: time.Duration(s.clock.Now() - start),
		sender: pubD.d.Sender().Stats(), receiver: subD.d.Receiver().Stats(), net: *s.net, traceHash: h.Sum64(), violations: inv.violations}
	if !r.ok {
		t.Errorf("seed %d (drop %.2f dup %.2f): delivered %d of %d messages in %v of simulated time; sender %+v receiver %+v", seed, drop, dup, v.total, n, r.simTime, r.sender, r.receiver)
	}
	return r
}

func TestSimLossyNetworkSweep(t *testing.T) {
	var naks, retrans, suppressed int64
	for seed := int64(1); seed <= int64(*simSeeds); seed++ {
		drop := float64(seed%7) * 0.05 // 0 .. 0.30
		dup := float64(seed%4) * 0.05
		delay := time.Duration(seed%5) * 2 * time.Millisecond
		r := runTransfer(t, seed, drop, dup, delay, 300, nil)
		naks += r.receiver.NaksSent
		retrans += r.sender.RetransmitDatagrams
		suppressed += r.sender.NaksSuppressed
		if t.Failed() {
			t.Fatalf("stopping at seed %d (reproduce with -run TestSimLossyNetworkSweep -sim_seeds=%d)", seed, seed)
		}
	}
	t.Logf("%d seeds: %d NAKs sent, %d datagrams retransmitted, %d repeated NAKs suppressed", *simSeeds, naks, retrans, suppressed)
	if naks == 0 || retrans == 0 {
		t.Fatalf("no loss recovery happened (%d NAKs, %d retransmissions): the simulation is not exercising loss", naks, retrans)
	}
}

// The same seed must give the same run, bit for bit.
func TestSimIsDeterministic(t *testing.T) {
	a := runTransfer(t, 42, 0.2, 0.1, 6*time.Millisecond, 200, nil)
	b := runTransfer(t, 42, 0.2, 0.1, 6*time.Millisecond, 200, nil)
	if a.traceHash != b.traceHash || a.simTime != b.simTime || a.sender != b.sender || a.receiver != b.receiver {
		t.Fatalf("two runs of one seed differ:\n%+v\n%+v", a, b)
	}
	c := runTransfer(t, 43, 0.2, 0.1, 6*time.Millisecond, 200, nil)
	if c.traceHash == a.traceHash {
		t.Fatal("different seeds gave the same run")
	}
}

var _ = proto.TypeNak
var _ = logbuf.HeaderLength
