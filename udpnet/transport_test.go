package udpnet

import (
	"math/rand"
	"net"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/xiaobaowan1988/myraft/raft"
)

func TestOrderedDeliveryWithoutFaults(t *testing.T) {
	tn := newTestNet(t, 2, netOpts{})
	const n = 1000
	for i := 1; i <= n; i++ {
		tn.tr[0].Send(small(0, 1, uint64(i)))
	}
	waitFor(t, 5*time.Second, "all messages", func() bool { return tn.rec[1].count() >= n })
	for i, m := range tn.rec[1].snapshot() {
		if m.Seq != uint64(i+1) {
			t.Fatalf("message %d has Seq %d: not in order or duplicated", i, m.Seq)
		}
	}
	if st := tn.tr[1].Stats(); st.Skipped != 0 {
		t.Errorf("skipped %d messages on a clean link", st.Skipped)
	}
}

func TestFragmentationOfLargeMessages(t *testing.T) {
	tn := newTestNet(t, 2, netOpts{})
	rng := rand.New(rand.NewSource(7))
	var want []raft.Message
	sizes := []int{0, 1, 1100, 1157, 1156 * 3, 20_000, 30_000, 5, 2000}
	for i, sz := range sizes {
		blob := make([]byte, sz)
		rng.Read(blob)
		m := raft.Message{Type: raft.MsgApp, From: 0, To: 1, Term: 2, Seq: uint64(i + 1), PrevLogIndex: i, PrevLogTerm: 1,
			Entries: []raft.LogEntry{{Term: 2, Command: blob}, {Term: 2, Command: strings.Repeat("s", sz/7)}}}
		want = append(want, m)
		tn.tr[0].Send(m)
	}
	// One message close to the 64 KiB limit: about 52 fragments in a single burst.
	big := raft.Message{Type: raft.MsgApp, From: 0, To: 1, Term: 2, Seq: 99, Entries: []raft.LogEntry{{Term: 2, Command: make([]byte, 60_000)}}}
	want = append(want, big)
	sizes = append(sizes, 60_000)
	tn.tr[0].Send(big)
	waitFor(t, 10*time.Second, "all fragmented messages", func() bool { return tn.rec[1].count() >= len(want) })
	got := tn.rec[1].snapshot()
	for i := range want {
		if !reflect.DeepEqual(got[i], want[i]) {
			t.Errorf("message %d (%d-byte payload) arrived altered", i, sizes[i])
		}
	}
}

func TestOversizedMessageIsRefused(t *testing.T) {
	tn := newTestNet(t, 2, netOpts{tweak: func(i int, c *Config) { c.MaxMessage = 10_000 }})
	tn.tr[0].Send(raft.Message{Type: raft.MsgApp, From: 0, To: 1, Term: 1, Seq: 1, Entries: []raft.LogEntry{{Term: 1, Command: make([]byte, 20_000)}}})
	tn.tr[0].Send(small(0, 1, 2))
	waitFor(t, 3*time.Second, "the small message", func() bool { return tn.rec[1].count() >= 1 })
	if st := tn.tr[0].Stats(); st.DroppedEncode != 1 {
		t.Errorf("DroppedEncode = %d, want 1", st.DroppedEncode)
	}
}

// With 20% loss, 10% duplication and random delay (reordering), the stream must
// still come out in order and without duplicates, and nearly everything must be
// repaired by retransmission rather than lost.
func TestLossReorderDuplicationAreRepaired(t *testing.T) {
	tn := newTestNet(t, 2, netOpts{drop: 0.20, dup: 0.10, maxDelay: 8 * time.Millisecond})
	const n = 600
	for i := 1; i <= n; i++ {
		tn.tr[0].Send(small(0, 1, uint64(i)))
		time.Sleep(time.Millisecond)
	}
	// Everything is either delivered or explicitly given up on.
	waitFor(t, 10*time.Second, "delivery + skips to account for every message", func() bool {
		st := tn.tr[1].Stats()
		return st.Delivered+st.Skipped >= n
	})
	got := tn.rec[1].snapshot()
	var last uint64
	for _, m := range got {
		if m.Seq <= last {
			t.Fatalf("Seq %d delivered after %d: reordered or duplicated", m.Seq, last)
		}
		last = m.Seq
	}
	// Nothing may be left behind in the receiver's buffers (duplicates must not leak).
	time.Sleep(100 * time.Millisecond)
	tn.tr[1].mu.Lock()
	leaked, bufBytes := len(tn.tr[1].in[0].pending), tn.tr[1].in[0].bufBytes
	tn.tr[1].mu.Unlock()
	if leaked != 0 || bufBytes != 0 {
		t.Errorf("receiver kept %d pending messages / %d bytes after everything was delivered or skipped", leaked, bufBytes)
	}
	rate := float64(len(got)) / n
	st0, st1 := tn.tr[0].Stats(), tn.tr[1].Stats()
	t.Logf("delivered %d/%d (%.1f%%), skipped %d, retransmits %d, naks sent %d recv %d", len(got), n, 100*rate, st1.Skipped, st0.Retransmits, st1.NaksSent, st0.NaksReceived)
	if rate < 0.95 {
		t.Errorf("only %.1f%% delivered; retransmission should repair almost all of a 20%% loss", 100*rate)
	}
	if st0.Retransmits == 0 || st1.NaksSent == 0 {
		t.Errorf("no repair traffic (retransmits %d, naks %d): loss was not exercised", st0.Retransmits, st1.NaksSent)
	}
}

func TestFixedNumberOfGoroutines(t *testing.T) {
	time.Sleep(50 * time.Millisecond)
	base := runtime.NumGoroutine()
	tn := newTestNet(t, 3, netOpts{})
	time.Sleep(100 * time.Millisecond)
	idle := runtime.NumGoroutine()
	for i := 1; i <= 5000; i++ {
		tn.tr[0].Send(small(0, 1, uint64(i)))
		tn.tr[0].Send(small(0, 2, uint64(i)))
	}
	busy := 0
	for end := time.Now().Add(500 * time.Millisecond); time.Now().Before(end); time.Sleep(2 * time.Millisecond) {
		busy = max(busy, runtime.NumGoroutine())
	}
	t.Logf("goroutines: before=%d with 3 transports=%d under load=%d", base, idle, busy)
	if idle-base != 9 || busy-base != 9 {
		t.Errorf("want exactly 3 goroutines per transport (9 in all), got +%d idle and +%d under load", idle-base, busy-base)
	}
}

func TestAuthenticationRejectsForgeries(t *testing.T) {
	tn := newTestNet(t, 3, netOpts{})
	b := tn.tr[1]

	attacker := listen(t)
	defer attacker.Close()
	send := func(pkt []byte) { attacker.WriteToUDP(pkt, tn.conns[1].LocalAddr().(*net.UDPAddr)) }

	// A genuine packet from node 0, then tampered with.
	body := make([]byte, dataHdrLen+1)
	body[7] = 1  // seq 1
	body[11] = 1 // 1 fragment
	good := tn.tr[0].seal(typeData, 1, body)
	bad := append([]byte(nil), good...)
	bad[hdrLen+dataHdrLen] ^= 0xff
	send(bad)

	// Wrong key.
	other, err := New(Config{ID: 0, Peers: []string{tn.conns[0].LocalAddr().String(), tn.conns[1].LocalAddr().String(), tn.conns[2].LocalAddr().String()},
		Key: []byte("a-completely-different-key!!"), Conn: listen(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	send(other.seal(typeData, 1, body))

	// Authentic, but addressed to a different node.
	send(tn.tr[0].seal(typeData, 2, body))

	waitFor(t, 2*time.Second, "forgeries to be counted", func() bool {
		st := b.Stats()
		return st.DroppedAuth >= 2 && st.DroppedMalformed >= 1
	})

	// Authentic packet, but the message inside claims another sender.
	tn.tr[0].Send(raft.Message{Type: raft.MsgVoteResp, From: 2, To: 1, Term: 9, Granted: true})
	waitFor(t, 2*time.Second, "spoofed From to be refused", func() bool { return b.Stats().DroppedMalformed >= 2 })

	if n := tn.rec[1].count(); n != 0 {
		t.Fatalf("%d forged messages reached the endpoint", n)
	}
	// And the genuine stream still works.
	tn.tr[0].Send(small(0, 1, 77))
	waitFor(t, 2*time.Second, "a genuine message", func() bool { return tn.rec[1].count() == 1 })
}

func TestCongestionWindowGrowsWithoutLossAndShrinksWithLoss(t *testing.T) {
	clean := newTestNet(t, 2, netOpts{})
	for i := 1; i <= 3000; i++ {
		clean.tr[0].Send(small(0, 1, uint64(i)))
	}
	waitFor(t, 10*time.Second, "clean transfer", func() bool { return clean.rec[1].count() >= 3000 })
	time.Sleep(50 * time.Millisecond)
	clean.tr[0].mu.Lock()
	cw := clean.tr[0].out[1].cwnd
	clean.tr[0].mu.Unlock()
	init := clean.tr[0].cfg.InitCwnd
	if cw <= init {
		t.Errorf("cwnd = %d did not grow beyond the initial %d on a clean link", cw, init)
	}

	lossy := newTestNet(t, 2, netOpts{drop: 0.15, maxDelay: 4 * time.Millisecond})
	for i := 1; i <= 1500; i++ {
		lossy.tr[0].Send(small(0, 1, uint64(i)))
		if i%20 == 0 {
			time.Sleep(time.Millisecond)
		}
	}
	waitFor(t, 15*time.Second, "lossy transfer to settle", func() bool {
		st := lossy.tr[1].Stats()
		return st.Delivered+st.Skipped >= 1500
	})
	lossy.tr[0].mu.Lock()
	ss, max := lossy.tr[0].out[1].ssthresh, lossy.tr[0].cfg.MaxCwnd
	lossy.tr[0].mu.Unlock()
	if ss >= max {
		t.Errorf("ssthresh = %d never fell below its initial %d despite 15%% loss", ss, max)
	}
	t.Logf("clean cwnd=%d (initial %d); lossy ssthresh=%d", cw, init, ss)
}

func TestUnreachablePeerIsReported(t *testing.T) {
	// Node 1's socket exists but nothing reads it, as if the machine were down.
	tn := newTestNet(t, 2, netOpts{})
	tn.tr[1].Close()
	waitFor(t, 3*time.Second, "an unreachable report", func() bool { return tn.rec[0].unreachableCount() > 0 })
	if tn.rec[0].unreachable[0] != 1 {
		t.Errorf("reported peer %d, want 1", tn.rec[0].unreachable[0])
	}
}

func TestFullSendQueueDropsAndReports(t *testing.T) {
	tn := newTestNet(t, 2, netOpts{tweak: func(i int, c *Config) { c.SendQueue = 8; c.PeerTimeout = time.Hour }})
	tn.tr[1].Close()
	for i := 1; i <= 200; i++ {
		tn.tr[0].Send(small(0, 1, uint64(i)))
	}
	if st := tn.tr[0].Stats(); st.DroppedQueue == 0 {
		t.Error("nothing was dropped although the queue holds only 8")
	}
	waitFor(t, 2*time.Second, "an overflow report", func() bool { return tn.rec[0].unreachableCount() > 0 })
}

// A restarted sender begins a new session; the receiver must accept its stream from
// sequence 1 again instead of treating it as old duplicates.
func TestRestartedPeerStartsANewStream(t *testing.T) {
	tn := newTestNet(t, 2, netOpts{})
	for i := 1; i <= 20; i++ {
		tn.tr[0].Send(small(0, 1, uint64(i)))
	}
	waitFor(t, 3*time.Second, "first incarnation's messages", func() bool { return tn.rec[1].count() >= 20 })

	tn.tr[0].Close()
	time.Sleep(5 * time.Millisecond) // a later session number
	c0 := listen(t)
	addrs := []string{c0.LocalAddr().String(), tn.conns[1].LocalAddr().String()}
	// Node 1 still knows node 0 at its old address; give the new node 0 that socket.
	c0.Close()
	re := listenOn(t, tn.conns[0].LocalAddr().(*net.UDPAddr))
	addrs[0] = re.LocalAddr().String()
	n0, err := New(Config{ID: 0, Peers: addrs, Key: testKey, Conn: re})
	if err != nil {
		t.Fatal(err)
	}
	defer n0.Close()
	n0.SetEndpoint(&recorder{})
	for i := 1; i <= 5; i++ {
		n0.Send(small(0, 1, uint64(1000+i)))
	}
	waitFor(t, 3*time.Second, "the restarted sender's messages", func() bool { return tn.rec[1].count() >= 25 })
	got := tn.rec[1].snapshot()
	for i := 0; i < 5; i++ {
		if got[20+i].Seq != uint64(1001+i) {
			t.Fatalf("restarted stream: message %d has Seq %d, want %d", i, got[20+i].Seq, 1001+i)
		}
	}
}

func listenOn(t *testing.T, a *net.UDPAddr) *net.UDPConn {
	t.Helper()
	var err error
	var c *net.UDPConn
	for i := 0; i < 50; i++ {
		if c, err = net.ListenUDP("udp", a); err == nil {
			return c
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(err)
	return nil
}
