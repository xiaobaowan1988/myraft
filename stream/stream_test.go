package stream

import (
	"bytes"
	"encoding/binary"
	"net"
	"runtime"
	"testing"
	"time"
)

func TestOrderedDeliveryWithoutFaults(t *testing.T) {
	tn := newTestNet(t, 2, netOpts{})
	const n = 2000
	for i := 1; i <= n; i++ {
		if _, res := tn.nodes[0].Offer(1, 0, numbered(i, 20)); res != Offered {
			t.Fatalf("Offer(%d) = %v", i, res)
		}
	}
	waitFor(t, 10*time.Second, "all messages", func() bool { return tn.rec[1].count() >= n })
	for i, m := range tn.rec[1].all() {
		if numberOf(m.msg) != i+1 || m.from != 0 {
			t.Fatalf("message %d is #%d from %d: not in order, or duplicated", i, numberOf(m.msg), m.from)
		}
	}
}

func TestMessagesOfAnySizeIncludingHuge(t *testing.T) {
	tn := newTestNet(t, 2, netOpts{})
	sizes := []int{0, 1, 7, 1160, 1161, 5000, 100_000, 1 << 20}
	for i, sz := range sizes {
		msg := bytes.Repeat([]byte{byte(i + 1)}, sz)
		if _, res := tn.nodes[0].Offer(1, 0, msg); res != Offered {
			t.Fatalf("Offer of %d bytes = %v", sz, res)
		}
	}
	waitFor(t, 20*time.Second, "all sizes", func() bool { return tn.rec[1].count() >= len(sizes) })
	for i, m := range tn.rec[1].all() {
		if len(m.msg) != sizes[i] || (sizes[i] > 0 && !bytes.Equal(m.msg, bytes.Repeat([]byte{byte(i + 1)}, sizes[i]))) {
			t.Errorf("message %d: got %d bytes, want %d intact", i, len(m.msg), sizes[i])
		}
	}
}

func TestOfferErrors(t *testing.T) {
	tn := newTestNet(t, 2, netOpts{tweak: func(i int, c *Config) { c.MaxMessage = 1000 }})
	if _, res := tn.nodes[0].Offer(1, 0, make([]byte, 1001)); res != TooBig {
		t.Errorf("oversized: %v", res)
	}
	if _, res := tn.nodes[0].Offer(0, 0, []byte("x")); res != BadPeer {
		t.Errorf("offer to self: %v", res)
	}
	if _, res := tn.nodes[0].Offer(9, 0, []byte("x")); res != BadPeer {
		t.Errorf("offer to unknown peer: %v", res)
	}
	if _, res := tn.nodes[0].Offer(1, 5, []byte("x")); res != BadPeer {
		t.Errorf("offer on a stream that does not exist: %v", res)
	}
	tn.nodes[0].Close()
	if _, res := tn.nodes[0].Offer(1, 0, []byte("x")); res != Closed {
		t.Errorf("after Close: %v", res)
	}
}

// The core promise: with 20% loss, 10% duplication and reordering, EVERY message
// arrives, once, in order. (Contrast with a datagram transport that may give up.)
func TestReliableUnderLossReorderAndDuplication(t *testing.T) {
	tn := newTestNet(t, 2, netOpts{drop: 0.20, dup: 0.10, maxDelay: 8 * time.Millisecond})
	const n = 600
	for i := 1; i <= n; i++ {
		if _, res := tn.nodes[0].Offer(1, 0, numbered(i, 40)); res != Offered {
			t.Fatalf("Offer(%d) = %v", i, res)
		}
		time.Sleep(time.Millisecond)
	}
	waitFor(t, 30*time.Second, "every message to arrive", func() bool { return tn.rec[1].count() >= n })
	time.Sleep(200 * time.Millisecond) // anything extra would show up now
	got := tn.rec[1].all()
	if len(got) != n {
		t.Fatalf("%d messages delivered, want exactly %d", len(got), n)
	}
	for i, m := range got {
		if numberOf(m.msg) != i+1 || !intact(m.msg) {
			t.Fatalf("message %d is #%d (intact=%v): not in order, duplicated or damaged", i, numberOf(m.msg), intact(m.msg))
		}
	}
	st0, st1 := tn.nodes[0].Stats(), tn.nodes[1].Stats()
	t.Logf("all %d delivered; retransmits %d, naks sent %d recv %d, frames %d", n, st0.Retransmits, st1.NaksSent, st0.NaksReceived, st0.FramesSent)
	if st0.Retransmits == 0 || st1.NaksSent == 0 {
		t.Errorf("no repair traffic (retransmits %d, naks %d): loss was not exercised", st0.Retransmits, st1.NaksSent)
	}
	// the receiver holds nothing back once everything is delivered
	tn.nodes[1].mu.Lock()
	buffered := tn.nodes[1].in[0].ss[0].rb.buffered()
	tn.nodes[1].mu.Unlock()
	if buffered != 0 {
		t.Errorf("receiver still buffers %d bytes after delivering everything", buffered)
	}
}

func TestHugeMessageSurvivesLoss(t *testing.T) {
	tn := newTestNet(t, 2, netOpts{drop: 0.10, dup: 0.05, maxDelay: 3 * time.Millisecond, tweak: func(i int, c *Config) {
		c.MaxMessage, c.RecvWindow, c.SendBuffer = 4<<20, 8<<20, 16<<20
	}})
	msgs := [][]byte{numbered(1, 2<<20), numbered(2, 300_000), numbered(3, 10)}
	for _, m := range msgs {
		if _, res := tn.nodes[0].Offer(1, 0, m); res != Offered {
			t.Fatalf("Offer = %v", res)
		}
	}
	waitFor(t, 60*time.Second, "the huge messages", func() bool { return tn.rec[1].count() >= len(msgs) })
	for i, m := range tn.rec[1].all() {
		if !bytes.Equal(m.msg, msgs[i]) {
			t.Errorf("message %d (%d bytes) arrived altered", i, len(msgs[i]))
		}
	}
}

// A consumer that cannot keep up must slow the sender down, not lose data: the
// window shrinks (consumed only advances after OnMessage returns), the send buffer
// fills, and Offer says BackPressured.
func TestSlowConsumerPushesBackWithoutLoss(t *testing.T) {
	tn := newTestNet(t, 2, netOpts{tweak: func(i int, c *Config) {
		c.SendBuffer = 256 << 10
		c.RecvWindow = 64 << 10
		c.MaxMessage = 32 << 10
	}})
	tn.rec[1].delay.Store(int64(2 * time.Millisecond))
	accepted := 0
	sawBackPressure := false
	for i := 1; i <= 400; i++ {
		_, res := tn.nodes[0].Offer(1, 0, numbered(i, 8<<10))
		if res == BackPressured {
			sawBackPressure = true
			break
		}
		if res != Offered {
			t.Fatalf("Offer(%d) = %v", i, res)
		}
		accepted++
	}
	if !sawBackPressure {
		t.Fatal("never back-pressured although the consumer is slow and the buffer is small")
	}
	waitFor(t, 30*time.Second, "all accepted messages", func() bool { return tn.rec[1].count() >= accepted })
	time.Sleep(100 * time.Millisecond)
	got := tn.rec[1].all()
	if len(got) != accepted {
		t.Fatalf("%d delivered, %d accepted", len(got), accepted)
	}
	for i, m := range got {
		if numberOf(m.msg) != i+1 || !intact(m.msg) {
			t.Fatalf("message %d is #%d: data lost or reordered under back-pressure", i, numberOf(m.msg))
		}
	}
	tn.nodes[1].mu.Lock()
	buffered := tn.nodes[1].in[0].ss[0].rb.buffered()
	tn.nodes[1].mu.Unlock()
	st := tn.nodes[0].Stats()
	if st.Retransmits > 5 {
		t.Errorf("%d retransmissions: the sender overran the receiver window instead of respecting it", st.Retransmits)
	}
	t.Logf("accepted %d messages before back-pressure; receiver buffered %d bytes at the end; retransmits %d of %d frames", accepted, buffered, st.Retransmits, st.FramesSent)
}

// A peer that goes silent ends the stream: OnPeerDown fires, Offer says
// NotConnected, queued messages are gone, and when the peer is back a fresh
// stream works and does not replay the old queue.
func TestSilentPeerResetsTheStreamAndRecovers(t *testing.T) {
	tn := newTestNet(t, 2, netOpts{})
	for i := 1; i <= 5; i++ {
		tn.nodes[0].Offer(1, 0, numbered(i, 20))
	}
	waitFor(t, 3*time.Second, "the first messages", func() bool { return tn.rec[1].count() >= 5 })

	tn.nodes[1].Close() // the peer disappears
	for i := 101; i <= 105; i++ {
		tn.nodes[0].Offer(1, 0, numbered(i, 20)) // queued, never acknowledged
	}
	waitFor(t, 3*time.Second, "OnPeerDown", func() bool { return tn.rec[0].downCount() > 0 })
	if _, res := tn.nodes[0].Offer(1, 0, numbered(200, 20)); res != NotConnected {
		t.Errorf("Offer to a down peer = %v, want NotConnected", res)
	}

	// The peer returns on the same address.
	tn.conns[1] = listenOn(t, tn.conns[1].LocalAddr().(*net.UDPAddr))
	tn.start(t, 1, netOpts{})
	waitFor(t, 3*time.Second, "the peer to be considered up again", func() bool {
		_, res := tn.nodes[0].Offer(1, 0, numbered(300, 20))
		return res == Offered
	})
	waitFor(t, 3*time.Second, "a message on the new stream", func() bool { return tn.rec[1].count() >= 1 })
	time.Sleep(200 * time.Millisecond)
	for _, m := range tn.rec[1].all() {
		if n := numberOf(m.msg); n != 300 {
			t.Errorf("the restarted peer received #%d: the old queue must not be replayed", n)
		}
	}
}

// A peer that restarts quickly (before being declared down) must still get a
// stream that starts at position 0, in both directions.
func TestRestartedPeerGetsAFreshStreamInBothDirections(t *testing.T) {
	tn := newTestNet(t, 2, netOpts{})
	for i := 1; i <= 20; i++ {
		tn.nodes[0].Offer(1, 0, numbered(i, 20))
		tn.nodes[1].Offer(0, 0, numbered(i, 20))
	}
	waitFor(t, 3*time.Second, "both directions", func() bool { return tn.rec[1].count() >= 20 && tn.rec[0].count() >= 20 })

	tn.nodes[1].Close()
	tn.conns[1] = listenOn(t, tn.conns[1].LocalAddr().(*net.UDPAddr))
	tn.start(t, 1, netOpts{}) // quickly: well inside PeerTimeout

	// Messages offered before node 0 notices the new instance go into the old stream
	// and are dropped when it is reset (the owner recovers them), so wait for it.
	waitFor(t, 3*time.Second, "node 0 to notice the new instance", func() bool {
		tn.nodes[0].mu.Lock()
		defer tn.nodes[0].mu.Unlock()
		return tn.nodes[0].out[1].peerInc == tn.nodes[1].incarnation
	})
	for i := 1; i <= 5; i++ {
		tn.nodes[0].Offer(1, 0, numbered(1000+i, 20))
		tn.nodes[1].Offer(0, 0, numbered(2000+i, 20))
	}
	waitFor(t, 5*time.Second, "node 1 to receive the new stream", func() bool { return tn.rec[1].count() >= 5 })
	waitFor(t, 5*time.Second, "node 0 to receive the restarted peer's stream", func() bool { return tn.rec[0].count() >= 25 })
	for i, m := range tn.rec[1].all() {
		if numberOf(m.msg) != 1001+i {
			t.Errorf("node 1 message %d is #%d, want %d", i, numberOf(m.msg), 1001+i)
		}
	}
	got := tn.rec[0].all()
	for i := 0; i < 5; i++ {
		if numberOf(got[20+i].msg) != 2001+i {
			t.Errorf("node 0 message %d is #%d, want %d", 20+i, numberOf(got[20+i].msg), 2001+i)
		}
	}
}

func TestAuthenticationRejectsForgeries(t *testing.T) {
	tn := newTestNet(t, 3, netOpts{})
	b := tn.nodes[1]
	attacker := listen(t)
	defer attacker.Close()
	send := func(pkt []byte) { attacker.WriteToUDP(pkt, tn.conns[1].LocalAddr().(*net.UDPAddr)) }

	body := make([]byte, dataHdrLen+4+3)
	binary.BigEndian.PutUint64(body[1:], tn.nodes[1].incarnation) // addressed to node 1's current instance
	body[dataHdrLen+3] = 3                                        // a 3-byte message "\0\0\0"
	good := tn.nodes[0].seal(typeData, 1, 12345, body)
	bad := append([]byte(nil), good...)
	bad[hdrLen+dataHdrLen+1] ^= 0xff
	send(bad) // tampered

	other, err := New(Config{ID: 0, Peers: tn.addrs, Key: []byte("a-completely-different-key!!"), Conn: listen(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	send(other.seal(typeData, 1, 12345, body)) // wrong key

	send(tn.nodes[0].seal(typeData, 2, 12345, body)) // authentic, but for node 2

	waitFor(t, 2*time.Second, "forgeries to be counted", func() bool {
		st := b.Stats()
		return st.DroppedAuth >= 2 && st.DroppedMalformed >= 1
	})
	if n := tn.rec[1].count(); n != 0 {
		t.Fatalf("%d forged messages reached the handler", n)
	}
	tn.nodes[0].Offer(1, 0, numbered(77, 20))
	waitFor(t, 2*time.Second, "a genuine message", func() bool { return tn.rec[1].count() == 1 })
}

// An authenticated peer that violates the framing (a length beyond MaxMessage) must
// not make the receiver allocate or crash; its stream is dropped.
func TestFramingViolationPoisonsOnlyThatStream(t *testing.T) {
	tn := newTestNet(t, 3, netOpts{})
	attacker := listen(t)
	defer attacker.Close()
	body := make([]byte, dataHdrLen+8)
	binary.BigEndian.PutUint64(body[1:], tn.nodes[1].incarnation)
	copy(body[dataHdrLen:], []byte{0xff, 0xff, 0xff, 0xff, 1, 2, 3, 4}) // claims a 4 GiB message
	attacker.WriteToUDP(tn.nodes[0].seal(typeData, 1, 99999999999999999, body), tn.conns[1].LocalAddr().(*net.UDPAddr))
	waitFor(t, 2*time.Second, "the stream to be poisoned", func() bool { return tn.nodes[1].Stats().Poisoned >= 1 })

	// The other peer's stream is unaffected.
	tn.nodes[2].Offer(1, 0, numbered(5, 20))
	waitFor(t, 2*time.Second, "node 2's message", func() bool { return tn.rec[1].count() >= 1 })
}

func TestFixedNumberOfGoroutines(t *testing.T) {
	time.Sleep(50 * time.Millisecond)
	base := runtime.NumGoroutine()
	tn := newTestNet(t, 3, netOpts{})
	time.Sleep(100 * time.Millisecond)
	idle := runtime.NumGoroutine()
	for i := 1; i <= 5000; i++ {
		tn.nodes[0].Offer(1, 0, numbered(i, 30))
		tn.nodes[0].Offer(2, 0, numbered(i, 30))
	}
	busy := 0
	for end := time.Now().Add(500 * time.Millisecond); time.Now().Before(end); time.Sleep(2 * time.Millisecond) {
		busy = max(busy, runtime.NumGoroutine())
	}
	t.Logf("goroutines: before=%d with 3 nodes=%d under load=%d", base, idle, busy)
	if idle-base != 9 || busy-base != 9 {
		t.Errorf("want exactly 3 goroutines per node (9 in all), got +%d idle and +%d under load", idle-base, busy-base)
	}
}

func TestCongestionWindowGrowsWithoutLossAndShrinksWithLoss(t *testing.T) {
	clean := newTestNet(t, 2, netOpts{})
	for i := 1; i <= 20000; i++ {
		clean.nodes[0].Offer(1, 0, numbered(i, 200))
	}
	waitFor(t, 20*time.Second, "clean transfer", func() bool { return clean.rec[1].count() >= 20000 })
	time.Sleep(50 * time.Millisecond)
	clean.nodes[0].mu.Lock()
	cw, init := clean.nodes[0].out[1].ss[0].cwnd, clean.nodes[0].cfg.InitCwnd
	clean.nodes[0].mu.Unlock()
	if cw <= init {
		t.Errorf("cwnd = %d frames did not grow beyond the initial %d on a clean link", cw, init)
	}

	lossy := newTestNet(t, 2, netOpts{drop: 0.15, maxDelay: 4 * time.Millisecond})
	for i := 1; i <= 3000; i++ {
		lossy.nodes[0].Offer(1, 0, numbered(i, 200))
		if i%20 == 0 {
			time.Sleep(time.Millisecond)
		}
	}
	waitFor(t, 60*time.Second, "lossy transfer", func() bool { return lossy.rec[1].count() >= 3000 })
	lossy.nodes[0].mu.Lock()
	ss, max := lossy.nodes[0].out[1].ss[0].ssthresh, lossy.nodes[0].cfg.MaxCwnd
	lossy.nodes[0].mu.Unlock()
	if ss >= max {
		t.Errorf("ssthresh = %d never fell below its initial %d despite 15%% loss", ss, max)
	}
	t.Logf("clean cwnd=%d (initial %d); lossy ssthresh=%d", cw, init, ss)
}

// Data addressed to an earlier life of this process must be ignored: the stream was
// created for an instance that no longer exists.
func TestDataForAnEarlierInstanceIsIgnored(t *testing.T) {
	tn := newTestNet(t, 2, netOpts{})
	oldInc := tn.nodes[1].incarnation

	tn.nodes[1].Close()
	tn.conns[1] = listenOn(t, tn.conns[1].LocalAddr().(*net.UDPAddr))
	tn.start(t, 1, netOpts{})
	if tn.nodes[1].incarnation == oldInc {
		t.Fatal("the restarted node kept its instance id")
	}

	// A well-formed, authentic frame carrying a whole message, but for the old instance.
	body := make([]byte, dataHdrLen+4+3)
	binary.BigEndian.PutUint64(body[1:], oldInc)
	body[dataHdrLen+3] = 3
	attacker := listen(t)
	defer attacker.Close()
	attacker.WriteToUDP(tn.nodes[0].seal(typeData, 1, uint64(time.Now().UnixNano())+1e9, body), tn.conns[1].LocalAddr().(*net.UDPAddr))
	waitFor(t, 2*time.Second, "the stale frame to be counted", func() bool { return tn.nodes[1].Stats().StaleStream >= 1 })
	time.Sleep(50 * time.Millisecond)
	if n := tn.rec[1].count(); n != 0 {
		t.Fatalf("%d messages from a stream meant for the previous instance were delivered", n)
	}
}

// The sender must not transmit past the right edge the receiver advertised. A
// consumer that is completely stuck keeps that edge where it is.
func TestSenderStopsAtTheAdvertisedWindowEdge(t *testing.T) {
	const window = 16 << 10
	tn := newTestNet(t, 2, netOpts{tweak: func(i int, c *Config) {
		c.RecvWindow, c.InitWindow, c.MaxMessage = window, window, 4<<10
		c.InitCwnd = 256 // so the congestion window is not what stops the sender: only the receiver's window can
	}})
	stuck := &recorder{block: make(chan struct{})}
	tn.nodes[1].SetHandler(stuck)

	const n = 100
	for i := 1; i <= n; i++ {
		if _, res := tn.nodes[0].Offer(1, 0, numbered(i, 1000)); res != Offered {
			t.Fatalf("Offer(%d) = %v", i, res)
		}
	}
	time.Sleep(400 * time.Millisecond)
	tn.nodes[0].mu.Lock()
	sent, limit := tn.nodes[0].out[1].ss[0].sentPos, tn.nodes[0].out[1].ss[0].limit
	tn.nodes[0].mu.Unlock()
	t.Logf("sender stopped at position %d (window edge %d, total queued about %d)", sent, limit, n*1004)
	if sent > window {
		t.Errorf("the sender transmitted up to position %d, past the %d-byte window the receiver agreed to buffer", sent, window)
	}
	if sent < window/2 {
		t.Errorf("the sender stopped at %d, far short of the %d-byte window: it is not using it", sent, window)
	}

	close(stuck.block) // the consumer recovers; nothing may have been lost
	waitFor(t, 15*time.Second, "the stream to drain", func() bool { return stuck.count() >= n })
	for i, m := range stuck.all() {
		if numberOf(m.msg) != i+1 || !intact(m.msg) {
			t.Fatalf("message %d is #%d: data lost or reordered across a stalled window", i, numberOf(m.msg))
		}
	}
}

// Streams are independent: a huge message in one must not hold up small messages in
// another. The control experiment sends the same small messages behind the huge one
// on the SAME stream, where they have to wait for it.
func TestStreamsDoNotBlockEachOther(t *testing.T) {
	opts := netOpts{tweak: func(i int, c *Config) {
		c.Streams = 2
		c.MaxMessage, c.RecvWindow, c.SendBuffer = 4<<20, 8<<20, 16<<20
	}}
	// run offers a huge message on stream 1, then 20 small ones on smallStream, and
	// returns when the last small one arrived and when the huge one did.
	run := func(smallStream int) (smallDone, hugeDone time.Duration) {
		tn := newTestNet(t, 2, opts)
		waitFor(t, 2*time.Second, "the handshake", func() bool {
			tn.nodes[0].mu.Lock()
			defer tn.nodes[0].mu.Unlock()
			return tn.nodes[0].out[1].peerInc != 0
		})
		start := time.Now()
		tn.nodes[0].Offer(1, 1, numbered(0, 3<<20))
		time.Sleep(5 * time.Millisecond)
		for i := 1; i <= 20; i++ {
			tn.nodes[0].Offer(1, smallStream, numbered(i, 30))
		}
		waitFor(t, 60*time.Second, "everything", func() bool { return tn.rec[1].count() >= 21 })
		for _, m := range tn.rec[1].all() {
			d := m.at.Sub(start)
			if len(m.msg) > 1000 {
				hugeDone = d
			} else {
				smallDone = max(smallDone, d)
			}
		}
		return
	}
	sepSmall, sepHuge := run(0)
	sameSmall, sameHuge := run(1)
	t.Logf("separate streams: small done %v, huge done %v | same stream: small done %v, huge done %v",
		sepSmall.Round(time.Millisecond), sepHuge.Round(time.Millisecond), sameSmall.Round(time.Millisecond), sameHuge.Round(time.Millisecond))
	if sepSmall > sepHuge/2 {
		t.Errorf("small messages on their own stream took %v, but the huge one took %v: they were held up", sepSmall, sepHuge)
	}
	if sameSmall < sameHuge {
		t.Errorf("control experiment: small messages behind the huge one on the same stream finished at %v, before the huge one (%v)", sameSmall, sameHuge)
	}
}

// Order is kept within each stream, whatever the interleaving between streams.
func TestOrderIsKeptPerStream(t *testing.T) {
	tn := newTestNet(t, 2, netOpts{drop: 0.10, dup: 0.05, maxDelay: 5 * time.Millisecond, tweak: func(i int, c *Config) { c.Streams = 3 }})
	const n = 300
	for i := 1; i <= n; i++ {
		for st := 0; st < 3; st++ {
			tn.nodes[0].Offer(1, st, numbered(i, 10+st*400))
		}
		if i%10 == 0 {
			time.Sleep(time.Millisecond)
		}
	}
	waitFor(t, 60*time.Second, "all streams", func() bool { return tn.rec[1].count() >= 3*n })
	next := [3]int{1, 1, 1}
	for _, m := range tn.rec[1].all() {
		if numberOf(m.msg) != next[m.stream] || !intact(m.msg) {
			t.Fatalf("stream %d delivered #%d, want #%d: order broken within the stream", m.stream, numberOf(m.msg), next[m.stream])
		}
		next[m.stream]++
	}
}
