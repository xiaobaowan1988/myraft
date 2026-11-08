package client

import (
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/xiaobaowan1988/myraft/driver"
	"github.com/xiaobaowan1988/myraft/driver/logbuf"
	"github.com/xiaobaowan1988/myraft/driver/proto"
)

type simPair struct {
	*sim
	pubD, subD *simDriver
	pub        *Publication
	sub        *Subscription
	v          *verifier
	sent       int
}

func newSimPair(t *testing.T, seed int64, mod func(*driver.Config)) *simPair {
	t.Helper()
	s := newSim(t, seed, mod)
	p := &simPair{sim: s, pubD: s.addDriver(), subD: s.addDriver(), v: newVerifier(t, 4)}
	var err error
	if p.sub, err = p.subD.c.AddSubscription("udp://127.0.0.1:0", 1); err != nil {
		t.Fatal(err)
	}
	if p.pub, err = p.pubD.c.AddPublication("udp://"+p.sub.LocalAddr(), 1); err != nil {
		t.Fatal(err)
	}
	if !s.runUntil(10*time.Second, func() bool { return p.pub.IsConnected() && p.sub.ImageCount() == 1 }) {
		t.Fatal("no connection")
	}
	return p
}

// publish offers messages (source src) until n have gone in, advancing the world as needed.
func (p *simPair) publish(src, n int) {
	p.t.Helper()
	deadline := p.clock.Now() + int64(120*time.Second)
	for i := 0; i < n; i++ {
		for {
			if _, res := p.pub.Offer(makeMsg(src, p.sent+i)); res == Offered {
				break
			}
			p.pollOnce()
			p.advance()
			if p.clock.Now() > deadline {
				p.t.Fatalf("could not publish message %d", p.sent+i)
			}
		}
	}
	p.sent += n
}

func (p *simPair) pollOnce() {
	if _, err := p.sub.Poll(p.v.handle, 64); err != nil {
		p.t.Fatal(err)
	}
}

func (p *simPair) deliveredAll(limit time.Duration, want int) bool {
	return p.runUntil(limit, func() bool { p.pollOnce(); return p.v.total >= want })
}

func isData(b []byte) bool {
	k, err := proto.Kind(b)
	return err == nil && (k == proto.TypeData || k == proto.TypePad)
}

// The last datagram of a stream is lost and nothing follows it. Only the sender's
// heartbeat tells the receiver there is more: it must then ask for it.
func TestSimLossAtTheTailIsFoundByHeartbeat(t *testing.T) {
	p := newSimPair(t, 5, nil)
	p.publish(0, 20)
	if !p.deliveredAll(10*time.Second, 20) {
		t.Fatalf("setup: %d of 20", p.v.total)
	}
	dropped := 0
	p.net.filter = func(from, to netip.AddrPort, b []byte) bool {
		if dropped == 0 && isData(b) {
			dropped++
			return true
		}
		return false
	}
	p.publish(0, 1) // the only datagram, lost
	if !p.deliveredAll(10*time.Second, 21) {
		t.Fatalf("the lost tail message never arrived (%d of 21); sender %+v receiver %+v", p.v.total, p.pubD.d.Sender().Stats(), p.subD.d.Receiver().Stats())
	}
	if dropped != 1 || p.subD.d.Receiver().Stats().NaksSent == 0 || p.pubD.d.Sender().Stats().RetransmitDatagrams == 0 {
		t.Fatalf("recovery did not go through NAK and retransmission: dropped %d, receiver %+v, sender %+v", dropped, p.subD.d.Receiver().Stats(), p.pubD.d.Sender().Stats())
	}
}

// The control messages are lost too: Setup, Status and NAK are all ordinary datagrams.
func TestSimControlMessagesAreLostToo(t *testing.T) {
	for _, kind := range []struct {
		name string
		typ  uint16
		n    int
	}{{"Setup", proto.TypeSetup, 3}, {"Status", proto.TypeStatus, 40}, {"NAK", proto.TypeNak, 3}} {
		t.Run(kind.name, func(t *testing.T) {
			s := newSim(t, 9, nil)
			dropped := 0
			s.net.filter = func(from, to netip.AddrPort, b []byte) bool {
				if k, err := proto.Kind(b); err == nil && k == kind.typ && dropped < kind.n {
					dropped++
					return true
				}
				if kind.typ == proto.TypeNak && isData(b) && s.net.Sent%5 == 0 { // some data loss so that NAKs are needed
					return true
				}
				return false
			}
			pubD, subD := s.addDriver(), s.addDriver()
			sub, _ := subD.c.AddSubscription("udp://127.0.0.1:0", 1)
			pub, _ := pubD.c.AddPublication("udp://"+sub.LocalAddr(), 1)
			if !s.runUntil(20*time.Second, func() bool { return pub.IsConnected() && sub.ImageCount() == 1 }) {
				t.Fatalf("no connection although only %d %s messages were lost", dropped, kind.name)
			}
			v := newVerifier(t, 1)
			sent := 0
			deadline := s.clock.Now() + int64(120*time.Second)
			for v.total < 200 && s.clock.Now() < deadline {
				for i := 0; i < 4 && sent < 200; i++ {
					if _, res := pub.Offer(makeMsg(0, sent)); res != Offered {
						break
					}
					sent++
				}
				sub.Poll(v.handle, 64)
				s.advance()
			}
			if v.total != 200 {
				t.Fatalf("delivered %d of 200 after losing %d %s messages", v.total, dropped, kind.name)
			}
			if dropped != kind.n {
				t.Fatalf("only %d of the planned %d %s messages were dropped", dropped, kind.n, kind.name)
			}
		})
	}
}

// Total silence on the network for a while, then it comes back.
func TestSimNetworkBlackout(t *testing.T) {
	p := newSimPair(t, 11, nil)
	p.publish(0, 50)
	if !p.deliveredAll(10*time.Second, 50) {
		t.Fatal("setup")
	}
	p.runUntil(100*time.Millisecond, func() bool { return false }) // consumption reaches the publisher
	black := true
	p.net.filter = func(from, to netip.AddrPort, b []byte) bool { return black }
	p.publish(0, 20) // what fits in the window while nothing gets through
	p.runUntil(300*time.Millisecond, func() bool { return false })
	black = false
	if !p.deliveredAll(60*time.Second, 70) {
		t.Fatalf("delivered %d of 70 after the blackout; sender %+v receiver %+v", p.v.total, p.pubD.d.Sender().Stats(), p.subD.d.Receiver().Stats())
	}
}

// NAKs that name data the sender cannot or should not resend are refused, and
// refused NAKs cause no retransmission. A good one is served.
func TestSimBadNaksAreRefused(t *testing.T) {
	p := newSimPair(t, 13, nil)
	var senderAddr, subAddr netip.AddrPort
	p.net.trace = func(now int64, from, to netip.AddrPort, b []byte) {
		if k, _ := proto.Kind(b); k == proto.TypeSetup {
			senderAddr, subAddr = from, to
		}
	}
	// the Setup was sent before the trace; find the addresses from the open connections instead
	for a := range p.net.conns {
		if int(a.Port()) == mustPort(t, p.sub.LocalAddr()) {
			subAddr = a
		} else if a.Port() != 0 {
			senderAddr = a
		}
	}
	p.publish(0, 60)
	if !p.deliveredAll(10*time.Second, 60) {
		t.Fatal("setup")
	}
	p.runUntil(200*time.Millisecond, func() bool { return false }) // let consumption be reported
	p.publish(0, 10)                                               // sent and received, but not read: in flight as far as consumption goes
	p.runUntil(100*time.Millisecond, func() bool { return false })
	snd := p.pubD.d.Sender()
	session := p.pub.SessionID()
	initial := p.pub.log.InitialTermID()
	sentPos := ctr(p.pubD.c, driver.CounterSenderPos)
	consumed := ctr(p.pubD.c, driver.CounterSenderCons)
	if consumed <= 64 || sentPos <= consumed {
		t.Fatalf("setup: consumed %d, sent %d", consumed, sentPos)
	}
	nak := func(session int32, pos int64, length int32) {
		b := proto.EncodeNak(proto.Nak{SessionID: session, StreamID: 1, TermID: initial + int32(pos>>16), TermOffset: int32(pos & (1<<16 - 1)), Length: length})
		p.net.inject(subAddr, senderAddr, b)
		p.runUntil(5*time.Millisecond, func() bool { return false })
	}
	_ = subAddr
	before := snd.Stats()
	nak(session, consumed-64, 64)     // data the receiver has already consumed: it may be gone
	nak(session, consumed+40, 64)     // not at a frame boundary
	nak(session, sentPos+1024, 64)    // data that was never sent
	nak(session, sentPos+(1<<20), 64) // far in the future
	nak(session, consumed, 1<<20)     // longer than a term
	after := snd.Stats()
	if after.NaksRejected-before.NaksRejected != 5 {
		t.Fatalf("%d of 5 bad NAKs were refused (stats %+v)", after.NaksRejected-before.NaksRejected, after)
	}
	if after.RetransmitDatagrams != before.RetransmitDatagrams {
		t.Fatal("a refused NAK caused a retransmission")
	}
	nak(session+1, consumed, 64) // another session's: not for this sender at all
	if snd.Stats().Malformed <= before.Malformed {
		t.Fatal("a NAK for another session was not counted as malformed")
	}
	// A good one: from the consumption position to the sender's position, both frame boundaries.
	good := int32(sentPos - consumed)
	nak(session, consumed, good)
	if got := snd.Stats().RetransmitDatagrams; got <= before.RetransmitDatagrams {
		t.Fatal("a valid NAK caused no retransmission")
	}
	// and a repeat within the linger time is suppressed
	nak(session, consumed, good)
	if snd.Stats().NaksSuppressed == before.NaksSuppressed {
		t.Fatal("a repeated NAK was not suppressed")
	}
	// the stream is unharmed
	p.publish(0, 20)
	if !p.deliveredAll(10*time.Second, 90) {
		t.Fatalf("the stream broke: %d of 90", p.v.total)
	}
}

// A publisher that restarts is a new session: the subscriber gets a second image, the old
// one goes away when its sender has been silent too long, and nothing is lost or mixed up.
func TestSimPublisherRestart(t *testing.T) {
	p := newSimPair(t, 17, nil)
	p.publish(0, 30)
	if !p.deliveredAll(10*time.Second, 30) {
		t.Fatal("setup")
	}
	oldSession := p.pub.SessionID()
	p.pubD.c.Close()
	p.pubD.d.Close()
	p.drivers = p.drivers[1:] // the world no longer runs it (drivers[0] was the publisher)
	newPubD := p.addDriver()
	pub2, err := newPubD.c.AddPublication("udp://"+p.sub.LocalAddr(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if pub2.SessionID() == oldSession {
		t.Fatal("a restarted publisher reused the session")
	}
	if !p.runUntil(10*time.Second, func() bool { return pub2.IsConnected() && p.sub.ImageCount() == 2 }) {
		t.Fatalf("the new publisher did not get an image (images %d)", p.sub.ImageCount())
	}
	for i := 0; i < 40; i++ {
		for {
			if _, res := pub2.Offer(makeMsg(1, i)); res == Offered {
				break
			}
			p.pollOnce()
			p.advance()
		}
	}
	if !p.runUntil(30*time.Second, func() bool { p.pollOnce(); return p.v.next[1] == 40 }) {
		t.Fatalf("delivered %d of the new publisher's 40 messages", p.v.next[1])
	}
	if !p.runUntil(30*time.Second, func() bool { p.pollOnce(); return p.sub.ImageCount() == 1 }) {
		t.Fatalf("the dead publisher's image never went away (%d images)", p.sub.ImageCount())
	}
	if p.v.next[0] != 30 {
		t.Fatalf("messages of the first publisher changed: %d", p.v.next[0])
	}
}

// A subscriber driver that restarts on its address is joined by the publisher again, at
// the sender's position at that moment.
func TestSimSubscriberRestart(t *testing.T) {
	p := newSimPair(t, 19, nil)
	addr := p.sub.LocalAddr()
	p.publish(0, 20)
	if !p.deliveredAll(10*time.Second, 20) {
		t.Fatal("setup")
	}
	p.sub.Close()
	p.subD.c.Close()
	p.subD.d.Close()
	p.drivers = p.drivers[:1]
	if !p.runUntil(10*time.Second, func() bool { return !p.pub.IsConnected() }) {
		t.Fatal("the publisher did not notice the receiver is gone")
	}
	subD2 := p.addDriver()
	sub2, err := subD2.c.AddSubscription("udp://"+addr, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !p.runUntil(10*time.Second, func() bool { return p.pub.IsConnected() && sub2.ImageCount() == 1 }) {
		t.Fatal("the publisher did not reconnect")
	}
	v := newVerifier(t, 1)
	v.next[0] = 20 // the new image starts where the sender is: the next message is number 20
	for i := 20; i < 60; i++ {
		for {
			if _, res := p.pub.Offer(makeMsg(0, i)); res == Offered {
				break
			}
			sub2.Poll(v.handle, 64)
			p.advance()
		}
	}
	if !p.runUntil(30*time.Second, func() bool { sub2.Poll(v.handle, 64); return v.total == 40 }) {
		t.Fatalf("the restarted subscriber got %d of 40 messages", v.total)
	}
}

// A subscription added while a stream is flowing joins at the current position.
func TestSimLateSubscriptionOnTheSameEndpoint(t *testing.T) {
	p := newSimPair(t, 23, nil)
	p.publish(0, 40)
	if !p.deliveredAll(10*time.Second, 40) {
		t.Fatal("setup")
	}
	late, err := p.subD.c.AddSubscription("udp://"+p.sub.LocalAddr(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if !p.runUntil(10*time.Second, func() bool { return late.ImageCount() == 1 }) {
		t.Fatal("the late subscription got no image")
	}
	v := newVerifier(t, 4)
	v.next[0] = 40
	for i := 40; i < 80; i++ {
		for {
			if _, res := p.pub.Offer(makeMsg(0, i)); res == Offered {
				break
			}
			p.pollOnce()
			late.Poll(v.handle, 64)
			p.advance()
		}
	}
	if !p.runUntil(30*time.Second, func() bool { p.pollOnce(); late.Poll(v.handle, 64); return v.total == 40 && p.v.total == 80 }) {
		t.Fatalf("late %d of 40, first %d of 80", v.total, p.v.total)
	}
}

func TestSimManyTermsWithLoss(t *testing.T) {
	r := runTransfer(t, 31, 0.1, 0.05, 4*time.Millisecond, 4000, func(c *driver.Config) {
		c.TermLength = 1 << 16
		c.PublicationWindow = 1 << 14
		c.ReceiverWindow = 1 << 14
	})
	if r.sender.RetransmitDatagrams == 0 {
		t.Fatal("no retransmissions in a lossy run")
	}
	_ = logbuf.HeaderLength
	_ = fmt.Sprint
}

// A retransmission must stop where the sender's own position is: the log holds frames the
// publisher wrote ahead of the sender, and a retransmitted datagram that carried them would
// ignore the flow-control window. (Found by the seed sweep, seed 485: the receiver then
// reported a position beyond anything the sender had sent, and the stream wedged for good.)
func TestSimRetransmissionStopsAtTheSenderPosition(t *testing.T) {
	p := newSimPair(t, 29, nil)
	var maxEnd int64
	var offered []int64
	p.net.trace = func(now int64, from, to netip.AddrPort, b []byte) {
		if h, err := proto.CheckData(b); err == nil && isData(b) {
			end := int64(h.TermOffset) + int64(proto.DataSpan(b)) // everything here is in term 0
			if end > maxEnd {
				maxEnd = end
			}
		}
	}
	// The subscriber reads nothing. The sender may send a receiver window past consumption,
	// the publisher may write a (larger) publication window past it: the log fills with
	// frames that have not been sent.
	if !p.runUntil(5*time.Second, func() bool { return p.pub.PositionLimit() > 0 }) {
		t.Fatal("the publication limit never opened")
	}
	var senderAddr, subAddr netip.AddrPort
	for a := range p.net.conns {
		if int(a.Port()) == mustPort(t, p.sub.LocalAddr()) {
			subAddr = a
		} else {
			senderAddr = a
		}
	}
	for i := 0; i < 190; i++ {
		pos, res := p.pub.Offer(make([]byte, 100)) // 160-byte frames: boundaries every 160 bytes
		if res != Offered {
			break
		}
		offered = append(offered, pos)
		p.runUntil(time.Millisecond, func() bool { return false })
	}
	p.runUntil(200*time.Millisecond, func() bool { return false })
	sentPos := ctr(p.pubD.c, driver.CounterSenderPos)
	if p.pub.Position() <= sentPos+2000 {
		t.Fatalf("setup: the publisher is at %d, the sender at %d: no unsent backlog to expose the bug", p.pub.Position(), sentPos)
	}
	if sentPos%160 != 0 {
		t.Fatalf("setup: sender position %d is not a frame boundary", sentPos)
	}
	start := sentPos - 2*160 // the last two frames that were sent
	wireBefore := maxEnd
	nak := proto.EncodeNak(proto.Nak{SessionID: p.pub.SessionID(), StreamID: 1, TermID: p.pub.log.InitialTermID(), TermOffset: int32(start), Length: int32(sentPos - start)})
	p.net.inject(subAddr, senderAddr, nak)
	p.runUntil(50*time.Millisecond, func() bool { return false })
	if p.pubD.d.Sender().Stats().RetransmitDatagrams == 0 {
		t.Fatal("the valid NAK was not served")
	}
	if maxEnd > sentPos && maxEnd > wireBefore {
		t.Fatalf("a retransmission carried data up to %d, beyond the sender's position %d", maxEnd, sentPos)
	}
}

// Seeds that once broke the stream, kept as regression tests.
func TestSimRegressionSeeds(t *testing.T) {
	for _, seed := range []int64{485} {
		drop := float64(seed%7) * 0.05
		dup := float64(seed%4) * 0.05
		delay := time.Duration(seed%5) * 2 * time.Millisecond
		runTransfer(t, seed, drop, dup, delay, 300, nil)
	}
}
