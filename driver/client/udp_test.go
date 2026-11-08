package client

import (
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xiaobaowan1988/myraft/driver"
	"github.com/xiaobaowan1988/myraft/driver/counters"
	"github.com/xiaobaowan1988/myraft/driver/logbuf"
	"github.com/xiaobaowan1988/myraft/driver/proto"
	"github.com/xiaobaowan1988/myraft/driver/shm"
)

func shmHeap(n int) shm.Buffer { return shm.Wrap(shm.Heap(n)) }

// fastUDP shortens the protocol timers so tests run in milliseconds.
func fastUDP(c *driver.Config) {
	c.TermLength = 1 << 20
	c.MTU = 1408
	c.PublicationWindow = 1 << 18
	c.ReceiverWindow = 1 << 16
	c.SetupInterval = 5 * time.Millisecond
	c.StatusInterval = 2 * time.Millisecond
	c.HeartbeatInterval = 20 * time.Millisecond
	c.ReceiverTimeout = 3 * time.Second
	c.ImageLivenessTimeout = 3 * time.Second
}

// udpPair is a publishing driver and a subscribing driver, each with a client, as if on two machines.
type udpPair struct {
	pubDrv, subDrv *driver.MediaDriver
	pubC, subC     *Client
}

func newUDPPair(t *testing.T, mod func(*driver.Config)) *udpPair {
	t.Helper()
	cfg := func(c *driver.Config) {
		fastUDP(c)
		if mod != nil {
			mod(c)
		}
	}
	p := &udpPair{pubDrv: launch(t, cfg), subDrv: launch(t, cfg)}
	p.pubC = connect(t, p.pubDrv, nil)
	p.subC = connect(t, p.subDrv, nil)
	stallDump = func(t testing.TB) {
		t.Logf("sender %+v", p.pubDrv.Sender().Stats())
		t.Logf("receiver %+v", p.subDrv.Receiver().Stats())
		for _, c := range []*Client{p.pubC, p.subC} {
			c.Counters().ForEach(func(i counters.Info) { t.Logf("counter %d %s = %d", i.TypeID, i.Label, i.Value) })
		}
	}
	t.Cleanup(func() { stallDump = nil })
	return p
}

// open makes a subscription on an ephemeral port and a publication towards it.
func (p *udpPair) open(t *testing.T, stream int32) (*Publication, *Subscription) {
	t.Helper()
	sub, err := p.subC.AddSubscription("udp://127.0.0.1:0", stream)
	if err != nil {
		t.Fatal(err)
	}
	if sub.LocalAddr() == "" {
		t.Fatal("a UDP subscription did not report the address it is bound to")
	}
	pub, err := p.pubC.AddPublication("udp://"+sub.LocalAddr(), stream)
	if err != nil {
		t.Fatal(err)
	}
	return pub, sub
}

func TestUDPPublishAndSubscribe(t *testing.T) {
	p := newUDPPair(t, nil)
	pub, sub := p.open(t, 1)
	waitConnected(t, pub)
	const n = 3000
	v := newVerifier(t, 1)
	go func() {
		for s := 0; s < n; s++ {
			offer(t, pub, makeMsg(0, s))
		}
	}()
	pollUntil(t, sub, v.handle, func() bool { return v.total == n }, "all messages over UDP")
	if sub.ImageCount() != 1 {
		t.Fatalf("image count %d", sub.ImageCount())
	}
	ss, rs := p.pubDrv.Sender().Stats(), p.subDrv.Receiver().Stats()
	if ss.SetupsSent == 0 || ss.DatagramsSent == 0 || rs.StatusSent == 0 || rs.FramesInserted == 0 {
		t.Fatalf("sender %+v receiver %+v: the protocol did not run as expected", ss, rs)
	}
	t.Logf("%d messages, %d bytes sent in %d datagrams (%d heartbeats), %d status messages", n, ss.BytesSent, ss.DatagramsSent, ss.HeartbeatsSent, rs.StatusSent)
}

func TestUDPInOneDriverAndInSharedThreadingMode(t *testing.T) {
	d := launch(t, func(c *driver.Config) { fastUDP(c); c.Threading = driver.Shared })
	c := connect(t, d, nil)
	sub, err := c.AddSubscription("udp://127.0.0.1:0", 1)
	if err != nil {
		t.Fatal(err)
	}
	pub, err := c.AddPublication("udp://"+sub.LocalAddr(), 1)
	if err != nil {
		t.Fatal(err)
	}
	waitConnected(t, pub)
	v := newVerifier(t, 1)
	go func() {
		for s := 0; s < 500; s++ {
			offer(t, pub, makeMsg(0, s))
		}
	}()
	pollUntil(t, sub, v.handle, func() bool { return v.total == 500 }, "messages through one shared-thread driver")
}

func TestUDPSubscriberAfterPublisher(t *testing.T) {
	p := newUDPPair(t, nil)
	// The subscriber must exist to know its port, so create it, then drop it, and
	// start the publication first towards a fixed port.
	l, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	port := l.LocalAddr().(*net.UDPAddr).Port
	l.Close()
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	pub, err := p.pubC.AddPublication("udp://"+addr, 4)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // Setups go out into nothing
	if _, res := pub.Offer([]byte("x")); res != NotConnected {
		t.Fatalf("Offer with no receiver gave %v", res)
	}
	sub, err := p.subC.AddSubscription("udp://"+addr, 4)
	if err != nil {
		t.Fatal(err)
	}
	waitConnected(t, pub)
	offer(t, pub, []byte("hello"))
	var got string
	pollUntil(t, sub, func(m []byte, _ logbuf.FrameInfo) { got = string(m) }, func() bool { return got != "" }, "a message to a late subscriber")
	if got != "hello" {
		t.Fatalf("got %q", got)
	}
}

func TestUDPLargeMessagesCrossManyDatagrams(t *testing.T) {
	p := newUDPPair(t, nil)
	pub, sub := p.open(t, 1)
	waitConnected(t, pub)
	size := pub.MaxMessageLength()
	var want [][]byte
	for i := 0; i < 8; i++ {
		m := make([]byte, size-i*1000)
		for j := range m {
			m[j] = byte(j*3 + i)
		}
		want = append(want, m)
	}
	go func() {
		for _, m := range want {
			offer(t, pub, m)
		}
	}()
	var got [][]byte
	pollUntil(t, sub, func(m []byte, _ logbuf.FrameInfo) { got = append(got, append([]byte(nil), m...)) }, func() bool { return len(got) == len(want) }, "large messages")
	for i := range want {
		if string(got[i]) != string(want[i]) {
			t.Fatalf("large message %d (%d bytes) came out different (%d bytes)", i, len(want[i]), len(got[i]))
		}
	}
}

// How much may be in flight is bounded twice over: the receiver's window (what the
// sender may send past the subscriber's position) and the publication window (what
// the publisher may write past it).
func TestUDPFlowControl(t *testing.T) {
	const recvWindow, pubWindow = 1 << 16, 1 << 18
	p := newUDPPair(t, func(c *driver.Config) { c.ReceiverWindow = recvWindow; c.PublicationWindow = pubWindow })
	pub, sub := p.open(t, 1)
	waitConnected(t, pub)
	eventually(t, 5*time.Second, "the conductor to open the publication limit", func() bool { return pub.PositionLimit() > 0 })
	msg := make([]byte, 1000)
	accepted := 0
	for i := 0; ; i++ {
		_, res := pub.Offer(msg)
		if res == BackPressured {
			break
		}
		if res != Offered {
			if i > 100000 {
				t.Fatalf("Offer gave %v", res)
			}
			time.Sleep(100 * time.Microsecond)
			continue
		}
		accepted++
	}
	// The subscriber has read nothing: the publisher stopped one window ahead of it.
	if pos := pub.Position(); pos < pubWindow-2048 || pos > pubWindow+2048 {
		t.Fatalf("the publisher stopped at %d, want about the publication window %d", pos, pubWindow)
	}
	// ...and the receiver holds no more than its window, however long we wait.
	time.Sleep(200 * time.Millisecond)
	var hwm, subPos int64
	p.subC.Counters().ForEach(func(i counters.Info) {
		switch i.TypeID {
		case driver.CounterRcvHWM:
			hwm = i.Value
		case driver.CounterSubPos:
			subPos = i.Value
		}
	})
	if hwm-subPos > recvWindow {
		t.Fatalf("the receiver holds %d bytes beyond the subscriber, window is %d", hwm-subPos, recvWindow)
	}
	if hwm-subPos < recvWindow/2 {
		t.Fatalf("the sender only got %d bytes through of a window of %d: flow control is too tight", hwm-subPos, recvWindow)
	}
	// Reading releases it all.
	got := 0
	pollUntil(t, sub, func([]byte, logbuf.FrameInfo) { got++ }, func() bool { return got == accepted }, "the backlog to arrive")
	eventually(t, 5*time.Second, "the publisher to be released", func() bool { _, res := pub.Offer(msg); return res == Offered })
}

func TestUDPStreamsAndSessionsAreIndependent(t *testing.T) {
	a, b := newUDPPair(t, nil), newUDPPair(t, nil)
	_ = b
	// two streams to one endpoint from one publisher driver
	sub1, err := a.subC.AddSubscription("udp://127.0.0.1:0", 1)
	if err != nil {
		t.Fatal(err)
	}
	sub2, err := a.subC.AddSubscription("udp://"+sub1.LocalAddr(), 2) // same endpoint, another stream
	if err != nil {
		t.Fatal(err)
	}
	pub1, _ := a.pubC.AddPublication("udp://"+sub1.LocalAddr(), 1)
	pub2, _ := a.pubC.AddPublication("udp://"+sub1.LocalAddr(), 2)
	waitConnected(t, pub1)
	waitConnected(t, pub2)
	for i := 0; i < 100; i++ {
		offer(t, pub1, []byte(fmt.Sprintf("one-%d", i)))
		offer(t, pub2, []byte(fmt.Sprintf("two-%d", i)))
	}
	for sub, prefix := range map[*Subscription]string{sub1: "one-", sub2: "two-"} {
		var got []string
		pollUntil(t, sub, func(m []byte, _ logbuf.FrameInfo) { got = append(got, string(m)) }, func() bool { return len(got) == 100 }, "stream messages")
		for i, m := range got {
			if m != fmt.Sprintf("%s%d", prefix, i) {
				t.Fatalf("message %d is %q", i, m)
			}
		}
	}
	// two publisher drivers (two sessions) into one subscription: two images
	third := launch(t, fastUDP)
	thirdC := connect(t, third, nil)
	sub3, _ := a.subC.AddSubscription("udp://127.0.0.1:0", 9)
	pubA, _ := a.pubC.AddPublication("udp://"+sub3.LocalAddr(), 9)
	pubB, err := thirdC.AddPublication("udp://"+sub3.LocalAddr(), 9)
	if err != nil {
		t.Fatal(err)
	}
	waitConnected(t, pubA)
	waitConnected(t, pubB)
	eventually(t, 5*time.Second, "two images", func() bool { return sub3.ImageCount() == 2 })
	const per = 300
	for src, pub := range []*Publication{pubA, pubB} {
		go func(src int, pub *Publication) {
			for s := 0; s < per; s++ {
				offer(t, pub, makeMsg(src, s))
			}
		}(src, pub)
	}
	v := newVerifier(t, 2)
	pollUntil(t, sub3, v.handle, func() bool { return v.total == 2*per }, "messages of two sessions")
}

func TestUDPMalformedDatagramsAreIgnored(t *testing.T) {
	p := newUDPPair(t, nil)
	pub, sub := p.open(t, 1)
	waitConnected(t, pub)
	target := netip.MustParseAddrPort(sub.LocalAddr())
	c, err := net.DialUDP("udp4", nil, net.UDPAddrFromAddrPort(target))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	garbage := [][]byte{{}, {1}, make([]byte, 31), make([]byte, 32), make([]byte, 33), make([]byte, 1400)}
	rnd := make([]byte, 200)
	for i := range rnd {
		rnd[i] = byte(i * 37)
	}
	garbage = append(garbage, rnd)
	for i := 0; i < 20; i++ {
		for _, g := range garbage {
			c.Write(g)
		}
	}
	offer(t, pub, []byte("after the garbage"))
	var got string
	pollUntil(t, sub, func(m []byte, _ logbuf.FrameInfo) { got = string(m) }, func() bool { return got != "" }, "a message after garbage")
	if rs := p.subDrv.Receiver().Stats(); rs.Malformed == 0 {
		t.Fatalf("garbage was not noticed: %+v", rs)
	}
}

func TestUDPPublisherGoneImageDrainsAndCloses(t *testing.T) {
	p := newUDPPair(t, func(c *driver.Config) { c.ImageLivenessTimeout = 300 * time.Millisecond })
	pub, sub := p.open(t, 1)
	waitConnected(t, pub)
	const n = 50
	for i := 0; i < n; i++ {
		offer(t, pub, []byte(fmt.Sprintf("message-%d", i)))
	}
	pub.Close()
	p.pubC.Close()
	p.pubDrv.Close() // the sender is gone: no more heartbeats
	var got []string
	pollUntil(t, sub, func(m []byte, _ logbuf.FrameInfo) { got = append(got, string(m)) }, func() bool { return len(got) == n }, "everything sent before the publisher went")
	eventually(t, 10*time.Second, "the image to close", func() bool { return sub.ImageCount() == 0 })
	eventually(t, 5*time.Second, "the receiver's log file to be removed", func() bool {
		files, _ := filepath.Glob(filepath.Join(p.subDrv.Dir(), "img-*.log"))
		return len(files) == 0
	})
	if s := p.subDrv.Conductor().Stats(); s.Images != 0 {
		t.Fatalf("stats %+v", s)
	}
}

func TestUDPSubscriberGonePublisherDisconnects(t *testing.T) {
	p := newUDPPair(t, func(c *driver.Config) { c.ReceiverTimeout = 300 * time.Millisecond })
	pub, sub := p.open(t, 1)
	waitConnected(t, pub)
	offer(t, pub, []byte("x"))
	sub.Close()
	p.subC.Close()
	p.subDrv.Close()
	eventually(t, 5*time.Second, "the publisher to notice the receiver is gone", func() bool { return !pub.IsConnected() })
	if _, res := pub.Offer([]byte("x")); res != NotConnected {
		t.Fatalf("Offer gave %v", res)
	}
}

// A subscriber driver that restarts on the same port gets the stream again, from the
// position the sender is at then (a late joiner): earlier messages are not replayed.
func TestUDPReceiverRestart(t *testing.T) {
	p := newUDPPair(t, func(c *driver.Config) { c.ReceiverTimeout = 300 * time.Millisecond })
	pub, sub := p.open(t, 1)
	waitConnected(t, pub)
	addr := sub.LocalAddr()
	offer(t, pub, []byte("before"))
	var got []string
	pollUntil(t, sub, func(m []byte, _ logbuf.FrameInfo) { got = append(got, string(m)) }, func() bool { return len(got) == 1 }, "the first message")
	sub.Close()
	p.subC.Close()
	p.subDrv.Close()
	eventually(t, 5*time.Second, "the disconnect", func() bool { return !pub.IsConnected() })

	cfg := smallConfig(filepath.Join(t.TempDir(), "driver2"))
	fastUDP(&cfg)
	d2, err := driver.Launch(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d2.Close() })
	c2 := connect(t, d2, nil)
	sub2, err := c2.AddSubscription("udp://"+addr, 1) // the same port again
	if err != nil {
		t.Fatal(err)
	}
	waitConnected(t, pub)
	got = nil
	for i := 0; i < 20; i++ {
		offer(t, pub, []byte(fmt.Sprintf("after-%d", i)))
	}
	pollUntil(t, sub2, func(m []byte, _ logbuf.FrameInfo) { got = append(got, string(m)) }, func() bool { return len(got) >= 20 }, "messages after the restart")
	if len(got) != 20 || got[0] != "after-0" || got[19] != "after-19" {
		t.Fatalf("got %q", got)
	}
}

func TestUDPCleanupWhenBothSidesRemove(t *testing.T) {
	p := newUDPPair(t, nil)
	pub, sub := p.open(t, 1)
	waitConnected(t, pub)
	offer(t, pub, []byte("x"))
	n := 0
	pollUntil(t, sub, func([]byte, logbuf.FrameInfo) { n++ }, func() bool { return n == 1 }, "the message")
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	if err := pub.Close(); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "both drivers to release everything", func() bool {
		a, b := p.pubDrv.Conductor().Stats(), p.subDrv.Conductor().Stats()
		return a.Publications == 0 && b.Publications == 0 && b.Images == 0 && b.Endpoints == 0
	})
	for _, d := range []*driver.MediaDriver{p.pubDrv, p.subDrv} {
		if files, _ := filepath.Glob(filepath.Join(d.Dir(), "*.log")); len(files) != 0 {
			t.Fatalf("log files left in %s: %v", d.Dir(), files)
		}
	}
	// the subscriber's port is free again
	if l, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: mustPort(t, sub.LocalAddr())}); err != nil {
		t.Fatalf("the subscription's socket was not closed: %v", err)
	} else {
		l.Close()
	}
	_ = os.Stderr
}

func mustPort(t *testing.T, addr string) int {
	t.Helper()
	ap, err := netip.ParseAddrPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	return int(ap.Port())
}

func TestUDPBadChannels(t *testing.T) {
	d := launch(t, fastUDP)
	c := connect(t, d, nil)
	for _, ch := range []string{"udp://localhost:4000", "udp://127.0.0.1", "udp://127.0.0.1:0", "tcp://127.0.0.1:4000"} {
		if _, err := c.AddPublication(ch, 1); err == nil {
			t.Errorf("publication to %q was accepted", ch)
		}
	}
	if _, err := c.AddSubscription("udp://999.0.0.1:5", 1); err == nil {
		t.Error("a subscription on a bad address was accepted")
	}
}

// An idle stream is kept alive by heartbeats: without them the receiver would give up
// on the sender after ImageLivenessTimeout.
func TestUDPIdleStreamStaysAlive(t *testing.T) {
	p := newUDPPair(t, func(c *driver.Config) { c.ImageLivenessTimeout = 300 * time.Millisecond })
	pub, sub := p.open(t, 1)
	waitConnected(t, pub)
	offer(t, pub, []byte("first"))
	time.Sleep(900 * time.Millisecond) // three liveness timeouts of silence from the application
	if sub.ImageCount() != 1 {
		t.Fatalf("the image went away during an idle period (%d images)", sub.ImageCount())
	}
	offer(t, pub, []byte("second"))
	var got []string
	pollUntil(t, sub, func(m []byte, _ logbuf.FrameInfo) { got = append(got, string(m)) }, func() bool { return len(got) == 2 }, "messages around the idle period")
	if p.pubDrv.Sender().Stats().HeartbeatsSent == 0 {
		t.Fatal("no heartbeats were sent")
	}
}

// Many terms through a small log: the receiver must clean each term before it is
// reused, or stale frames would be taken for new ones.
func TestUDPDataSpanningManyTerms(t *testing.T) {
	p := newUDPPair(t, func(c *driver.Config) {
		c.TermLength = 1 << 16
		c.MTU = 1024
		c.PublicationWindow = 1 << 14
		c.ReceiverWindow = 1 << 14
	})
	pub, sub := p.open(t, 1)
	waitConnected(t, pub)
	const n = 6000
	v := newVerifier(t, 1)
	go func() {
		for s := 0; s < n; s++ {
			offer(t, pub, makeMsgUpTo(0, s, 900))
		}
	}()
	pollUntil(t, sub, v.handle, func() bool { return v.total == n }, "messages across many terms")
	if terms := pub.Position() >> 16; terms < 20 {
		t.Fatalf("only %d terms were used", terms)
	}
}

func makeMsgUpTo(p, s, max int) []byte {
	m := makeMsg(p, s)
	if len(m) > max {
		// shrink while keeping the self-describing header valid
		size := 12 + (len(m)-12)%(max-12)
		m = m[:size]
		m[8], m[9], m[10], m[11] = byte(size>>24), byte(size>>16), byte(size>>8), byte(size)
		for i := 12; i < size; i++ {
			m[i] = byte(p + s + i)
		}
	}
	return m
}

// Creating and destroying publications and subscriptions quickly exercises the
// hand-over of logs and sockets between the conductor, sender and receiver.
func TestUDPChurn(t *testing.T) {
	p := newUDPPair(t, nil)
	for i := 0; i < 30; i++ {
		pub, sub := p.open(t, int32(i%3))
		waitConnected(t, pub)
		for j := 0; j < 5; j++ {
			offer(t, pub, []byte(fmt.Sprintf("round %d message %d", i, j)))
		}
		if i%2 == 0 { // sometimes read, sometimes leave without reading
			n := 0
			pollUntil(t, sub, func([]byte, logbuf.FrameInfo) { n++ }, func() bool { return n == 5 }, "the messages of a round")
		}
		if i%3 == 0 {
			pub.Close()
			sub.Close()
		} else {
			sub.Close()
			pub.Close()
		}
	}
	eventually(t, 20*time.Second, "everything to be released", func() bool {
		a, b := p.pubDrv.Conductor().Stats(), p.subDrv.Conductor().Stats()
		return a.Publications == 0 && b.Publications == 0 && b.Images == 0 && b.Endpoints == 0
	})
}

// --- a hand-made sender ----------------------------------------------------------

// rogue plays the sending side of the protocol by hand, so a test can do what a real
// sender would never do (skip ahead, spoof, mis-state its parameters) and check the
// receiver's rules one at a time.
type rogue struct {
	t       *testing.T
	sock    *net.UDPConn
	dest    *net.UDPAddr
	pl      *logbuf.LogBuffer // the log the frames come from
	session int32
	stream  int32
	initial int32
	termLen int
	limit   int64 // what the last Status allows: consumption + window
	cons    int64
	window  int64
	sent    int64 // position up to which datagrams have been sent
}

func newRogue(t *testing.T, dest string, stream int32, termLen, mtu int) *rogue {
	t.Helper()
	sock, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sock.Close() })
	d, _ := net.ResolveUDPAddr("udp4", dest)
	r := &rogue{t: t, sock: sock, dest: d, session: 77, stream: stream, initial: 100, termLen: termLen}
	r.pl, err = logbuf.Create(shmHeap(logbuf.FileLength(termLen)), logbuf.Params{TermLength: termLen, MTU: mtu, InitialTermID: r.initial, SessionID: r.session, StreamID: stream})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func (r *rogue) setup(termLen, mtu int, pos int64) {
	r.t.Helper()
	b := proto.EncodeSetup(proto.Setup{SessionID: r.session, StreamID: r.stream, TermID: r.initial + int32(pos/int64(termLen)),
		TermOffset: int32(pos % int64(termLen)), InitialTermID: r.initial, TermLength: int32(termLen), MTU: int32(mtu)})
	if _, err := r.sock.WriteToUDP(b, r.dest); err != nil {
		r.t.Fatal(err)
	}
}

// status waits for the next Status for this session; ok is false if none arrives in d.
func (r *rogue) status(d time.Duration) (proto.Status, bool) {
	r.sock.SetReadDeadline(time.Now().Add(d))
	buf := make([]byte, 2048)
	for {
		n, _, err := r.sock.ReadFromUDP(buf)
		if err != nil {
			return proto.Status{}, false
		}
		if st, err := proto.ParseStatus(buf[:n]); err == nil && st.SessionID == r.session && st.StreamID == r.stream {
			r.cons = int64(st.TermID-r.initial)*int64(r.termLen) + int64(st.TermOffset)
			r.window = int64(st.Window)
			r.limit = r.cons + r.window
			return st, true
		}
	}
}

// drainStatus applies every Status that is waiting.
func (r *rogue) drainStatus() {
	// the receiver reports every few milliseconds, so stop at the first quiet moment
	for i := 0; i < 20; i++ {
		if _, ok := r.status(300 * time.Microsecond); !ok {
			return
		}
	}
}

func (r *rogue) datagram(pos int64, maxLen int) ([]byte, int) {
	frames, adv := r.pl.Scan(pos, maxLen)
	return append([]byte(nil), frames...), adv
}

func (r *rogue) send(b []byte) {
	r.t.Helper()
	if _, err := r.sock.WriteToUDP(b, r.dest); err != nil {
		r.t.Fatal(err)
	}
}

func receiverCounter(t *testing.T, c *Client, typ int32) int64 {
	t.Helper()
	var v int64 = -1
	c.Counters().ForEach(func(i counters.Info) {
		if i.TypeID == typ {
			v = i.Value
		}
	})
	return v
}

func TestUDPReceiverRulesAgainstAHandMadeSender(t *testing.T) {
	const termLen, mtu, window = 1 << 16, 1024, 1 << 14
	d := launch(t, func(c *driver.Config) {
		fastUDP(c)
		c.TermLength, c.MTU, c.ReceiverWindow, c.PublicationWindow = termLen, mtu, window, window
	})
	c := connect(t, d, nil)
	sub, err := c.AddSubscription("udp://127.0.0.1:0", 1)
	if err != nil {
		t.Fatal(err)
	}
	r := newRogue(t, sub.LocalAddr(), 1, termLen, mtu)
	rs := func() driver.ReceiverStats { return d.Receiver().Stats() }

	// Setups the receiver must refuse: no image, no Status.
	r.setup(termLen+1, mtu, 0) // term length not a power of two
	r.setup(termLen, 100, 0)   // MTU not a multiple of 32
	r.setup(termLen, mtu, 40)  // join point not frame aligned
	other := *r
	other.stream = 2 // a stream nobody subscribed to
	other.setup(termLen, mtu, 0)
	if _, ok := r.status(150 * time.Millisecond); ok {
		t.Fatal("the receiver answered a Setup it should have refused")
	}
	if rs().Malformed < 3 {
		t.Fatalf("bad Setups were not counted as malformed: %+v", rs())
	}

	// A good Setup gets an image and a Status at the join position with the window.
	r.setup(termLen, mtu, 0)
	st, ok := r.status(2 * time.Second)
	if !ok {
		t.Fatal("no Status after a good Setup")
	}
	if r.cons != 0 || st.Window != window {
		t.Fatalf("first Status: consumption %d window %d, want 0 and %d", r.cons, st.Window, window)
	}
	eventually(t, 5*time.Second, "the subscription to get its image", func() bool { return sub.ImageCount() == 1 })

	// 32-byte frames (empty messages) let the test place positions exactly.
	empty := []byte{}
	for r.pl.Position() < termLen-96 {
		if _, st := r.pl.Offer(empty, 1<<60); st != logbuf.Ok && st != logbuf.AdminAction {
			t.Fatal(st)
		}
	}
	if r.pl.Position() != termLen-96 {
		t.Fatalf("setup: log at %d", r.pl.Position())
	}
	// This message needs 160 bytes, 96 are left: the log pads the rest of the term and
	// the message starts the next one.
	firstOfTerm1 := append([]byte("first of term 1"), make([]byte, 85)...)
	for {
		if _, st := r.pl.Offer(firstOfTerm1, 1<<60); st == logbuf.Ok {
			break
		}
	}

	// Data beyond the window is refused: skip ahead past consumption + window.
	far, _ := r.datagram(window+64, mtu)
	r.send(far)
	eventually(t, 5*time.Second, "the receiver to drop a datagram beyond its window", func() bool { return rs().Dropped >= 1 })
	if n, _ := sub.Poll(func([]byte, logbuf.FrameInfo) {}, 10); n != 0 {
		t.Fatal("data beyond the window reached the subscriber")
	}
	// A datagram from another socket is not the sender's.
	spoof, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer spoof.Close()
	first, _ := r.datagram(0, mtu)
	before := rs().Dropped
	spoof.WriteToUDP(first, r.dest)
	eventually(t, 5*time.Second, "a spoofed datagram to be dropped", func() bool { return rs().Dropped > before })
	if receiverCounter(t, c, driver.CounterRcvPos) != 0 {
		t.Fatal("a datagram from the wrong address was accepted")
	}

	// Now send the data properly, obeying the window as a real sender would, in
	// REVERSE order within each window: the gap must be tolerated until it is filled.
	// The subscriber consumes only part of it, so that consumption stops at a chosen
	// place just short of the padding at the end of the term.
	padPos := int64(termLen - 96)
	stopAt := padPos - window + 32 // consumption such that the padding header fits the window exactly
	consumeFrames := int(stopAt / 32)
	consumed := 0
	consume := func() {
		cap := consumeFrames - consumed
		if cap <= 0 {
			return
		}
		n, _ := sub.Poll(func([]byte, logbuf.FrameInfo) { consumed++ }, cap)
		_ = n
	}
	loopDeadline := time.Now().Add(10 * time.Second)
	for r.sent < padPos {
		if time.Now().After(loopDeadline) {
			dg, adv := r.datagram(r.sent, mtu)
			t.Fatalf("stuck: sent %d of %d, consumption %d, limit %d, consumed %d of %d frames, rcv-pos %d; next datagram %d bytes advancing %d; images %d", r.sent, padPos, r.cons, r.limit, consumed, consumeFrames, receiverCounter(t, c, driver.CounterRcvPos), len(dg), adv, sub.ImageCount())
		}
		r.drainStatus()
		var batch [][]byte
		pos := r.sent
		for pos < padPos {
			dg, adv := r.datagram(pos, mtu)
			if adv == 0 || pos+int64(len(dg)) > r.limit {
				break
			}
			batch = append(batch, dg)
			pos += int64(adv)
		}
		for i := len(batch) - 1; i >= 0; i-- {
			r.send(batch[i])
		}
		r.sent = pos
		consume()
		time.Sleep(time.Millisecond)
	}
	eventually(t, 10*time.Second, "everything up to the padding to arrive and the subscriber to stop at the chosen place", func() bool {
		consume()
		return receiverCounter(t, c, driver.CounterRcvPos) == padPos && consumed == consumeFrames
	})
	r.drainStatus()
	if r.cons != stopAt {
		t.Fatalf("consumption is %d, want %d", r.cons, stopAt)
	}

	// The padding frame: 32 bytes on the wire, covering 96 bytes. It starts inside the
	// window (padPos+32 == consumption+window) although the space it covers does not
	// fit. The receiver must take it.
	pad, adv := r.datagram(padPos, mtu)
	if len(pad) != logbuf.HeaderLength || adv != 96 {
		t.Fatalf("setup: the log's next datagram is %d bytes advancing %d, want a 32 byte padding header advancing 96", len(pad), adv)
	}
	dropped := rs().Dropped
	r.send(pad)
	eventually(t, 5*time.Second, "the padding frame to be accepted", func() bool {
		return receiverCounter(t, c, driver.CounterRcvPos) == int64(termLen)
	})
	if rs().Dropped != dropped {
		t.Fatalf("the padding frame was dropped (%d -> %d): a stream would stall at the end of its first term", dropped, rs().Dropped)
	}

	// Release the subscriber: it reads on, over the padding, into the next term.
	var after [][]byte
	pollUntil(t, sub, func([]byte, logbuf.FrameInfo) { consumed++ }, func() bool { return int64(consumed)*32 >= padPos }, "the rest of term 0")
	dg, _ := r.datagram(int64(termLen), mtu)
	eventually(t, 5*time.Second, "the window to open for the next term", func() bool {
		r.drainStatus()
		return int64(termLen)+int64(len(dg)) <= r.limit // a sender obeys the window
	})
	r.send(dg)
	pollUntil(t, sub, func(m []byte, _ logbuf.FrameInfo) { after = append(after, append([]byte(nil), m...)) }, func() bool { return len(after) == 1 }, "the first message of the next term")
	if string(after[0]) != string(firstOfTerm1) {
		t.Fatalf("got %q", after[0])
	}
	// Duplicates change nothing.
	r.send(dg)
	r.send(first)
	time.Sleep(50 * time.Millisecond)
	if n, _ := sub.Poll(func([]byte, logbuf.FrameInfo) {}, 10); n != 0 {
		t.Fatal("a duplicate datagram was delivered again")
	}
}
