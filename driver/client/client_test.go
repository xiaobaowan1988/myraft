package client

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xiaobaowan1988/myraft/driver"
	"github.com/xiaobaowan1988/myraft/driver/cnc"
	"github.com/xiaobaowan1988/myraft/driver/counters"
	"github.com/xiaobaowan1988/myraft/driver/logbuf"
)

// --- fixtures ------------------------------------------------------------------

type fakeClock struct{ ns atomic.Int64 }

func newFakeClock() *fakeClock {
	c := &fakeClock{}
	c.ns.Store(1_000_000_000_000)
	return c
}
func (c *fakeClock) Now() int64              { return c.ns.Load() }
func (c *fakeClock) Advance(d time.Duration) { c.ns.Add(int64(d)) }

func smallConfig(dir string) driver.Config {
	return driver.Config{Dir: dir, TermLength: 1 << 16, MTU: 512, DriverTimeout: 10 * time.Second}
}

func launch(t *testing.T, mod func(*driver.Config)) *driver.MediaDriver {
	t.Helper()
	cfg := smallConfig(filepath.Join(t.TempDir(), "driver"))
	if mod != nil {
		mod(&cfg)
	}
	d, err := driver.Launch(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}

func connect(t *testing.T, d *driver.MediaDriver, mod func(*Options)) *Client {
	t.Helper()
	o := Options{Dir: d.Dir()}
	if mod != nil {
		mod(&o)
	}
	c, err := Connect(o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func mustPub(t *testing.T, c *Client, stream int32) *Publication {
	t.Helper()
	p, err := c.AddPublication("ipc", stream)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func mustSub(t *testing.T, c *Client, stream int32) *Subscription {
	t.Helper()
	s, err := c.AddSubscription("ipc", stream)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// offer retries until the message is in, treating back-pressure as "wait".
func offer(t testing.TB, p *Publication, msg []byte) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		_, res := p.Offer(msg)
		switch res {
		case Offered:
			return
		case NotConnected, BackPressured, AdminAction:
			if time.Now().After(deadline) {
				t.Fatalf("Offer kept returning %v", res)
			}
			time.Sleep(20 * time.Microsecond)
		default:
			t.Fatalf("Offer returned %v", res)
		}
	}
}

// makeMsg builds message s of source p: a header, a size, and a pattern, 12..1511 bytes
// (up to 4 fragments with an MTU of 512).
func makeMsg(p, s int) []byte {
	size := 12 + (p*131+s*37)%1500
	m := make([]byte, size)
	binary.BigEndian.PutUint32(m[0:], uint32(p))
	binary.BigEndian.PutUint32(m[4:], uint32(s))
	binary.BigEndian.PutUint32(m[8:], uint32(size))
	for i := 12; i < size; i++ {
		m[i] = byte(p + s + i)
	}
	return m
}

// verifier checks per-source order, completeness of content and exactly-once delivery.
type verifier struct {
	t     testing.TB
	next  []int
	total int
}

func newVerifier(t testing.TB, sources int) *verifier {
	return &verifier{t: t, next: make([]int, sources)}
}

func (v *verifier) handle(m []byte, _ logbuf.FrameInfo) {
	t := v.t
	if len(m) < 12 {
		t.Errorf("garbled message of %d bytes", len(m))
		return
	}
	p, s, size := int(binary.BigEndian.Uint32(m[0:])), int(binary.BigEndian.Uint32(m[4:])), int(binary.BigEndian.Uint32(m[8:]))
	if p >= len(v.next) || len(m) != size {
		t.Errorf("garbled message: source %d, length %d, header says %d", p, len(m), size)
		return
	}
	if s != v.next[p] {
		t.Errorf("source %d: got sequence %d, want %d", p, s, v.next[p])
		return
	}
	for i := 12; i < size; i++ {
		if m[i] != byte(p+s+i) {
			t.Errorf("source %d seq %d: corrupt byte %d", p, s, i)
			return
		}
	}
	v.next[p]++
	v.total++
}

func pollUntil(t testing.TB, s *Subscription, h Handler, done func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for !done() {
		n, err := s.Poll(h, 64)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			if time.Now().After(deadline) {
				if stallDump != nil {
					stallDump(t)
				}
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(20 * time.Microsecond)
		}
	}
}

// stallDump, when set by a test fixture, logs the state of the system when a poll times out.
var stallDump func(t testing.TB)

func waitConnected(t *testing.T, p *Publication) {
	t.Helper()
	eventually(t, 10*time.Second, "the publication to see a subscriber", p.IsConnected)
}

// --- basic flow ------------------------------------------------------------------

func TestPublishAndSubscribe(t *testing.T) {
	d := launch(t, nil)
	c := connect(t, d, nil)
	sub := mustSub(t, c, 1)
	pub := mustPub(t, c, 1)
	waitConnected(t, pub)
	const n = 3000
	v := newVerifier(t, 1)
	go func() {
		for s := 0; s < n; s++ {
			offer(t, pub, makeMsg(0, s))
		}
	}()
	pollUntil(t, sub, v.handle, func() bool { return v.total == n }, "all messages")
	if sub.ImageCount() != 1 {
		t.Fatalf("image count %d, want 1", sub.ImageCount())
	}
	t.Logf("%d messages through %d bytes of log", n, pub.Position())
}

func TestSubscriptionBeforePublicationAndNotConnected(t *testing.T) {
	d := launch(t, nil)
	c := connect(t, d, nil)
	pub := mustPub(t, c, 5)
	if _, res := pub.Offer([]byte("x")); res != NotConnected {
		t.Fatalf("Offer without a subscriber gave %v, want NotConnected", res)
	}
	sub := mustSub(t, c, 5)
	waitConnected(t, pub)
	eventually(t, 5*time.Second, "the image", func() bool { return sub.ImageCount() == 1 })
	offer(t, pub, []byte("hello"))
	var got string
	pollUntil(t, sub, func(m []byte, _ logbuf.FrameInfo) { got = string(m) }, func() bool { return got != "" }, "the message")
	if got != "hello" {
		t.Fatalf("got %q", got)
	}
	// and the other order: the subscription exists first
	sub2 := mustSub(t, c, 6)
	if sub2.ImageCount() != 0 {
		t.Fatal("an image with no publication")
	}
	pub2 := mustPub(t, c, 6)
	eventually(t, 5*time.Second, "the image of a later publication", func() bool { return sub2.ImageCount() == 1 && pub2.IsConnected() })
}

func TestLateJoinerSeesOnlyWhatIsPublishedAfterJoining(t *testing.T) {
	d := launch(t, nil)
	c := connect(t, d, nil)
	pub := mustPub(t, c, 1)
	first := mustSub(t, c, 1)
	waitConnected(t, pub)
	for i := 0; i < 10; i++ {
		offer(t, pub, []byte(fmt.Sprintf("early-%d", i)))
	}
	var firstGot []string
	pollUntil(t, first, func(m []byte, _ logbuf.FrameInfo) { firstGot = append(firstGot, string(m)) }, func() bool { return len(firstGot) == 10 }, "early messages")

	late := mustSub(t, c, 1)
	eventually(t, 5*time.Second, "the late image", func() bool { return late.ImageCount() == 1 })
	for i := 0; i < 10; i++ {
		offer(t, pub, []byte(fmt.Sprintf("late-%d", i)))
	}
	var lateGot []string
	pollUntil(t, late, func(m []byte, _ logbuf.FrameInfo) { lateGot = append(lateGot, string(m)) }, func() bool { return len(lateGot) >= 10 }, "late messages")
	time.Sleep(20 * time.Millisecond)
	late.Poll(func(m []byte, _ logbuf.FrameInfo) { lateGot = append(lateGot, string(m)) }, 100)
	if len(lateGot) != 10 || lateGot[0] != "late-0" || lateGot[9] != "late-9" {
		t.Fatalf("the late subscriber got %q, want exactly late-0..late-9", lateGot)
	}
}

func TestStreamsAreIndependent(t *testing.T) {
	d := launch(t, nil)
	c := connect(t, d, nil)
	subs := map[int32]*Subscription{1: mustSub(t, c, 1), 2: mustSub(t, c, 2)}
	pubs := map[int32]*Publication{1: mustPub(t, c, 1), 2: mustPub(t, c, 2)}
	waitConnected(t, pubs[1])
	waitConnected(t, pubs[2])
	for i := 0; i < 50; i++ {
		offer(t, pubs[1], []byte(fmt.Sprintf("one-%d", i)))
		offer(t, pubs[2], []byte(fmt.Sprintf("two-%d", i)))
	}
	for stream, prefix := range map[int32]string{1: "one-", 2: "two-"} {
		var got []string
		pollUntil(t, subs[stream], func(m []byte, _ logbuf.FrameInfo) { got = append(got, string(m)) }, func() bool { return len(got) == 50 }, "stream messages")
		for i, m := range got {
			if m != fmt.Sprintf("%s%d", prefix, i) {
				t.Fatalf("stream %d message %d is %q", stream, i, m)
			}
		}
	}
}

func TestTooBigMessage(t *testing.T) {
	d := launch(t, nil)
	c := connect(t, d, nil)
	mustSub(t, c, 1)
	pub := mustPub(t, c, 1)
	waitConnected(t, pub)
	if _, res := pub.Offer(make([]byte, pub.MaxMessageLength()+1)); res != TooBig {
		t.Fatalf("got %v, want TooBig", res)
	}
}

// --- flow control -------------------------------------------------------------

func TestBackPressureFollowsTheSlowestSubscriber(t *testing.T) {
	const window = 1 << 14
	d := launch(t, func(c *driver.Config) { c.PublicationWindow = window })
	c := connect(t, d, nil)
	fast, slow := mustSub(t, c, 1), mustSub(t, c, 1)
	pub := mustPub(t, c, 1)
	waitConnected(t, pub)
	eventually(t, 5*time.Second, "both images", func() bool { return fast.ImageCount() == 1 && slow.ImageCount() == 1 })
	eventually(t, 5*time.Second, "the limit", func() bool { return pub.PositionLimit() == window })

	msg := make([]byte, 200) // 256 bytes framed
	sent := 0
	for {
		_, res := pub.Offer(msg)
		if res == BackPressured {
			break
		}
		if res != Offered {
			t.Fatalf("Offer gave %v", res)
		}
		sent++
	}
	if pub.Position() < window || pub.Position() > window+256 {
		t.Fatalf("stopped at position %d, want %d..%d", pub.Position(), window, window+256)
	}
	// The fast subscriber reads everything; the publisher must still be held back by the slow one.
	n := 0
	pollUntil(t, fast, func([]byte, logbuf.FrameInfo) { n++ }, func() bool { return n == sent }, "the fast subscriber")
	time.Sleep(20 * time.Millisecond)
	if _, res := pub.Offer(msg); res != BackPressured {
		t.Fatalf("with a subscriber that has not read anything, Offer gave %v, want BackPressured", res)
	}
	// Once the slow one reads, the publisher runs again.
	m := 0
	pollUntil(t, slow, func([]byte, logbuf.FrameInfo) { m++ }, func() bool { return m == sent }, "the slow subscriber")
	eventually(t, 5*time.Second, "the limit to move", func() bool { _, res := pub.Offer(msg); return res == Offered })
}

// --- several publishers --------------------------------------------------------

func TestPublicationsOfAStreamShareOneLog(t *testing.T) {
	d := launch(t, nil)
	clients := []*Client{connect(t, d, nil), connect(t, d, nil)}
	sub := mustSub(t, clients[0], 1)
	var pubs []*Publication
	for _, c := range clients {
		pubs = append(pubs, mustPub(t, c, 1))
	}
	if pubs[0].SessionID() != pubs[1].SessionID() {
		t.Fatal("two publications of one stream got different logs")
	}
	waitConnected(t, pubs[0])
	const per = 1500
	// Four sources: two goroutines on each client's publication, all writing into the one log.
	v := newVerifier(t, 4)
	var wg sync.WaitGroup
	for src := 0; src < 4; src++ {
		wg.Add(1)
		go func(src int, p *Publication) {
			defer wg.Done()
			for s := 0; s < per; s++ {
				offer(t, p, makeMsg(src, s))
			}
		}(src, pubs[src/2])
	}
	pollUntil(t, sub, v.handle, func() bool { return v.total == 4*per }, "messages of all sources")
	wg.Wait()
	for src, n := range v.next {
		if n != per {
			t.Errorf("source %d: %d messages, want %d", src, n, per)
		}
	}
}

// --- errors -------------------------------------------------------------------

func TestDriverRejectsBadRequests(t *testing.T) {
	d := launch(t, nil)
	c := connect(t, d, nil)
	var de *DriverError
	if _, err := c.AddPublication("tcp://127.0.0.1:1", 1); !errors.As(err, &de) || de.Code != cnc.ErrCodeInvalidChannel {
		t.Fatalf("an unsupported channel gave %v, want an invalid-channel DriverError", err)
	}
	if _, err := c.AddSubscription("nonsense", 1); !errors.As(err, &de) || de.Code != cnc.ErrCodeInvalidChannel {
		t.Fatalf("got %v", err)
	}
	if _, err := c.request(cnc.Command{Type: cnc.CmdRemovePublication, RegistrationID: 12345}, func(cnc.Event) (any, error) { return nil, nil }); !errors.As(err, &de) || de.Code != cnc.ErrCodeUnknownRef {
		t.Fatalf("removing an unknown registration gave %v", err)
	}
	// a registration of another client cannot be removed
	other := connect(t, d, nil)
	p := mustPub(t, c, 1)
	if _, err := other.request(cnc.Command{Type: cnc.CmdRemovePublication, RegistrationID: p.regID}, func(cnc.Event) (any, error) { return nil, nil }); !errors.As(err, &de) {
		t.Fatalf("another client removed a publication it does not own: %v", err)
	}
}

func TestDriverSurvivesGarbageCommands(t *testing.T) {
	d := launch(t, nil)
	c := connect(t, d, nil)
	for _, typ := range []int32{cnc.CmdAddPublication, cnc.CmdRemoveSubscription, cnc.CmdClientKeepalive, 77} {
		c.cmds.Write(typ, []byte{1, 2, 3})
	}
	// a command whose string length points past its end
	b := cnc.EncodeCommand(cnc.Command{Type: cnc.CmdAddPublication, ClientID: c.id, CorrelationID: 1, StreamID: 1, Channel: "ipc"})
	c.cmds.Write(cnc.CmdAddPublication, b[:len(b)-2])
	eventually(t, 5*time.Second, "garbage to be counted", func() bool { return d.Conductor().Stats().BadCommands == 5 })
	sub := mustSub(t, c, 1)
	pub := mustPub(t, c, 1)
	waitConnected(t, pub)
	offer(t, pub, []byte("still works"))
	var got string
	pollUntil(t, sub, func(m []byte, _ logbuf.FrameInfo) { got = string(m) }, func() bool { return got != "" }, "a message after the garbage")
}

// --- removal -------------------------------------------------------------------

func TestRemovedPublicationIsDrainedBeforeItGoesAway(t *testing.T) {
	d := launch(t, nil)
	c := connect(t, d, nil)
	sub := mustSub(t, c, 1)
	pub := mustPub(t, c, 1)
	waitConnected(t, pub)
	session := pub.SessionID()
	const n = 20
	for i := 0; i < n; i++ { // all of it unread when the publication is closed
		offer(t, pub, []byte(fmt.Sprintf("message-%d", i)))
	}
	if err := pub.Close(); err != nil {
		t.Fatal(err)
	}
	if _, res := pub.Offer([]byte("x")); res != Closed {
		t.Fatalf("Offer after Close gave %v", res)
	}
	if sub.ImageCount() != 1 {
		t.Fatal("the image went away while it still held unread messages")
	}
	var got []string
	pollUntil(t, sub, func(m []byte, _ logbuf.FrameInfo) { got = append(got, string(m)) }, func() bool { return len(got) == n }, "the messages of a closed publication")
	for i, m := range got {
		if m != fmt.Sprintf("message-%d", i) {
			t.Fatalf("message %d is %q", i, m)
		}
	}
	eventually(t, 5*time.Second, "the image to disappear", func() bool { return sub.ImageCount() == 0 })
	eventually(t, 5*time.Second, "the driver to forget the publication", func() bool { return d.Conductor().Stats().Publications == 0 })
	if files, _ := filepath.Glob(filepath.Join(d.Dir(), "pub-*.log")); len(files) != 0 {
		t.Fatalf("log files left behind: %v", files)
	}
	// a new publication on the stream starts a fresh log
	pub2 := mustPub(t, c, 1)
	if pub2.SessionID() == session {
		t.Fatal("a new publication reused the old log")
	}
	eventually(t, 5*time.Second, "the new image", func() bool { return sub.ImageCount() == 1 && pub2.IsConnected() })
}

func TestRemovedPublicationLingersOnlyUntilTheLingerTimeout(t *testing.T) {
	clock := newFakeClock()
	d := launch(t, func(c *driver.Config) { c.Clock = clock.Now })
	c := connect(t, d, func(o *Options) { o.Clock = clock.Now; o.ManualConductor = true })
	sub := mustSub(t, c, 1)
	pub := mustPub(t, c, 1)
	eventually(t, 5*time.Second, "connection", func() bool { c.DoWork(); return pub.IsConnected() })
	for i := 0; i < 5; i++ {
		offer(t, pub, []byte("never read"))
	}
	pub.Close()
	eventually(t, 5*time.Second, "draining to start", func() bool { return d.Conductor().Stats().Draining == 1 })
	for i := 0; i < 8; i++ { // 8s of fake time against a 5s linger, keeping the client alive
		clock.Advance(time.Second)
		c.DoWork()
		time.Sleep(5 * time.Millisecond)
	}
	eventually(t, 5*time.Second, "the lingering publication to be dropped", func() bool {
		c.DoWork()
		return d.Conductor().Stats().Publications == 0 && sub.ImageCount() == 0
	})
}

func TestClientCloseReleasesEverything(t *testing.T) {
	d := launch(t, nil)
	observer := connect(t, d, nil)
	c, err := Connect(Options{Dir: d.Dir()})
	if err != nil {
		t.Fatal(err)
	}
	mustSub(t, c, 1)
	pub := mustPub(t, c, 1)
	waitConnected(t, pub)
	allocated := func() int {
		n := 0
		observer.Counters().ForEach(func(counters.Info) { n++ })
		return n
	}
	if allocated() < 2 {
		t.Fatalf("expected counters for the publication and the image, found %d", allocated())
	}
	c.Close()
	eventually(t, 5*time.Second, "everything to be released", func() bool {
		s := d.Conductor().Stats()
		return allocated() == 0 && s.Publications == 0 && s.Subscriptions == 0 && s.Links == 0
	})
	if _, res := pub.Offer([]byte("x")); res != Closed {
		t.Fatalf("Offer after the client closed gave %v", res)
	}
}

// --- liveness ---------------------------------------------------------------------

func TestSilentClientIsDroppedWithItsResources(t *testing.T) {
	clock := newFakeClock()
	d := launch(t, func(c *driver.Config) { c.Clock = clock.Now })
	silent := connect(t, d, func(o *Options) { o.Clock = clock.Now; o.ManualConductor = true })
	live := connect(t, d, func(o *Options) { o.Clock = clock.Now; o.ManualConductor = true })
	pub := mustPub(t, silent, 1)
	sub := mustSub(t, live, 1)
	eventually(t, 5*time.Second, "the link", func() bool { live.DoWork(); return sub.ImageCount() == 1 && pub.IsConnected() })
	for i := 0; i < 4; i++ { // 8s of fake time; only `live` speaks
		clock.Advance(2 * time.Second)
		live.DoWork()
		time.Sleep(10 * time.Millisecond)
	}
	eventually(t, 5*time.Second, "the silent client to be dropped", func() bool {
		live.DoWork()
		s := d.Conductor().Stats()
		return s.ClientsTimedOut == 1 && s.Clients == 1 && s.Publications == 0 && sub.ImageCount() == 0
	})
}

func TestClientNoticesADeadDriver(t *testing.T) {
	clock := newFakeClock()
	d := launch(t, func(c *driver.Config) { c.Clock = clock.Now })
	c := connect(t, d, func(o *Options) { o.Clock = clock.Now; o.ManualConductor = true })
	sub := mustSub(t, c, 1)
	pub := mustPub(t, c, 1)
	eventually(t, 5*time.Second, "the link", func() bool { c.DoWork(); return pub.IsConnected() })
	offer(t, pub, []byte("x"))
	d.Close() // the heartbeat stops
	clock.Advance(5 * time.Second)
	c.DoWork()
	if c.Err() != nil {
		t.Fatalf("the client gave up after 5s of a 10s timeout: %v", c.Err())
	}
	clock.Advance(6 * time.Second)
	c.DoWork()
	if !errors.Is(c.Err(), ErrDriverTimeout) {
		t.Fatalf("Err is %v, want ErrDriverTimeout", c.Err())
	}
	if _, res := pub.Offer([]byte("x")); res != Closed {
		t.Fatalf("Offer gave %v after the driver died", res)
	}
	if _, err := sub.Poll(func([]byte, logbuf.FrameInfo) {}, 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("Poll gave %v", err)
	}
	if _, err := c.AddPublication("ipc", 2); !errors.Is(err, ErrDriverTimeout) {
		t.Fatalf("a new request gave %v", err)
	}
}

func TestConnectNeedsALiveDriver(t *testing.T) {
	if _, err := Connect(Options{Dir: t.TempDir(), ConnectTimeout: 50 * time.Millisecond}); !errors.Is(err, ErrDriverNotActive) {
		t.Fatalf("connecting to nothing gave %v", err)
	}
	clock := newFakeClock()
	d := launch(t, func(c *driver.Config) { c.Clock = clock.Now })
	d.Close()
	clock.Advance(time.Minute)
	if _, err := Connect(Options{Dir: d.Dir(), Clock: clock.Now}); !errors.Is(err, ErrDriverNotActive) {
		t.Fatalf("connecting to a driver whose heartbeat is a minute old gave %v", err)
	}
}

func TestOnlyOneLiveDriverPerDirectory(t *testing.T) {
	clock := newFakeClock()
	d := launch(t, func(c *driver.Config) { c.Clock = clock.Now })
	cfg := smallConfig(d.Dir())
	cfg.Clock = clock.Now
	if _, err := driver.Open(cfg); !errors.Is(err, driver.ErrActiveDriver) {
		t.Fatalf("a second driver in a live directory gave %v", err)
	}
	// Leave a log file behind, kill the driver, let its heartbeat go stale.
	c := connect(t, d, func(o *Options) { o.Clock = clock.Now; o.ManualConductor = true })
	mustPub(t, c, 1)
	d.Close()
	clock.Advance(time.Minute)
	d2, err := driver.Open(cfg)
	if err != nil {
		t.Fatalf("a new driver could not replace a dead one: %v", err)
	}
	defer d2.Close()
	if files, _ := filepath.Glob(filepath.Join(d.Dir(), "pub-*.log")); len(files) != 0 {
		t.Fatalf("the dead driver's log files were not removed: %v", files)
	}
}

// --- dead publisher ----------------------------------------------------------------

func TestDriverUnblocksAHoleLeftByADeadPublisher(t *testing.T) {
	for _, marker := range []bool{true, false} {
		t.Run(fmt.Sprintf("marker=%v", marker), func(t *testing.T) {
			clock := newFakeClock()
			d := launch(t, func(c *driver.Config) { c.Clock = clock.Now })
			c := connect(t, d, func(o *Options) { o.Clock = clock.Now; o.ManualConductor = true })
			sub := mustSub(t, c, 1)
			pub := mustPub(t, c, 1)
			eventually(t, 5*time.Second, "the link", func() bool { c.DoWork(); return pub.IsConnected() && sub.ImageCount() == 1 })
			var got []string
			h := func(m []byte, _ logbuf.FrameInfo) { got = append(got, string(m)) }
			offer(t, pub, []byte("A"))
			pub.log.SimulateDeadPublisher(100, marker) // a publisher dies mid-Offer
			offer(t, pub, []byte("B"))
			pollUntil(t, sub, h, func() bool { return len(got) == 1 }, "A")
			time.Sleep(20 * time.Millisecond)
			if n, _ := sub.Poll(h, 10); n != 0 {
				t.Fatalf("the subscriber got past the hole: %q", got)
			}
			for i := 0; i < 8; i++ { // 16s of fake time against a 10s unblock timeout
				clock.Advance(2 * time.Second)
				c.DoWork()
				time.Sleep(10 * time.Millisecond)
			}
			eventually(t, 5*time.Second, "the driver to unblock the hole", func() bool { return d.Conductor().Stats().Unblocked >= 1 })
			pollUntil(t, sub, h, func() bool { return len(got) == 2 }, "B after the unblock")
			if got[0] != "A" || got[1] != "B" {
				t.Fatalf("got %q", got)
			}
		})
	}
}

// --- other processes ------------------------------------------------------------------

func TestMain(m *testing.M) {
	switch os.Getenv("DRIVER_ROLE") {
	case "publisher":
		os.Exit(childPublisher())
	case "subscriber":
		os.Exit(childSubscriber())
	case "idle":
		os.Exit(childIdle())
	}
	os.Exit(m.Run())
}

func envInt(name string) int {
	var n int
	fmt.Sscan(os.Getenv(name), &n)
	return n
}

func childPublisher() int {
	c, err := Connect(Options{Dir: os.Getenv("DRIVER_DIR")})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	defer c.Close()
	p, err := c.AddPublication("ipc", 1)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	src, n := envInt("DRIVER_SRC"), envInt("DRIVER_N")
	deadline := time.Now().Add(60 * time.Second)
	for s := 0; s < n; s++ {
		for {
			_, res := p.Offer(makeMsg(src, s))
			if res == Offered {
				break
			}
			if (res != BackPressured && res != NotConnected && res != AdminAction) || time.Now().After(deadline) {
				fmt.Fprintln(os.Stderr, "offer:", res)
				return 3
			}
			time.Sleep(20 * time.Microsecond)
		}
	}
	p.Close()
	return 0
}

func childSubscriber() int {
	c, err := Connect(Options{Dir: os.Getenv("DRIVER_DIR")})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	defer c.Close()
	s, err := c.AddSubscription("ipc", 1)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	n := envInt("DRIVER_N")
	next, bad := 0, false
	fmt.Println("ready")
	deadline := time.Now().Add(60 * time.Second)
	for next < n && time.Now().Before(deadline) {
		got, _ := s.Poll(func(m []byte, _ logbuf.FrameInfo) {
			if string(m) != fmt.Sprintf("message-%d", next) {
				bad = true
			}
			next++
		}, 64)
		if got == 0 {
			time.Sleep(20 * time.Microsecond)
		}
	}
	if bad || next != n {
		fmt.Fprintln(os.Stderr, "subscriber got", next, "of", n, "bad:", bad)
		return 3
	}
	return 0
}

func childIdle() int {
	c, err := Connect(Options{Dir: os.Getenv("DRIVER_DIR")})
	if err != nil {
		return 2
	}
	if _, err := c.AddPublication("ipc", 7); err != nil {
		return 2
	}
	if _, err := c.AddSubscription("ipc", 8); err != nil {
		return 2
	}
	fmt.Println("ready")
	time.Sleep(time.Hour)
	return 0
}

func child(t *testing.T, role, dir string, env ...string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), append([]string{"DRIVER_ROLE=" + role, "DRIVER_DIR=" + dir}, env...)...)
	cmd.Stderr = os.Stderr
	return cmd
}

func TestPublishersInOtherProcesses(t *testing.T) {
	const procs, per = 3, 2000
	d := launch(t, nil)
	c := connect(t, d, nil)
	sub := mustSub(t, c, 1)
	var cmds []*exec.Cmd
	for i := 0; i < procs; i++ {
		cmd := child(t, "publisher", d.Dir(), fmt.Sprintf("DRIVER_SRC=%d", i), fmt.Sprintf("DRIVER_N=%d", per))
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		cmds = append(cmds, cmd)
	}
	v := newVerifier(t, procs)
	pollUntil(t, sub, v.handle, func() bool { return v.total == procs*per }, "messages from other processes")
	for i, cmd := range cmds {
		if err := cmd.Wait(); err != nil {
			t.Errorf("publisher process %d: %v", i, err)
		}
	}
	for p, n := range v.next {
		if n != per {
			t.Errorf("process %d delivered %d messages, want %d", p, n, per)
		}
	}
}

func TestSubscriberInAnotherProcess(t *testing.T) {
	const n = 5000
	d := launch(t, nil)
	c := connect(t, d, nil)
	pub := mustPub(t, c, 1)
	cmd := child(t, "subscriber", d.Dir(), fmt.Sprintf("DRIVER_N=%d", n))
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	if line, _ := bufio.NewReader(out).ReadString('\n'); line != "ready\n" {
		t.Fatalf("the subscriber process said %q", line)
	}
	waitConnected(t, pub)
	for i := 0; i < n; i++ {
		offer(t, pub, []byte(fmt.Sprintf("message-%d", i)))
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("subscriber process: %v", err)
	}
}

func TestKilledClientProcessIsCleanedUp(t *testing.T) {
	d := launch(t, func(c *driver.Config) { c.ClientLivenessTimeout = 500 * time.Millisecond })
	observer := connect(t, d, nil)
	cmd := child(t, "idle", d.Dir())
	out, _ := cmd.StdoutPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	if line, _ := bufio.NewReader(out).ReadString('\n'); line != "ready\n" {
		t.Fatalf("the client process said %q", line)
	}
	if s := d.Conductor().Stats(); s.Publications != 1 || s.Subscriptions != 1 {
		t.Fatalf("expected the child's resources in the driver, got %+v", s)
	}
	cmd.Process.Kill()
	cmd.Wait()
	eventually(t, 10*time.Second, "the killed client's resources to be reclaimed", func() bool {
		s := d.Conductor().Stats()
		n := 0
		observer.Counters().ForEach(func(counters.Info) { n++ })
		return s.ClientsTimedOut == 1 && s.Publications == 0 && s.Subscriptions == 0 && n == 0
	})
	if s := d.Conductor().Stats(); s.Clients != 1 {
		t.Fatalf("the observer should be the one client left, got %+v", s)
	}
}

// A live driver keeps its heartbeat fresh: a client must not give up on it however
// long it lives.
func TestHeartbeatKeepsAClientAlive(t *testing.T) {
	clock := newFakeClock()
	d := launch(t, func(c *driver.Config) { c.Clock = clock.Now })
	c := connect(t, d, func(o *Options) { o.Clock = clock.Now; o.ManualConductor = true })
	for i := 0; i < 12; i++ { // 36s of fake time against a 10s driver timeout
		clock.Advance(3 * time.Second)
		time.Sleep(10 * time.Millisecond) // let the driver's duty cycle run
		c.DoWork()
		if err := c.Err(); err != nil {
			t.Fatalf("after %ds the client declared the driver dead: %v", 3*(i+1), err)
		}
	}
	if s := d.Conductor().Stats(); s.ClientsTimedOut != 0 {
		t.Fatalf("the driver dropped a client that kept sending keepalives: %+v", s)
	}
}

// When the last subscriber goes, publishers are told, not left writing into a log
// nobody reads.
func TestPublicationIsNotConnectedOnceTheLastSubscriberLeaves(t *testing.T) {
	d := launch(t, nil)
	c := connect(t, d, nil)
	sub := mustSub(t, c, 1)
	pub := mustPub(t, c, 1)
	waitConnected(t, pub)
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, "the publication to see no subscriber", func() bool { return !pub.IsConnected() })
	if _, res := pub.Offer([]byte("x")); res != NotConnected {
		t.Fatalf("Offer gave %v, want NotConnected", res)
	}
	sub2 := mustSub(t, c, 1) // and it comes back with a new one
	waitConnected(t, pub)
	eventually(t, 5*time.Second, "the new image", func() bool { return sub2.ImageCount() == 1 })
}
