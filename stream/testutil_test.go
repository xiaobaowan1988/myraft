package stream

import (
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var testKey = []byte("0123456789abcdef-test-key")

// recorder is a Handler that remembers what it was given. delay slows OnMessage
// down, like a consumer that cannot keep up.
type recorder struct {
	mu    sync.Mutex
	msgs  []rec
	downs []int
	delay atomic.Int64  // nanoseconds
	block chan struct{} // if non-nil, OnMessage waits for it to be closed
}

type rec struct {
	from, stream int
	msg          []byte
	at           time.Time
}

func (r *recorder) OnMessage(from, stream int, msg []byte) {
	if r.block != nil {
		<-r.block
	}
	if d := r.delay.Load(); d > 0 {
		time.Sleep(time.Duration(d))
	}
	r.mu.Lock()
	r.msgs = append(r.msgs, rec{from, stream, msg, time.Now()})
	r.mu.Unlock()
}

func (r *recorder) OnPeerDown(peer int) {
	r.mu.Lock()
	r.downs = append(r.downs, peer)
	r.mu.Unlock()
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.msgs)
}

func (r *recorder) all() []rec {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]rec(nil), r.msgs...)
}

func (r *recorder) downCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.downs)
}

// faultConn wraps a UDP socket and damages what it writes: loss, duplication and
// random delay (which also reorders).
type faultConn struct {
	*net.UDPConn
	mu       sync.Mutex
	rng      *rand.Rand
	drop     float64
	dup      float64
	maxDelay time.Duration
}

func newFaultConn(c *net.UDPConn, seed int64, drop, dup float64, maxDelay time.Duration) *faultConn {
	return &faultConn{UDPConn: c, rng: rand.New(rand.NewSource(seed)), drop: drop, dup: dup, maxDelay: maxDelay}
}

func (f *faultConn) WriteToUDP(b []byte, addr *net.UDPAddr) (int, error) {
	pkt := append([]byte(nil), b...)
	f.mu.Lock()
	lost := f.rng.Float64() < f.drop
	copies := 1
	if f.rng.Float64() < f.dup {
		copies = 2
	}
	delays := make([]time.Duration, copies)
	for i := range delays {
		if f.maxDelay > 0 {
			delays[i] = time.Duration(f.rng.Int63n(int64(f.maxDelay)))
		}
	}
	f.mu.Unlock()
	if lost {
		return len(b), nil
	}
	for _, d := range delays {
		if d == 0 {
			f.UDPConn.WriteToUDP(pkt, addr)
		} else {
			time.AfterFunc(d, func() { f.UDPConn.WriteToUDP(pkt, addr) })
		}
	}
	return len(b), nil
}

func listen(t *testing.T) *net.UDPConn {
	t.Helper()
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	c.SetReadBuffer(4 << 20)
	c.SetWriteBuffer(4 << 20)
	return c
}

func listenOn(t *testing.T, a *net.UDPAddr) *net.UDPConn {
	t.Helper()
	var err error
	for i := 0; i < 100; i++ {
		var c *net.UDPConn
		if c, err = net.ListenUDP("udp", a); err == nil {
			c.SetReadBuffer(4 << 20)
			c.SetWriteBuffer(4 << 20)
			return c
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(err)
	return nil
}

// testNet is n nodes on loopback sockets that know each other.
type testNet struct {
	nodes []*Node
	rec   []*recorder
	conns []*net.UDPConn
	addrs []string
}

type netOpts struct {
	drop, dup float64
	maxDelay  time.Duration
	tweak     func(i int, c *Config)
}

func newTestNet(t *testing.T, n int, o netOpts) *testNet {
	t.Helper()
	tn := &testNet{}
	for i := 0; i < n; i++ {
		c := listen(t)
		tn.conns = append(tn.conns, c)
		tn.addrs = append(tn.addrs, c.LocalAddr().String())
	}
	for i := 0; i < n; i++ {
		tn.start(t, i, o)
	}
	t.Cleanup(func() {
		for _, nd := range tn.nodes {
			nd.Close()
		}
	})
	return tn
}

// start (re)creates node i on its socket.
func (tn *testNet) start(t *testing.T, i int, o netOpts) {
	t.Helper()
	var pc PacketConn = tn.conns[i]
	if o.drop > 0 || o.dup > 0 || o.maxDelay > 0 {
		pc = newFaultConn(tn.conns[i], int64(100+i), o.drop, o.dup, o.maxDelay)
	}
	cfg := Config{ID: i, Peers: tn.addrs, Key: testKey, Conn: pc}
	if o.tweak != nil {
		o.tweak(i, &cfg)
	}
	nd, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	r := &recorder{}
	nd.SetHandler(r)
	if i < len(tn.nodes) {
		tn.nodes[i], tn.rec[i] = nd, r
	} else {
		tn.nodes, tn.rec = append(tn.nodes, nd), append(tn.rec, r)
	}
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// numbered builds a message that carries i, so order and loss can be checked.
func numbered(i int, size int) []byte {
	b := make([]byte, max(size, 8))
	for k := 0; k < 8; k++ {
		b[k] = byte(i >> (8 * (7 - k)))
	}
	for k := 8; k < len(b); k++ {
		b[k] = byte(i + k) // content that depends on the position inside the message
	}
	return b
}

func numberOf(b []byte) int {
	v := 0
	for k := 0; k < 8; k++ {
		v = v<<8 | int(b[k])
	}
	return v
}

func intact(b []byte) bool {
	i := numberOf(b)
	for k := 8; k < len(b); k++ {
		if b[k] != byte(i+k) {
			return false
		}
	}
	return true
}
