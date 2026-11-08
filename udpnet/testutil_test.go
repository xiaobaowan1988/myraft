package udpnet

import (
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/xiaobaowan1988/myraft/raft"
	"github.com/xiaobaowan1988/myraft/stream"
)

var testKey = []byte("0123456789abcdef-test-key")

// recorder is a raft.Endpoint that remembers what it was given.
type recorder struct {
	block       chan struct{} // if non-nil, Step waits for it to be closed
	mu          sync.Mutex
	msgs        []raft.Message
	unreachable []int
}

func (r *recorder) Step(m raft.Message) {
	r.mu.Lock()
	block := r.block
	r.mu.Unlock()
	if block != nil {
		<-block
	}
	r.mu.Lock()
	r.msgs = append(r.msgs, m)
	r.mu.Unlock()
}

// setBlock makes Step wait until ch is closed (nil removes the block).
func (r *recorder) setBlock(ch chan struct{}) {
	r.mu.Lock()
	r.block = ch
	r.mu.Unlock()
}

func (r *recorder) ReportUnreachable(peer int) {
	r.mu.Lock()
	r.unreachable = append(r.unreachable, peer)
	r.mu.Unlock()
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.msgs)
}

func (r *recorder) snapshot() []raft.Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]raft.Message(nil), r.msgs...)
}

func (r *recorder) unreachableCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.unreachable)
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

type netOpts struct {
	drop, dup float64
	maxDelay  time.Duration
	tweak     func(i int, c *Config)
}

// wrap applies the fault model to a socket.
func (o netOpts) wrap(c *net.UDPConn, seed int64) stream.PacketConn {
	if o.drop > 0 || o.dup > 0 || o.maxDelay > 0 {
		return &faultConn{UDPConn: c, rng: rand.New(rand.NewSource(seed)), drop: o.drop, dup: o.dup, maxDelay: o.maxDelay}
	}
	return c
}

// testNet is n transports on loopback sockets that know each other.
type testNet struct {
	tr    []*Transport
	rec   []*recorder
	conns []*net.UDPConn
	addrs []string
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
		cfg := Config{ID: i, Peers: tn.addrs, Key: testKey, Conn: o.wrap(tn.conns[i], int64(100+i))}
		if o.tweak != nil {
			o.tweak(i, &cfg)
		}
		tr, err := New(cfg, nil)
		if err != nil {
			t.Fatal(err)
		}
		rec := &recorder{}
		tr.SetEndpoint(rec)
		tn.tr = append(tn.tr, tr)
		tn.rec = append(tn.rec, rec)
	}
	t.Cleanup(func() {
		for _, tr := range tn.tr {
			tr.Close()
		}
	})
	return tn
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
