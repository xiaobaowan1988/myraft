package client

import (
	"container/heap"
	"errors"
	"math/rand"
	"net/netip"

	"github.com/xiaobaowan1988/myraft/driver"
)

// memNet is an in-memory datagram network for deterministic simulation. Datagrams
// are delivered by advancing a fake clock; loss, duplication and reordering (random
// per-datagram delay) come from a seeded generator, so a run is reproducible.
type memNet struct {
	rng   *rand.Rand
	conns map[netip.AddrPort]*memConn
	next  uint16
	q     simQueue
	seq   uint64

	drop, dup          float64
	minDelay, maxDelay int64 // ns
	// filter, if set, sees every datagram before the random faults; returning true drops it.
	filter func(from, to netip.AddrPort, b []byte) bool
	// trace, if set, sees every datagram handed to Send (after the filter said keep).
	trace func(now int64, from, to netip.AddrPort, b []byte)

	Sent, Dropped, Duplicated, Delivered, Filtered int64
	now                                            int64
}

type simPacket struct {
	at       int64
	seq      uint64
	from, to netip.AddrPort
	data     []byte
}

type simQueue []simPacket

func (q simQueue) Len() int { return len(q) }
func (q simQueue) Less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	return q[i].seq < q[j].seq
}
func (q simQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *simQueue) Push(x any)   { *q = append(*q, x.(simPacket)) }
func (q *simQueue) Pop() any {
	old := *q
	x := old[len(old)-1]
	*q = old[:len(old)-1]
	return x
}

func newMemNet(seed int64) *memNet {
	return &memNet{rng: rand.New(rand.NewSource(seed)), conns: map[netip.AddrPort]*memConn{}, next: 40000, maxDelay: 0}
}

type memConn struct {
	n      *memNet
	local  netip.AddrPort
	inbox  []simPacket
	closed bool
}

// Listen implements driver.ListenFunc. The wildcard address and 127.0.0.1 are the same host.
func (n *memNet) Listen(addr netip.AddrPort) (driver.PacketConn, error) {
	ip := netip.MustParseAddr("127.0.0.1")
	port := addr.Port()
	if port == 0 {
		for {
			n.next++
			if _, used := n.conns[netip.AddrPortFrom(ip, n.next)]; !used {
				port = n.next
				break
			}
		}
	}
	local := netip.AddrPortFrom(ip, port)
	if c, used := n.conns[local]; used && !c.closed {
		return nil, errors.New("memnet: address in use")
	}
	c := &memConn{n: n, local: local}
	n.conns[local] = c
	return c, nil
}

func (c *memConn) LocalAddr() netip.AddrPort { return c.local }

func (c *memConn) Close() error {
	c.closed = true
	if c.n.conns[c.local] == c {
		delete(c.n.conns, c.local)
	}
	return nil
}

func (c *memConn) Recv(buf []byte) (int, netip.AddrPort, error) {
	if c.closed {
		return 0, netip.AddrPort{}, errors.New("memnet: closed")
	}
	if len(c.inbox) == 0 {
		return 0, netip.AddrPort{}, nil
	}
	p := c.inbox[0]
	c.inbox = c.inbox[1:]
	return copy(buf, p.data), p.from, nil
}

func (c *memConn) Send(b []byte, to netip.AddrPort) (bool, error) {
	if c.closed {
		return false, errors.New("memnet: closed")
	}
	n := c.n
	n.Sent++
	if n.filter != nil && n.filter(c.local, to, b) {
		n.Filtered++
		return true, nil
	}
	if n.trace != nil {
		n.trace(n.now, c.local, to, b)
	}
	if n.drop > 0 && n.rng.Float64() < n.drop {
		n.Dropped++
		return true, nil
	}
	copies := 1
	if n.dup > 0 && n.rng.Float64() < n.dup {
		copies = 2
		n.Duplicated++
	}
	for i := 0; i < copies; i++ {
		d := n.minDelay
		if n.maxDelay > n.minDelay {
			d += n.rng.Int63n(n.maxDelay - n.minDelay)
		}
		n.seq++
		heap.Push(&n.q, simPacket{at: n.now + d, seq: n.seq, from: c.local, to: to, data: append([]byte(nil), b...)})
	}
	return true, nil
}

// advance delivers every datagram due at or before now.
func (n *memNet) advance(now int64) {
	n.now = now
	for len(n.q) > 0 && n.q[0].at <= now {
		p := heap.Pop(&n.q).(simPacket)
		dst := n.conns[p.to]
		if dst == nil || dst.closed || len(dst.inbox) >= 4096 {
			n.Dropped++ // nobody is listening, or its buffer is full
			continue
		}
		dst.inbox = append(dst.inbox, p)
		n.Delivered++
	}
}

// inject puts a datagram into the network as if from `from`.
func (n *memNet) inject(from, to netip.AddrPort, b []byte) {
	n.seq++
	heap.Push(&n.q, simPacket{at: n.now, seq: n.seq, from: from, to: to, data: append([]byte(nil), b...)})
}
