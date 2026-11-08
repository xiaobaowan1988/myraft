package udpnet

import (
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xiaobaowan1988/myraft/raft"
)

// raftCluster is real Raft nodes talking over real UDP sockets on loopback.
type raftCluster struct {
	t     *testing.T
	nodes []*raftNode

	mu     sync.Mutex
	global map[int]interface{} // index -> command; must never conflict across nodes
}

type raftNode struct {
	r       *raft.Raft
	tr      *Transport
	applied map[int]interface{}
	alive   bool
	stop    chan struct{}
}

func newRaftCluster(t *testing.T, n int, o netOpts) *raftCluster {
	t.Helper()
	// On a reliable stream raft.AppendTimeout is only a safety net: see the package doc.
	oldTimeout := raft.AppendTimeout
	raft.AppendTimeout = 10 * time.Second
	t.Cleanup(func() { raft.AppendTimeout = oldTimeout })

	c := &raftCluster{t: t, global: map[int]interface{}{}}
	conns := make([]*net.UDPConn, n)
	addrs := make([]string, n)
	for i := range conns {
		conns[i] = listen(t)
		addrs[i] = conns[i].LocalAddr().String()
	}
	for i := 0; i < n; i++ {
		tr, err := New(Config{ID: i, Peers: addrs, Key: testKey, Conn: o.wrap(conns[i], int64(500+i))}, nil)
		if err != nil {
			t.Fatal(err)
		}
		applyCh := make(chan raft.ApplyMsg, 64)
		r, err := raft.Make(i, n, tr, &raft.MemStorage{}, applyCh)
		if err != nil {
			t.Fatal(err)
		}
		tr.SetEndpoint(r)
		nd := &raftNode{r: r, tr: tr, applied: map[int]interface{}{}, alive: true, stop: make(chan struct{})}
		c.nodes = append(c.nodes, nd)
		go func() {
			for {
				select {
				case m := <-applyCh:
					if !m.CommandValid {
						continue
					}
					c.mu.Lock()
					if old, ok := c.global[m.CommandIndex]; ok && old != m.Command {
						t.Errorf("DIVERGENCE at index %d: %v vs %v", m.CommandIndex, old, m.Command)
					}
					c.global[m.CommandIndex] = m.Command
					nd.applied[m.CommandIndex] = m.Command
					c.mu.Unlock()
				case <-nd.stop:
					return
				}
			}
		}()
	}
	t.Cleanup(func() {
		for i := range c.nodes {
			c.kill(i)
		}
	})
	return c
}

func (c *raftCluster) kill(i int) {
	c.mu.Lock()
	nd := c.nodes[i]
	if !nd.alive {
		c.mu.Unlock()
		return
	}
	nd.alive = false
	c.mu.Unlock()
	nd.r.Kill()
	nd.tr.Close()
	close(nd.stop)
}

func (c *raftCluster) isAlive(i int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[i].alive
}

// leader waits for a leader among the live nodes and returns the one with the highest term.
func (c *raftCluster) leader() int {
	c.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		best, bestTerm := -1, -1
		seen := map[int]int{}
		for i, nd := range c.nodes {
			if !c.isAlive(i) {
				continue
			}
			term, isL := nd.r.State()
			if isL {
				if other, dup := seen[term]; dup {
					c.t.Fatalf("two leaders in term %d: %d and %d", term, other, i)
				}
				seen[term] = i
				if term > bestTerm {
					best, bestTerm = i, term
				}
			}
		}
		if best >= 0 {
			return best
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatal("no leader elected")
	return -1
}

func (c *raftCluster) count(idx int, cmd interface{}) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, nd := range c.nodes {
		if v, ok := nd.applied[idx]; ok && v == cmd {
			n++
		}
	}
	return n
}

// commit proposes cmd until at least want nodes have applied it.
func (c *raftCluster) commit(cmd interface{}, want int, timeout time.Duration) (int, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for i, nd := range c.nodes {
			if !c.isAlive(i) {
				continue
			}
			idx, _, err := nd.r.Propose(cmd)
			if err != nil {
				continue
			}
			wait := time.Now().Add(2 * time.Second)
			for time.Now().Before(wait) {
				if c.count(idx, cmd) >= want {
					return idx, true
				}
				time.Sleep(5 * time.Millisecond)
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return 0, false
}

// converged checks that every live node applied exactly the same commands.
func (c *raftCluster) checkSame(live ...int) {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	first := c.nodes[live[0]].applied
	for _, i := range live[1:] {
		other := c.nodes[i].applied
		for idx, v := range first {
			if w, ok := other[idx]; ok && w != v {
				c.t.Errorf("nodes %d and %d disagree at index %d: %v vs %v", live[0], i, idx, v, w)
			}
		}
	}
}

func TestRaftOverUDPElectsAndCommits(t *testing.T) {
	c := newRaftCluster(t, 3, netOpts{})
	c.leader()
	for cmd := 1; cmd <= 30; cmd++ {
		if _, ok := c.commit(cmd, 3, 10*time.Second); !ok {
			t.Fatalf("command %d was not committed on all 3 nodes", cmd)
		}
	}
	c.checkSame(0, 1, 2)
}

// The whole point: Raft running on a lossy, duplicating, reordering network.
func TestRaftOverUDPWithLossReorderAndDuplication(t *testing.T) {
	c := newRaftCluster(t, 3, netOpts{drop: 0.10, dup: 0.05, maxDelay: 5 * time.Millisecond})
	c.leader()
	committed := map[int]int{} // cmd -> index
	for cmd := 1; cmd <= 60; cmd++ {
		idx, ok := c.commit(cmd, 2, 15*time.Second)
		if !ok {
			t.Fatalf("command %d was not committed on a majority", cmd)
		}
		committed[cmd] = idx
	}
	// Give stragglers time to catch up, then every node must hold the same log.
	time.Sleep(time.Second)
	c.checkSame(0, 1, 2)
	for cmd, idx := range committed {
		if n := c.count(idx, cmd); n < 2 {
			t.Errorf("committed command %d at index %d is on only %d nodes", cmd, idx, n)
		}
	}
	var retrans, naks uint64
	for _, nd := range c.nodes {
		st := nd.tr.Stats()
		retrans += st.Stream.Retransmits
		naks += st.Stream.NaksSent
	}
	t.Logf("60 commands committed; repair traffic: %d retransmissions, %d NAKs", retrans, naks)
}

func TestRaftOverUDPLeaderFailover(t *testing.T) {
	c := newRaftCluster(t, 3, netOpts{})
	l1 := c.leader()
	for cmd := 1; cmd <= 10; cmd++ {
		if _, ok := c.commit(cmd, 3, 10*time.Second); !ok {
			t.Fatalf("command %d not committed", cmd)
		}
	}
	c.kill(l1)
	l2 := c.leader()
	if l2 == l1 {
		t.Fatal("the killed node is still leader")
	}
	var survivors []int
	for i := range c.nodes {
		if i != l1 {
			survivors = append(survivors, i)
		}
	}
	for cmd := 11; cmd <= 20; cmd++ {
		if _, ok := c.commit(cmd, 2, 10*time.Second); !ok {
			t.Fatalf("command %d not committed after failover", cmd)
		}
	}
	time.Sleep(300 * time.Millisecond)
	c.checkSame(survivors...)
	// Everything committed before the failure must still be there on the survivors.
	for cmd := 1; cmd <= 10; cmd++ {
		found := false
		c.mu.Lock()
		for _, v := range c.nodes[survivors[0]].applied {
			if v == cmd {
				found = true
			}
		}
		c.mu.Unlock()
		if !found {
			t.Errorf("command %d, committed before the failover, is missing on node %d", cmd, survivors[0])
		}
	}
}

// Raft over a reliable stream under heavier damage than the plain-datagram transport
// could stand: the stream repairs every loss, so Raft never even sees them.
func TestRaftOverUDPWithHeavyLoss(t *testing.T) {
	c := newRaftCluster(t, 3, netOpts{drop: 0.20, dup: 0.10, maxDelay: 8 * time.Millisecond})
	c.leader()
	for cmd := 1; cmd <= 40; cmd++ {
		if _, ok := c.commit(cmd, 2, 20*time.Second); !ok {
			t.Fatalf("command %d was not committed on a majority under 20%% loss", cmd)
		}
	}
	time.Sleep(time.Second)
	c.checkSame(0, 1, 2)
}

// A single command of hundreds of KiB is replicated: there is no message size limit
// any more (the previous datagram transport could not carry anything over 64 KiB).
func TestRaftOverUDPReplicatesHugeCommands(t *testing.T) {
	// 4ms of jitter and a little loss: enough that delivering the command takes longer than
	// the default 80ms AppendTimeout (which the next test shows is a problem here).
	c := newRaftCluster(t, 3, netOpts{drop: 0.03, dup: 0.03, maxDelay: 4 * time.Millisecond})
	c.leader()
	huge := strings.Repeat("0123456789abcdef", 20_000) // 320 KB
	if _, ok := c.commit(huge, 3, 30*time.Second); !ok {
		t.Fatal("a 320 KB command was not committed on all nodes")
	}
	for cmd := 1; cmd <= 5; cmd++ { // and the log keeps working behind it
		if _, ok := c.commit(cmd, 3, 10*time.Second); !ok {
			t.Fatalf("command %d after the huge one was not committed", cmd)
		}
	}
	c.checkSame(0, 1, 2)
}
