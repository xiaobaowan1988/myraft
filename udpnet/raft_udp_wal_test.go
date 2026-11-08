package udpnet

import (
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/xiaobaowan1988/myraft/raft"
)

// walNode is one Raft node on a real UDP socket with a real file WAL. It can be
// stopped and started again from the same file and the same UDP address.
type walNode struct {
	r    *raft.Raft
	tr   *Transport
	stop chan struct{}
}

type walUDPCluster struct {
	t     *testing.T
	addrs []string
	dir   string
	nodes []*walNode

	mu      sync.Mutex
	applied []map[int]interface{}
}

func newWALUDPCluster(t *testing.T, n int) *walUDPCluster {
	t.Helper()
	old := raft.AppendTimeout
	raft.AppendTimeout = 10 * time.Second
	t.Cleanup(func() { raft.AppendTimeout = old })

	c := &walUDPCluster{t: t, dir: t.TempDir(), nodes: make([]*walNode, n), applied: make([]map[int]interface{}, n)}
	conns := make([]*net.UDPConn, n)
	for i := range conns {
		conns[i] = listen(t)
		c.addrs = append(c.addrs, conns[i].LocalAddr().String())
	}
	for i := range conns {
		c.start(i, conns[i])
	}
	t.Cleanup(func() {
		for i := range c.nodes {
			c.stopNode(i)
		}
	})
	return c
}

func (c *walUDPCluster) start(i int, conn *net.UDPConn) {
	c.t.Helper()
	if conn == nil { // restart: take the same address again
		addr, _ := net.ResolveUDPAddr("udp", c.addrs[i])
		var err error
		for try := 0; try < 50; try++ {
			if conn, err = net.ListenUDP("udp", addr); err == nil {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if err != nil {
			c.t.Fatal(err)
		}
		conn.SetReadBuffer(4 << 20)
		conn.SetWriteBuffer(4 << 20)
	}
	wal, err := raft.OpenFileWAL(filepath.Join(c.dir, "node"+string(rune('0'+i))+".wal"))
	if err != nil {
		c.t.Fatal(err)
	}
	tr, err := New(Config{ID: i, Peers: c.addrs, Key: testKey, Conn: conn}, nil)
	if err != nil {
		c.t.Fatal(err)
	}
	applyCh := make(chan raft.ApplyMsg, 64)
	r, err := raft.Make(i, len(c.addrs), tr, wal, applyCh)
	if err != nil {
		c.t.Fatal(err)
	}
	tr.SetEndpoint(r)
	nd := &walNode{r: r, tr: tr, stop: make(chan struct{})}
	c.mu.Lock()
	c.nodes[i] = nd
	c.applied[i] = map[int]interface{}{} // a restarted state machine starts empty; Raft replays into it
	m := c.applied[i]
	c.mu.Unlock()
	go func() {
		for {
			select {
			case a := <-applyCh:
				if a.CommandValid {
					c.mu.Lock()
					m[a.CommandIndex] = a.Command
					c.mu.Unlock()
				}
			case <-nd.stop:
				return
			}
		}
	}()
}

func (c *walUDPCluster) stopNode(i int) {
	c.mu.Lock()
	nd := c.nodes[i]
	c.nodes[i] = nil
	c.mu.Unlock()
	if nd == nil {
		return
	}
	nd.r.Kill()
	nd.tr.Close()
	close(nd.stop)
}

func (c *walUDPCluster) node(i int) *walNode {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[i]
}

func (c *walUDPCluster) has(i, idx int, cmd interface{}) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.applied[i][idx]
	return ok && v == cmd
}

// commit proposes cmd until `want` live nodes applied it at the same index.
func (c *walUDPCluster) commit(cmd interface{}, want int) int {
	c.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		for i := range c.nodes {
			nd := c.node(i)
			if nd == nil {
				continue
			}
			idx, _, err := nd.r.Propose(cmd)
			if err != nil {
				continue
			}
			wait := time.Now().Add(2 * time.Second)
			for time.Now().Before(wait) {
				n := 0
				for j := range c.nodes {
					if c.has(j, idx, cmd) {
						n++
					}
				}
				if n >= want {
					return idx
				}
				time.Sleep(5 * time.Millisecond)
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("command %v was not committed on %d nodes", cmd, want)
	return 0
}

// Real sockets, real files: write, stop every node, start them again from their
// files only, and everything committed must come back and the log keep growing.
func TestRaftOverUDPWithFileWALSurvivesFullRestart(t *testing.T) {
	c := newWALUDPCluster(t, 3)
	idx := map[int]int{}
	for cmd := 1; cmd <= 15; cmd++ {
		idx[cmd] = c.commit(cmd, 3)
	}
	for round := 0; round < 2; round++ {
		for i := range c.nodes {
			c.stopNode(i)
		}
		for i := range c.nodes {
			c.start(i, nil)
		}
		// A full restart: everything committed earlier must be replayed on every node.
		deadline := time.Now().Add(15 * time.Second)
		for {
			missing := 0
			for cmd, ix := range idx {
				for i := range c.nodes {
					if !c.has(i, ix, cmd) {
						missing++
					}
				}
			}
			if missing == 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("round %d: after a full restart %d (node, command) pairs were never re-applied", round, missing)
			}
			time.Sleep(20 * time.Millisecond)
		}
		for k := 0; k < 5; k++ {
			cmd := 100*(round+1) + k
			idx[cmd] = c.commit(cmd, 3)
		}
	}
	// Indices are unique per command: nothing was overwritten by the restarts.
	seen := map[int]int{}
	for cmd, ix := range idx {
		if other, dup := seen[ix]; dup {
			t.Fatalf("commands %d and %d share index %d", other, cmd, ix)
		}
		seen[ix] = cmd
	}
}
