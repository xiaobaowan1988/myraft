package raft

import (
	"math/rand"
	"sync"
	"testing"
	"time"
)

type cluster struct {
	t      *testing.T
	n      int
	net    *SimNet
	stores []*MemStorage

	mu      sync.Mutex
	rafts   []*Raft
	stops   []chan struct{}
	alive   []bool
	applied []map[int]interface{} // per node: index -> command
	global  map[int]interface{}   // index -> command, must never conflict
}

func newCluster(t *testing.T, n int) *cluster {
	c := &cluster{
		t: t, n: n, net: NewSimNet(n, time.Now().UnixNano()),
		stores: make([]*MemStorage, n), rafts: make([]*Raft, n),
		stops: make([]chan struct{}, n), alive: make([]bool, n),
		applied: make([]map[int]interface{}, n), global: map[int]interface{}{},
	}
	for i := 0; i < n; i++ {
		c.stores[i] = &MemStorage{}
		c.applied[i] = map[int]interface{}{}
		c.start(i)
	}
	t.Cleanup(func() {
		for i := 0; i < n; i++ {
			c.crash(i)
		}
	})
	return c
}

func (c *cluster) start(i int) {
	applyCh := make(chan ApplyMsg, 16)
	r, err := Make(i, c.n, c.net.Endpoint(i), c.stores[i], applyCh)
	if err != nil {
		c.t.Fatalf("Make(%d): %v", i, err)
	}
	stop := make(chan struct{})
	c.mu.Lock()
	c.rafts[i], c.stops[i], c.alive[i] = r, stop, true
	c.mu.Unlock()
	c.net.Register(i, r)
	c.net.SetUp(i, true)
	go func() {
		for {
			select {
			case m := <-applyCh:
				if !m.CommandValid {
					continue
				}
				c.mu.Lock()
				if old, ok := c.global[m.CommandIndex]; ok && old != m.Command {
					c.t.Errorf("DIVERGENCE at index %d: %v vs %v (node %d)", m.CommandIndex, old, m.Command, i)
				}
				c.global[m.CommandIndex] = m.Command
				c.applied[i][m.CommandIndex] = m.Command
				c.mu.Unlock()
			case <-stop:
				return
			}
		}
	}()
}

func (c *cluster) crash(i int) {
	c.mu.Lock()
	if !c.alive[i] {
		c.mu.Unlock()
		return
	}
	c.alive[i] = false
	r, stop := c.rafts[i], c.stops[i]
	c.mu.Unlock()
	c.net.SetUp(i, false)
	r.Kill()
	close(stop)
}

func (c *cluster) restart(i int) { c.start(i) }

func (c *cluster) isAlive(i int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.alive[i]
}

func (c *cluster) raft(i int) *Raft {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rafts[i]
}

// waitLeader waits until, among nodes, exactly one leader holds the highest
// term seen, and returns it. It also fails if two leaders share a term.
func (c *cluster) waitLeader(nodes ...int) int {
	c.t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		leaders := map[int][]int{}
		maxTerm := 0
		for _, i := range nodes {
			if !c.isAlive(i) {
				continue
			}
			term, isL := c.raft(i).State()
			if term > maxTerm {
				maxTerm = term
			}
			if isL {
				leaders[term] = append(leaders[term], i)
			}
		}
		for term, ls := range leaders {
			if len(ls) > 1 {
				c.t.Fatalf("two leaders in term %d: %v", term, ls)
			}
		}
		if ls := leaders[maxTerm]; len(ls) == 1 {
			return ls[0]
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("no leader among %v", nodes)
	return -1
}

func (c *cluster) nCommitted(idx int, cmd interface{}) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	cnt := 0
	for i := 0; i < c.n; i++ {
		if v, ok := c.applied[i][idx]; ok && v == cmd {
			cnt++
		}
	}
	return cnt
}

// try proposes cmd until at least want nodes applied it, or timeout.
func (c *cluster) try(cmd int, want int, timeout time.Duration) (int, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for i := 0; i < c.n; i++ {
			if !c.isAlive(i) {
				continue
			}
			idx, _, ok := c.raft(i).Start(cmd)
			if !ok {
				continue
			}
			wait := time.Now().Add(time.Second)
			for time.Now().Before(wait) {
				if c.nCommitted(idx, cmd) >= want {
					return idx, true
				}
				time.Sleep(10 * time.Millisecond)
			}
			// This node may be a stale leader in a minority partition: try the next one.
		}
		time.Sleep(30 * time.Millisecond)
	}
	return 0, false
}

func (c *cluster) one(cmd int, want int) int {
	c.t.Helper()
	idx, ok := c.try(cmd, want, 10*time.Second)
	if !ok {
		c.t.Fatalf("command %d not committed on %d nodes", cmd, want)
	}
	return idx
}

func ids(xs ...int) []int { return xs }

func TestInitialElection(t *testing.T) {
	c := newCluster(t, 5)
	l := c.waitLeader(0, 1, 2, 3, 4)
	term1, _ := c.raft(l).State()
	time.Sleep(600 * time.Millisecond)
	term2, isL := c.raft(l).State()
	if !isL || term1 != term2 {
		t.Fatalf("leadership unstable: term %d -> %d, leader=%v", term1, term2, isL)
	}
}

func TestReElection(t *testing.T) {
	c := newCluster(t, 5)
	l1 := c.waitLeader(0, 1, 2, 3, 4)
	c.net.Partition(ids(l1)) // isolate the leader
	rest := []int{}
	for i := 0; i < 5; i++ {
		if i != l1 {
			rest = append(rest, i)
		}
	}
	l2 := c.waitLeader(rest...)
	if l2 == l1 {
		t.Fatal("same leader after isolation")
	}
	c.net.Heal()
	time.Sleep(500 * time.Millisecond)
	if _, isL := c.raft(l1).State(); isL {
		t.Fatal("old leader did not step down")
	}
	c.waitLeader(0, 1, 2, 3, 4)
}

func TestBasicAgree(t *testing.T) {
	c := newCluster(t, 5)
	c.waitLeader(0, 1, 2, 3, 4)
	for i := 1; i <= 5; i++ {
		c.one(100+i, 5)
	}
}

func TestLeaderFailureAgree(t *testing.T) {
	c := newCluster(t, 5)
	c.one(1, 5)
	l := c.waitLeader(0, 1, 2, 3, 4)
	c.crash(l)
	c.one(2, 4)
	c.one(3, 4)
	c.restart(l)
	c.one(4, 5) // restarted node catches up and applies everything
}

func TestMinorityPartition(t *testing.T) {
	c := newCluster(t, 5)
	c.one(1, 5)
	l := c.waitLeader(0, 1, 2, 3, 4)
	minority := ids(l, (l+1)%5)
	majority := ids((l+2)%5, (l+3)%5, (l+4)%5)
	c.net.Partition(minority, majority)

	// Old leader accepts a command it can never commit.
	idx, _, ok := c.raft(l).Start(666)
	if !ok {
		t.Fatal("old leader should still accept Start")
	}
	c.waitLeader(majority...)
	for i := 10; i < 13; i++ {
		c.one(i, 3)
	}
	if c.nCommitted(idx, 666) != 0 {
		t.Fatal("minority committed a command")
	}
	c.net.Heal()
	c.one(99, 5)
	if c.nCommitted(idx, 666) != 0 {
		t.Fatal("uncommitted minority entry survived")
	}
}

func TestPersistRestartAll(t *testing.T) {
	c := newCluster(t, 5)
	for i := 1; i <= 3; i++ {
		c.one(i, 5)
	}
	for i := 0; i < 5; i++ {
		c.crash(i)
	}
	for i := 0; i < 5; i++ {
		c.restart(i)
	}
	c.waitLeader(0, 1, 2, 3, 4)
	c.one(4, 5)
	// Entries 1..3 must have been re-applied at the same indexes everywhere.
	for i := 0; i < 5; i++ {
		for _, cmd := range []int{1, 2, 3} {
			found := false
			c.mu.Lock()
			for _, v := range c.applied[i] {
				if v == cmd {
					found = true
				}
			}
			c.mu.Unlock()
			if !found {
				t.Fatalf("node %d lost command %d", i, cmd)
			}
		}
	}
}

func TestChaos(t *testing.T) {
	c := newCluster(t, 5)
	c.waitLeader(0, 1, 2, 3, 4)
	c.net.SetChaos(0.15, 20*time.Millisecond, 0.05, 500*time.Millisecond)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	confirmed := map[int]int{} // index -> cmd, confirmed on a majority
	var cmu sync.Mutex

	wg.Add(1)
	go func() { // client
		defer wg.Done()
		for cmd := 1000; ; cmd++ {
			select {
			case <-stop:
				return
			default:
			}
			if idx, ok := c.try(cmd, 3, 2*time.Second); ok {
				cmu.Lock()
				confirmed[idx] = cmd
				cmu.Unlock()
			}
		}
	}()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	down := map[int]bool{}
	end := time.Now().Add(8 * time.Second)
	for time.Now().Before(end) {
		switch rng.Intn(4) {
		case 0: // random 2|3 partition
			perm := rng.Perm(5)
			c.net.Partition(perm[:2], perm[2:])
		case 1:
			c.net.Heal()
		case 2: // crash (keep a majority alive)
			if i := rng.Intn(5); !down[i] && len(down) < 2 {
				c.crash(i)
				down[i] = true
			}
		case 3:
			for i := range down {
				c.restart(i)
				delete(down, i)
				break
			}
		}
		time.Sleep(time.Duration(200+rng.Intn(400)) * time.Millisecond)
	}
	close(stop)
	wg.Wait()

	// Heal everything: system must be live again.
	c.net.Heal()
	c.net.SetChaos(0, 0, 0, 0)
	for i := range down {
		c.restart(i)
	}
	c.waitLeader(0, 1, 2, 3, 4)
	final := c.one(999999, 5)

	// Every confirmed command is still at its index on every node, and all
	// nodes agree on the whole prefix up to the final entry.
	cmu.Lock()
	defer cmu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	for idx, cmd := range confirmed {
		for i := 0; i < 5; i++ {
			if v, ok := c.applied[i][idx]; !ok || v != cmd {
				t.Errorf("node %d: index %d = %v (ok=%v), want confirmed %d", i, idx, v, ok, cmd)
			}
		}
	}
	for idx := 1; idx <= final; idx++ {
		for i := 1; i < 5; i++ {
			if c.applied[i][idx] != c.applied[0][idx] {
				t.Errorf("index %d differs between node 0 and %d", idx, i)
			}
		}
	}
	t.Logf("confirmed %d commands, final index %d", len(confirmed), final)
}
