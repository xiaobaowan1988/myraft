package raft

import (
	"bytes"
	"errors"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// crashingWAL is a FileWAL that can be told to "die" in the middle of its next
// Append, the way a process does when it is killed or the machine loses power:
// only a prefix of the frames reaches the file (or all of it, written and synced,
// but nothing after), and then the writing goroutine simply ends. The ready loop
// calling Append is that goroutine, so after the crash nothing more is persisted,
// sent, applied or released: exactly what a dead process would do. The test then
// kills the rest of the node and reopens the file.
type crashingWAL struct {
	*FileWAL
	mu        sync.Mutex
	armed     bool
	cut       float64 // fraction of the encoded bytes that reaches the file
	afterSync bool    // the whole write is on disk (and synced), but nothing after it happens
	crashed   chan struct{}
}

func newCrashingWAL(w *FileWAL) *crashingWAL {
	return &crashingWAL{FileWAL: w, crashed: make(chan struct{})}
}

// arm makes the next Append crash. cut=0.4 leaves 40% of its bytes in the file.
func (w *crashingWAL) arm(cut float64, afterSync bool) {
	w.mu.Lock()
	w.armed, w.cut, w.afterSync = true, cut, afterSync
	w.mu.Unlock()
}

func (w *crashingWAL) hasCrashed() bool {
	select {
	case <-w.crashed:
		return true
	default:
		return false
	}
}

func (w *crashingWAL) Append(recs []Record) error {
	w.mu.Lock()
	armed, cut, after := w.armed, w.cut, w.afterSync
	w.armed = false
	w.mu.Unlock()
	if !armed {
		return w.FileWAL.Append(recs)
	}
	data, err := encodeFrames(recs)
	if err != nil {
		return err
	}
	n := int(float64(len(data)) * cut)
	if after {
		n = len(data)
	}
	w.FileWAL.mu.Lock()
	w.FileWAL.f.Write(data[:n])
	if after {
		w.FileWAL.f.Sync()
	}
	w.FileWAL.mu.Unlock()
	close(w.crashed)
	runtime.Goexit() // the "process" dies here: its deferred cleanup runs, nothing else does
	return nil
}

// hasAll reports whether node i has applied every command in cmds.
func (c *cluster) hasAll(i int, cmds []int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	seen := map[interface{}]bool{}
	for _, v := range c.applied[i] {
		seen[v] = true
	}
	for _, cmd := range cmds {
		if !seen[cmd] {
			return false
		}
	}
	return true
}

func (c *cluster) waitAllHave(cmds []int, nodes ...int) {
	c.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, i := range nodes {
			if !c.hasAll(i, cmds) {
				ok = false
			}
		}
		if ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, i := range nodes {
		if !c.hasAll(i, cmds) {
			c.t.Fatalf("node %d never re-applied all of %v (has %d entries): committed data was lost or not recovered from its WAL", i, cmds, len(c.applied[i]))
		}
	}
}

func seq(from, to int) []int {
	var out []int
	for i := from; i <= to; i++ {
		out = append(out, i)
	}
	return out
}

// Every node is stopped and restarted from nothing but its WAL file. What was
// committed must come back (the restarted state machines start empty and are rebuilt
// from the logs), terms must not go backwards, and the cluster must keep working.
func TestWALClusterSurvivesRestartOfEveryNode(t *testing.T) {
	c := newFileCluster(t, 3)
	c.waitLeader(0, 1, 2)
	for cmd := 1; cmd <= 30; cmd++ {
		c.one(cmd, 3)
	}
	time.Sleep(200 * time.Millisecond) // settle: nothing in flight
	before := make([]int, 3)
	for i := range before {
		before[i], _ = c.raft(i).State()
	}

	for i := 0; i < 3; i++ {
		c.crash(i)
	}
	for i := 0; i < 3; i++ {
		c.restart(i)
	}
	for i := 0; i < 3; i++ {
		if term, _ := c.raft(i).State(); term < before[i] {
			t.Errorf("node %d came back at term %d, below the %d it had reached: term was not persisted", i, term, before[i])
		}
	}
	c.waitLeader(0, 1, 2)
	c.one(31, 3)
	c.waitAllHave(seq(1, 31), 0, 1, 2)
}

// A node killed at the worst moments of its own write must recover from whatever
// that left on disk, and the cluster must stay consistent and lose nothing it had
// committed. victim is a follower or the leader; the write is cut at several points,
// or completes (synced) just before the process dies, so the messages that follow a
// persist are never sent.
func TestWALClusterCrashDuringAppend(t *testing.T) {
	type point struct {
		name      string
		cut       float64
		afterSync bool
	}
	points := []point{
		{"nothing written", 0, false},
		{"20% of the bytes", 0.2, false},
		{"60% of the bytes", 0.6, false},
		{"all but the last byte", 0.999, false},
		{"written and synced, then died before sending", 1, true},
	}
	for _, role := range []string{"follower", "leader"} {
		for _, pt := range points {
			role, pt := role, pt
			t.Run(role+"/"+pt.name, func(t *testing.T) {
				c := newFileCluster(t, 3)
				l := c.waitLeader(0, 1, 2)
				committed := seq(1, 10)
				for _, cmd := range committed {
					c.one(cmd, 3)
				}
				victim := l
				if role == "follower" {
					victim = (l + 1) % 3
				}
				var survivors []int
				for i := 0; i < 3; i++ {
					if i != victim {
						survivors = append(survivors, i)
					}
				}

				w := c.wal(victim)
				w.arm(pt.cut, pt.afterSync)
				if role == "leader" {
					go c.raft(l).Start(100) // its own Append is the one that dies
				} else {
					if _, ok := c.try(100, 2, 5*time.Second); ok {
						committed = append(committed, 100)
					}
				}
				select {
				case <-w.crashed:
				case <-time.After(5 * time.Second):
					t.Fatal("the armed write never happened")
				}
				c.crash(victim)

				c.waitLeader(survivors...)
				for _, cmd := range []int{200, 201} {
					c.one(cmd, 2)
					committed = append(committed, cmd)
				}

				if err := c.startErr(victim); err != nil {
					t.Fatalf("the victim could not restart from the file the crash left: %v", err)
				}
				c.one(300, 3)
				committed = append(committed, 300)
				c.waitAllHave(committed, 0, 1, 2)
			})
		}
	}
}

// Damage in the middle of a node's WAL is refused, not repaired by dropping what
// follows it; damage in the very last frame is indistinguishable from a torn write
// and is dropped like one.
func TestWALClusterMidFileCorruptionIsRefusedAndTailDamageIsNot(t *testing.T) {
	c := newFileCluster(t, 3)
	c.waitLeader(0, 1, 2)
	for cmd := 1; cmd <= 10; cmd++ {
		c.one(cmd, 3)
	}
	c.crash(1)
	path := c.walPath(1)
	orig, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	bad := append([]byte(nil), orig...)
	bad[len(bad)/3] ^= 0xff
	os.WriteFile(path, bad, 0o644)
	err = c.startErr(1)
	var ce *CorruptError
	if !errors.As(err, &ce) {
		t.Fatalf("starting on a WAL damaged in the middle gave err=%v, want *CorruptError", err)
	}
	if after, _ := os.ReadFile(path); !bytes.Equal(after, bad) {
		t.Fatal("the refused WAL was modified")
	}

	// Only the last frame damaged: indistinguishable from a torn write, so the node
	// starts and drops it. NOTE what this simulates: the node was stopped cleanly
	// first, so that last frame was one it had already acknowledged. Losing
	// acknowledged data is outside Raft's fault model (a real torn write, which the
	// crash-injection test covers, only ever eats a frame that was never
	// acknowledged), and a leader cannot repair it: its matchIndex for this node
	// only ever rises, as in etcd. So expect the healthy majority to carry on and
	// nothing to diverge, but not that this node catches up.
	tail := append([]byte(nil), orig...)
	tail[len(tail)-2] ^= 0xff
	os.WriteFile(path, tail, 0o644)
	if err := c.startErr(1); err != nil {
		t.Fatalf("a WAL with only its last frame damaged should start (it is a torn tail): %v", err)
	}
	c.one(50, 2)
	c.waitAllHave(append(seq(1, 10), 50), 0, 2)
}

// The big one: partitions, loss, duplication, clean crashes and crashes in the middle
// of a write, all at random, with every node on a real file. Afterwards: no node ever
// disagreed, everything the client saw committed is still there on every node
// (rebuilt from WALs for the ones that restarted), and the cluster is live again.
func TestWALClusterChaosWithCrashesAndTornWrites(t *testing.T) {
	c := newFileCluster(t, 5)
	c.waitLeader(0, 1, 2, 3, 4)
	c.net.SetChaos(0.10, 20*time.Millisecond, 0.05, 300*time.Millisecond)
	c.net.SetDup(0.03)

	var downMu sync.Mutex
	down := map[int]bool{}
	setDown := func(i int, v bool) {
		downMu.Lock()
		defer downMu.Unlock()
		if v {
			down[i] = true
		} else {
			delete(down, i)
		}
	}
	downCount := func() int {
		downMu.Lock()
		defer downMu.Unlock()
		return len(down)
	}
	firstDown := func() (int, bool) {
		downMu.Lock()
		defer downMu.Unlock()
		for i := 0; i < 5; i++ {
			if down[i] {
				return i, true
			}
		}
		return 0, false
	}

	var tornCrashes, cleanCrashes, restarts atomic.Int32
	stop := make(chan struct{})
	var wg sync.WaitGroup
	confirmed := map[int]int{}
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

	wg.Add(1)
	go func() { // watcher: a node whose write "killed" it is dead; finish the job
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Millisecond):
			}
			for i := 0; i < 5; i++ {
				if w := c.wal(i); w != nil && w.hasCrashed() && c.isAlive(i) {
					c.crash(i)
					setDown(i, true)
					tornCrashes.Add(1)
				}
			}
		}
	}()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	end := time.Now().Add(10 * time.Second)
	for time.Now().Before(end) {
		if downCount() >= 2 { // keep a majority alive: bring one back before anything else
			if i, ok := firstDown(); ok {
				c.restart(i)
				setDown(i, false)
				restarts.Add(1)
			}
		}
		switch rng.Intn(8) {
		case 0:
			perm := rng.Perm(5)
			c.net.Partition(perm[:2], perm[2:])
		case 1:
			c.net.Heal()
		case 2: // clean crash
			if i := rng.Intn(5); c.isAlive(i) && downCount() < 2 {
				c.crash(i)
				setDown(i, true)
				cleanCrashes.Add(1)
			}
		case 3, 4: // restart a dead node from its file
			if i, ok := firstDown(); ok {
				c.restart(i)
				setDown(i, false)
				restarts.Add(1)
			}
		default: // die in the middle of the next write
			if i := rng.Intn(5); c.isAlive(i) && downCount() < 2 {
				if w := c.wal(i); w != nil && !w.hasCrashed() {
					w.arm(rng.Float64(), rng.Intn(4) == 0)
				}
			}
		}
		time.Sleep(time.Duration(60+rng.Intn(200)) * time.Millisecond)
	}
	close(stop)
	wg.Wait()

	// Heal everything and bring every dead node back from its file.
	c.net.Heal()
	c.net.SetChaos(0, 0, 0, 0)
	c.net.SetDup(0)
	for i := 0; i < 5; i++ {
		if !c.isAlive(i) {
			if err := c.startErr(i); err != nil {
				t.Fatalf("node %d could not restart from the file left by the chaos: %v", i, err)
			}
		}
	}
	c.waitLeader(0, 1, 2, 3, 4)
	final := c.one(999999, 5)

	cmu.Lock()
	defer cmu.Unlock()
	var cmds []int
	for _, cmd := range confirmed {
		cmds = append(cmds, cmd)
	}
	c.waitAllHave(append(cmds, 999999), 0, 1, 2, 3, 4)
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
	t.Logf("%d commands confirmed, final index %d; %d crashes in the middle of a write, %d clean crashes, %d restarts from files", len(confirmed), final, tornCrashes.Load(), cleanCrashes.Load(), restarts.Load())
	if tornCrashes.Load() < 5 {
		t.Errorf("only %d crashes in the middle of a write happened: the test is not exercising torn writes", tornCrashes.Load())
	}
}
