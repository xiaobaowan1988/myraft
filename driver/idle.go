package driver

import (
	"runtime"
	"sync/atomic"
	"time"
)

// IdleStrategy decides what an agent's goroutine does when a duty cycle found no
// work: spin (lowest latency, burns a core), yield, or sleep (cheapest, adds latency).
type IdleStrategy interface {
	// Idle is called after every duty cycle with the amount of work it did.
	Idle(workCount int)
}

// BusySpin never gives up the CPU.
type BusySpin struct{}

func (BusySpin) Idle(int) {}

// Yielding yields the processor to other goroutines when idle.
type Yielding struct{}

func (Yielding) Idle(n int) {
	if n == 0 {
		runtime.Gosched()
	}
}

// Sleeping sleeps for a fixed time when idle.
type Sleeping struct{ D time.Duration }

func (s Sleeping) Idle(n int) {
	if n == 0 {
		time.Sleep(s.D)
	}
}

// Backoff spins, then yields, then parks for exponentially longer up to a limit; any
// work resets it.
type Backoff struct {
	maxSpins, maxYields int
	minPark, maxPark    time.Duration
	spins, yields       int
	park                time.Duration
}

func NewBackoffIdle(maxSpins, maxYields int, minPark, maxPark time.Duration) *Backoff {
	return &Backoff{maxSpins: maxSpins, maxYields: maxYields, minPark: minPark, maxPark: maxPark, park: minPark}
}

func (b *Backoff) Idle(n int) {
	if n > 0 {
		b.spins, b.yields, b.park = 0, 0, b.minPark
		return
	}
	switch {
	case b.spins < b.maxSpins:
		b.spins++
	case b.yields < b.maxYields:
		b.yields++
		runtime.Gosched()
	default:
		time.Sleep(b.park)
		if b.park *= 2; b.park > b.maxPark {
			b.park = b.maxPark
		}
	}
}

// Agent is a unit of work run in a loop: one DoWork call is one duty cycle and
// returns how much it did. Keeping the loop outside the agent lets tests drive an
// agent one cycle at a time with a clock they control.
type Agent interface {
	Name() string
	DoWork(now int64) int
	OnClose()
}

// runner runs an agent on its own goroutine until stopped.
type runner struct {
	agent Agent
	idle  IdleStrategy
	clock func() int64
	stop  atomic.Bool
	done  chan struct{}
}

func startRunner(a Agent, idle IdleStrategy, clock func() int64) *runner {
	r := &runner{agent: a, idle: idle, clock: clock, done: make(chan struct{})}
	go func() {
		defer close(r.done)
		for !r.stop.Load() {
			r.idle.Idle(a.DoWork(clock()))
		}
	}()
	return r
}

func (r *runner) close() {
	r.stop.Store(true)
	<-r.done
}
