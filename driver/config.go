// Package driver is the media driver: the process that owns the transport. Clients
// talk to it through the shared-memory CnC file and log buffers; it moves their
// messages between processes (IPC) and, from stage 21.4, between machines (UDP).
package driver

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/xiaobaowan1988/myraft/driver/logbuf"
)

// Counter type ids, as shown in the counters table.
const (
	CounterPubLimit int32 = 1 // written by the conductor: how far publishers may write
	CounterSubPos   int32 = 2 // written by the subscriber: how far it has consumed an image
)

// Config configures a MediaDriver. Zero values get the defaults noted.
type Config struct {
	Dir string // the driver directory; required

	TermLength        int // log buffer term length; default 16 MiB
	MTU               int // max frame size on IPC; default 1408
	PublicationWindow int // how far publishers may run ahead of the slowest subscriber; default TermLength/4, at most TermLength/2
	ToDriverCapacity  int // command ring size; default 64 KiB
	ToClientsCapacity int // event broadcast size; default 64 KiB
	MaxCounters       int // default 1024

	ClientLivenessTimeout time.Duration // a client silent this long is dropped with all its resources; default 5s
	DriverTimeout         time.Duration // a driver heartbeat older than this means the driver is dead; default 10s
	UnblockTimeout        time.Duration // how long a reader may stand still before a publisher's hole is unblocked; default 10s
	Linger                time.Duration // how long a removed publication waits for its subscribers to drain it; default 5s

	// Clock returns nanoseconds. Tests replace it to control time.
	Clock func() int64
	// Idle makes the idle strategy of each agent runner. Default: backoff, parking up to 1ms.
	Idle func() IdleStrategy
	// Seed seeds session and term ids; default time-based.
	Seed int64
	// DeleteDirOnClose removes the driver directory when the driver closes.
	DeleteDirOnClose bool
}

func (c *Config) setDefaults() error {
	if c.Dir == "" {
		return errors.New("driver: Config.Dir is required")
	}
	def := func(v *int, d int) {
		if *v == 0 {
			*v = d
		}
	}
	def(&c.TermLength, 16<<20)
	def(&c.MTU, 1408)
	def(&c.PublicationWindow, c.TermLength/4)
	def(&c.ToDriverCapacity, 64<<10)
	def(&c.ToClientsCapacity, 64<<10)
	def(&c.MaxCounters, 1024)
	if c.PublicationWindow > c.TermLength/2 {
		return fmt.Errorf("driver: publication window %d exceeds half the term length %d", c.PublicationWindow, c.TermLength/2)
	}
	if err := (logbuf.Params{TermLength: c.TermLength, MTU: c.MTU}).Validate(); err != nil {
		return err
	}
	deft := func(v *time.Duration, d time.Duration) {
		if *v == 0 {
			*v = d
		}
	}
	deft(&c.ClientLivenessTimeout, 5*time.Second)
	deft(&c.DriverTimeout, 10*time.Second)
	deft(&c.UnblockTimeout, 10*time.Second)
	deft(&c.Linger, 5*time.Second)
	if c.Clock == nil {
		c.Clock = func() int64 { return time.Now().UnixNano() }
	}
	if c.Idle == nil {
		c.Idle = func() IdleStrategy { return NewBackoffIdle(100, 10, time.Microsecond, time.Millisecond) }
	}
	if c.Seed == 0 {
		c.Seed = time.Now().UnixNano()
	}
	return nil
}

// ErrActiveDriver is returned by Open when the directory belongs to a driver that is still alive.
var ErrActiveDriver = errors.New("driver: another driver is active in this directory")

func ensureDir(dir string) error { return os.MkdirAll(dir, 0o755) }
