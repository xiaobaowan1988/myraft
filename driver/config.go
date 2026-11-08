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

	CounterSenderPos   int32 = 3 // written by the sender: how far the log has been sent
	CounterSenderLimit int32 = 4 // written by the sender: how far it may send (receiver consumption + window)
	CounterSenderCons  int32 = 5 // written by the sender: the receiver's consumption position, from its last Status
	CounterRcvHWM      int32 = 6 // written by the receiver: the highest position seen
	CounterRcvPos      int32 = 7 // written by the receiver: the position up to which data is contiguous
	CounterImageCons   int32 = 8 // written by the conductor: the slowest subscriber of an image
)

// ThreadingMode chooses how many goroutines run the agents.
type ThreadingMode int

const (
	Dedicated ThreadingMode = iota // conductor, sender and receiver each get a goroutine
	Shared                         // one goroutine runs all three in turn
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

	ReceiverWindow int // bytes a receiver lets a sender have in flight; default 128 KiB, never above half a term

	SetupInterval        time.Duration // a sender repeats Setup this often until a receiver answers; default 50ms
	HeartbeatInterval    time.Duration // an idle sender sends a heartbeat this often; default 100ms
	StatusInterval       time.Duration // a receiver reports its position at least this often; default 10ms
	ReceiverTimeout      time.Duration // a sender that hears no Status for this long considers the receiver gone; default 5s
	ImageLivenessTimeout time.Duration // an image that hears nothing from its sender for this long is drained and closed; default 10s
	NakDelay             time.Duration // a gap must persist this long before it is NAKed, to ride out reordering; default 2ms
	NakRetryInterval     time.Duration // first NAK repeat interval, doubled each time up to 200ms; default 20ms
	RetransmitLinger     time.Duration // repeated NAKs for the same range within this time are ignored; default 20ms
	MaxRetransmits       int           // concurrent retransmissions a sender serves; default 16

	Threading ThreadingMode
	// ListenPacket opens datagram sockets; default ListenUDP. Tests substitute an
	// in-memory network with injected faults.
	ListenPacket ListenFunc

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
	deft(&c.NakDelay, 2*time.Millisecond)
	deft(&c.NakRetryInterval, 20*time.Millisecond)
	deft(&c.RetransmitLinger, 20*time.Millisecond)
	def(&c.MaxRetransmits, 16)
	deft(&c.SetupInterval, 50*time.Millisecond)
	deft(&c.HeartbeatInterval, 100*time.Millisecond)
	deft(&c.StatusInterval, 10*time.Millisecond)
	deft(&c.ReceiverTimeout, 5*time.Second)
	deft(&c.ImageLivenessTimeout, 10*time.Second)
	def(&c.ReceiverWindow, 128<<10)
	if c.MTU > 65000 {
		return fmt.Errorf("driver: MTU %d is larger than a UDP datagram can carry", c.MTU)
	}
	if c.ListenPacket == nil {
		c.ListenPacket = ListenUDP
	}
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
