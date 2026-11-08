// Package client is the application side of the media driver. It maps the driver's
// CnC file, asks the driver for publications and subscriptions, and then reads and
// writes the log buffers in shared memory directly, without involving the driver.
package client

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/xiaobaowan1988/myraft/driver/cnc"
	"github.com/xiaobaowan1988/myraft/driver/counters"
	"github.com/xiaobaowan1988/myraft/driver/ring"
)

var (
	ErrDriverNotActive = errors.New("client: no active driver in that directory")
	ErrDriverTimeout   = errors.New("client: the driver stopped sending heartbeats")
	ErrClosed          = errors.New("client: closed")
	ErrTimeout         = errors.New("client: the driver did not answer in time")
)

// DriverError is an error the driver reported for a request.
type DriverError struct {
	Code    int32
	Message string
}

func (e *DriverError) Error() string { return fmt.Sprintf("driver error %d: %s", e.Code, e.Message) }

// Options configures Connect. Zero values get the defaults noted.
type Options struct {
	Dir               string        // the driver directory; required
	ConnectTimeout    time.Duration // wait this long for a driver to be there; default 10s
	DriverTimeout     time.Duration // heartbeat older than this means the driver is dead; default 10s
	ResourceTimeout   time.Duration // wait this long for an answer to a request; default 10s
	KeepaliveInterval time.Duration // default a fifth of the driver's client timeout, at most 500ms
	Clock             func() int64  // nanoseconds; tests replace it
	// ManualConductor stops Connect from starting the background goroutine that
	// handles driver events and sends keepalives. The caller drives DoWork instead
	// (requests also drive it while they wait).
	ManualConductor bool
	// Pump, if set, is called each time a request waits for the driver's answer. A
	// test that runs the driver by hand uses it to advance the whole simulation.
	Pump func()
}

// Client is a connection to a media driver. It is safe for concurrent use.
type Client struct {
	opts Options
	file *cnc.File
	cmds *ring.ManyToOne
	rx   *ring.Receiver
	ctrs *counters.Reader
	id   int64

	mu            sync.Mutex // guards everything below and serialises DoWork
	pending       map[int64]*pending
	pubs          map[int64]*Publication
	subs          map[int64]*Subscription
	err           error // set when the client has failed; sticky
	closed        bool
	lastKeepalive int64

	stop chan struct{}
	done chan struct{}
}

type pending struct {
	build func(ev cnc.Event) (any, error) // runs in DoWork, under mu
	res   chan result
}

type result struct {
	val any
	err error
}

// Connect attaches to the driver in opts.Dir.
func Connect(opts Options) (*Client, error) {
	if opts.Dir == "" {
		return nil, errors.New("client: Options.Dir is required")
	}
	if opts.ConnectTimeout == 0 {
		opts.ConnectTimeout = 10 * time.Second
	}
	if opts.DriverTimeout == 0 {
		opts.DriverTimeout = 10 * time.Second
	}
	if opts.ResourceTimeout == 0 {
		opts.ResourceTimeout = 10 * time.Second
	}
	if opts.Clock == nil {
		opts.Clock = func() int64 { return time.Now().UnixNano() }
	}
	deadline := time.Now().Add(opts.ConnectTimeout)
	var f *cnc.File
	for {
		var err error
		f, err = cnc.Open(opts.Dir)
		if err == nil {
			break
		}
		if !errors.Is(err, cnc.ErrNotReady) && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("%w: %v", ErrDriverNotActive, err)
		}
		time.Sleep(time.Millisecond)
	}
	c := &Client{opts: opts, file: f, pending: map[int64]*pending{}, pubs: map[int64]*Publication{}, subs: map[int64]*Subscription{}}
	var err error
	if c.cmds, err = f.ToDriver(); err == nil {
		if c.rx, err = f.NewReceiver(); err == nil {
			c.ctrs, err = f.CountersReader()
		}
	}
	if err != nil {
		f.Close()
		return nil, err
	}
	if age := time.Duration(opts.Clock() - c.cmds.ConsumerHeartbeat()); age > opts.DriverTimeout {
		f.Close()
		return nil, fmt.Errorf("%w (heartbeat %v old)", ErrDriverNotActive, age)
	}
	if c.opts.KeepaliveInterval == 0 {
		c.opts.KeepaliveInterval = min(500*time.Millisecond, time.Duration(f.ClientTimeoutNs())/5)
	}
	c.id = c.cmds.NextCorrelationID()
	c.lastKeepalive = opts.Clock()
	c.send(cnc.Command{Type: cnc.CmdClientKeepalive, ClientID: c.id}) // register with the driver at once
	c.stop, c.done = make(chan struct{}), make(chan struct{})
	if opts.ManualConductor {
		close(c.done)
	} else {
		go c.loop()
	}
	return c, nil
}

// Counters gives read access to the driver's counters table.
func (c *Client) Counters() *counters.Reader { return c.ctrs }

// ID is this client's id, as the driver knows it.
func (c *Client) ID() int64 { return c.id }

// Err returns why the client failed (for instance ErrDriverTimeout), or nil.
func (c *Client) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

func (c *Client) loop() {
	defer close(c.done)
	for {
		select {
		case <-c.stop:
			return
		default:
		}
		if c.DoWork() == 0 {
			time.Sleep(200 * time.Microsecond)
		}
	}
}

// DoWork handles driver events, sends a keepalive when due, and checks that the
// driver is alive. The background goroutine calls it; with ManualConductor the
// caller does. It returns the number of events handled.
func (c *Client) DoWork() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.err != nil {
		return 0
	}
	now := c.opts.Clock()
	n := 0
	for i := 0; i < 64; i++ {
		typ, payload, ok := c.rx.Receive()
		if !ok {
			break
		}
		n++
		ev, err := cnc.DecodeEvent(typ, payload)
		if err != nil {
			continue
		}
		c.onEvent(ev)
	}
	if now-c.lastKeepalive >= int64(c.opts.KeepaliveInterval) {
		c.send(cnc.Command{Type: cnc.CmdClientKeepalive, ClientID: c.id})
		c.lastKeepalive = now
	}
	if time.Duration(now-c.cmds.ConsumerHeartbeat()) > c.opts.DriverTimeout {
		c.failLocked(ErrDriverTimeout)
	}
	return n
}

func (c *Client) onEvent(ev cnc.Event) {
	switch ev.Type {
	case cnc.EvPublicationReady, cnc.EvOperationSuccess:
		if p := c.pending[ev.CorrelationID]; p != nil {
			delete(c.pending, ev.CorrelationID)
			v, err := p.build(ev)
			p.res <- result{v, err}
		}
	case cnc.EvError:
		if p := c.pending[ev.CorrelationID]; p != nil {
			delete(c.pending, ev.CorrelationID)
			p.res <- result{nil, &DriverError{Code: ev.Code, Message: ev.Message}}
		}
	case cnc.EvAvailableImage:
		if s := c.subs[ev.RegistrationID]; s != nil {
			s.addImage(c, ev)
		}
	case cnc.EvUnavailableImage:
		if s := c.subs[ev.RegistrationID]; s != nil {
			s.removeImage(ev.CorrelationID)
		}
	}
}

// send puts a command in the driver's ring, retrying briefly if the ring is full.
func (c *Client) send(cmd cnc.Command) bool {
	b := cnc.EncodeCommand(cmd)
	for i := 0; i < 1000; i++ {
		if c.cmds.Write(cmd.Type, b) {
			return true
		}
		time.Sleep(100 * time.Microsecond)
	}
	return false
}

// request sends cmd and waits for the driver's answer. build runs when it arrives.
func (c *Client) request(cmd cnc.Command, build func(cnc.Event) (any, error)) (any, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrClosed
	}
	if c.err != nil {
		err := c.err
		c.mu.Unlock()
		return nil, err
	}
	cmd.ClientID = c.id
	cmd.CorrelationID = c.cmds.NextCorrelationID()
	p := &pending{build: build, res: make(chan result, 1)}
	c.pending[cmd.CorrelationID] = p
	c.mu.Unlock()
	if !c.send(cmd) {
		c.dropPending(cmd.CorrelationID)
		return nil, errors.New("client: the driver's command ring is full")
	}
	deadline := time.Now().Add(c.opts.ResourceTimeout)
	for {
		if c.opts.Pump != nil {
			c.opts.Pump()
		}
		if c.opts.ManualConductor {
			c.DoWork()
		}
		select {
		case r := <-p.res:
			return r.val, r.err
		case <-time.After(200 * time.Microsecond):
		}
		if err := c.Err(); err != nil {
			c.dropPending(cmd.CorrelationID)
			return nil, err
		}
		if time.Now().After(deadline) {
			c.dropPending(cmd.CorrelationID)
			return nil, ErrTimeout
		}
	}
}

func (c *Client) dropPending(id int64) {
	c.mu.Lock()
	delete(c.pending, id)
	c.mu.Unlock()
}

// failLocked marks the client dead and releases every mapping. Caller holds c.mu.
func (c *Client) failLocked(err error) {
	if c.err == nil {
		c.err = err
	}
	for id, p := range c.pending {
		delete(c.pending, id)
		p.res <- result{nil, err}
	}
	for _, p := range c.pubs {
		p.release()
	}
	for _, s := range c.subs {
		s.release()
	}
	c.pubs, c.subs = map[int64]*Publication{}, map[int64]*Subscription{}
}

// Close tells the driver this client is gone, releases every mapping, and stops the
// background goroutine. Publications and subscriptions become unusable.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	failed := c.err != nil
	c.mu.Unlock()
	if !failed {
		c.send(cnc.Command{Type: cnc.CmdClientClose, ClientID: c.id})
	}
	close(c.stop)
	<-c.done
	c.mu.Lock()
	c.failLocked(ErrClosed)
	c.mu.Unlock()
	return c.file.Close()
}
