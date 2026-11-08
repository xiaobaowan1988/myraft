package client

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/xiaobaowan1988/myraft/driver/cnc"
	"github.com/xiaobaowan1988/myraft/driver/counters"
	"github.com/xiaobaowan1988/myraft/driver/logbuf"
	"github.com/xiaobaowan1988/myraft/driver/shm"
)

// OfferResult is the outcome of Publication.Offer.
type OfferResult int

const (
	Offered       OfferResult = iota // the message was written
	NotConnected                     // no subscriber is attached yet
	BackPressured                    // subscribers are too far behind; try again later
	AdminAction                      // the log is rotating terms; try again at once
	TooBig                           // longer than MaxMessageLength
	Closed                           // the publication or the client is closed
)

func (r OfferResult) String() string {
	return [...]string{"Offered", "NotConnected", "BackPressured", "AdminAction", "TooBig", "Closed"}[r]
}

// Publication writes messages to a stream. Any number of goroutines, in any number of
// processes, may publish to the same stream; Offer is lock-free in shared memory.
type Publication struct {
	c      *Client
	regID  int64
	stream int32

	mu      sync.RWMutex // held for reading by Offer, for writing to unmap
	closed  bool
	mapping *shm.Mapping
	log     *logbuf.LogBuffer
	limit   counters.Counter
}

// AddPublication asks the driver for a publication on the channel and stream. All
// publications of a stream in a driver share one log: their messages interleave in it.
func (c *Client) AddPublication(channel string, stream int32) (*Publication, error) {
	v, err := c.request(cnc.Command{Type: cnc.CmdAddPublication, StreamID: stream, Channel: channel}, func(ev cnc.Event) (any, error) {
		p, err := c.openPublication(ev)
		if err != nil {
			return nil, err
		}
		c.pubs[ev.RegistrationID] = p
		return p, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*Publication), nil
}

func (c *Client) openPublication(ev cnc.Event) (*Publication, error) {
	m, err := shm.Open(filepath.Join(c.opts.Dir, ev.Name))
	if err != nil {
		return nil, fmt.Errorf("client: cannot map the publication's log: %w", err)
	}
	lb, err := logbuf.Wrap(m.Buffer())
	if err != nil {
		m.Close()
		return nil, err
	}
	return &Publication{c: c, regID: ev.RegistrationID, stream: ev.StreamID, mapping: m, log: lb,
		limit: c.ctrs.Counter(int(ev.CounterID))}, nil
}

// Offer appends msg as one message and returns the stream position after it. It
// never blocks. The caller retries on BackPressured and NotConnected.
func (p *Publication) Offer(msg []byte) (int64, OfferResult) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return 0, Closed
	}
	if !p.log.IsConnected() {
		return 0, NotConnected
	}
	for i := 0; i < 16; i++ {
		pos, st := p.log.Offer(msg, p.limit.Get())
		switch st {
		case logbuf.Ok:
			return pos, Offered
		case logbuf.BackPressured:
			return 0, BackPressured
		case logbuf.TooBig:
			return 0, TooBig
		}
	}
	return 0, AdminAction
}

// IsConnected reports whether at least one subscriber is attached.
func (p *Publication) IsConnected() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return !p.closed && p.log.IsConnected()
}

// Position is where the next message will be written.
func (p *Publication) Position() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return 0
	}
	return p.log.Position()
}

// PositionLimit is how far publishers may write now.
func (p *Publication) PositionLimit() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return 0
	}
	return p.limit.Get()
}

func (p *Publication) MaxMessageLength() int { return p.log.MaxMessageLength() }
func (p *Publication) SessionID() int32      { return p.log.SessionID() }
func (p *Publication) StreamID() int32       { return p.stream }

// release unmaps the log; later calls return Closed.
func (p *Publication) release() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.closed {
		p.closed = true
		p.mapping.Close()
	}
}

// Close removes the publication. The driver keeps the log until its subscribers
// have read it (or a linger time passes).
func (p *Publication) Close() error {
	p.mu.RLock()
	closed := p.closed
	p.mu.RUnlock()
	if closed {
		return nil
	}
	_, err := p.c.request(cnc.Command{Type: cnc.CmdRemovePublication, RegistrationID: p.regID}, func(cnc.Event) (any, error) {
		delete(p.c.pubs, p.regID)
		return nil, nil
	})
	p.release()
	return err
}
