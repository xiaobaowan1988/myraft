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

// Handler receives one complete message. msg is only valid during the call.
type Handler func(msg []byte, info logbuf.FrameInfo)

// Subscription reads a stream. It has one image per publication it is attached to.
// Poll must be called by one goroutine at a time.
type Subscription struct {
	c      *Client
	regID  int64
	stream int32
	local  string

	mu     sync.RWMutex // held for reading by Poll, for writing to change images or unmap
	closed bool
	images []*Image
	rr     int
}

// Image is a subscription's view of one publication: its own position in that log.
type Image struct {
	id      int64
	session int32
	mapping *shm.Mapping
	log     *logbuf.LogBuffer
	pos     counters.Counter
	asm     *logbuf.Assembler
	deliver Handler
	count   int
}

// AddSubscription asks the driver for a subscription. It starts at the position
// each publication has reached when the two are linked: earlier messages are not seen.
func (c *Client) AddSubscription(channel string, stream int32) (*Subscription, error) {
	v, err := c.request(cnc.Command{Type: cnc.CmdAddSubscription, StreamID: stream, Channel: channel}, func(ev cnc.Event) (any, error) {
		s := &Subscription{c: c, regID: ev.CorrelationID, stream: stream, local: ev.Name}
		c.subs[s.regID] = s // registered before any image event is handled
		return s, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*Subscription), nil
}

// addImage runs in the client conductor when the driver announces an image.
func (s *Subscription) addImage(c *Client, ev cnc.Event) {
	m, err := shm.Open(filepath.Join(c.opts.Dir, ev.Name))
	if err != nil {
		return // the publication is already gone
	}
	lb, err := logbuf.Wrap(m.Buffer())
	if err != nil {
		m.Close()
		return
	}
	img := &Image{id: ev.CorrelationID, session: ev.SessionID, mapping: m, log: lb, pos: c.ctrs.Counter(int(ev.CounterID))}
	img.asm = logbuf.NewAssembler(func(p []byte, info logbuf.FrameInfo) {
		img.count++
		img.deliver(p, info)
	})
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		m.Close()
		return
	}
	s.images = append(s.images, img)
}

func (s *Subscription) removeImage(id int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, img := range s.images {
		if img.id == id {
			img.mapping.Close()
			s.images = append(s.images[:i:i], s.images[i+1:]...)
			return
		}
	}
}

// LocalAddr is the address a UDP subscription is bound to (useful when it asked for
// port 0); empty for IPC.
func (s *Subscription) LocalAddr() string { return s.local }

// ImageCount is the number of publications this subscription is attached to.
func (s *Subscription) ImageCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.images)
}

// Poll delivers up to fragmentLimit frames, as messages, from the images in turn,
// and returns how many messages were delivered. Consumption is published to the
// driver (and so to the publishers' limit) after the handler returns.
func (s *Subscription) Poll(h Handler, fragmentLimit int) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return 0, ErrClosed
	}
	n := len(s.images)
	delivered := 0
	for i := 0; i < n && fragmentLimit > 0; i++ {
		img := s.images[(s.rr+i)%n]
		img.deliver, img.count = h, 0
		pos := img.pos.Get()
		next, frags, err := img.log.Poll(pos, img.asm.OnFragment, fragmentLimit)
		if next != pos {
			img.pos.Set(next)
		}
		delivered += img.count
		fragmentLimit -= frags
		if err != nil {
			return delivered, err
		}
	}
	if n > 0 {
		s.rr = (s.rr + 1) % n
	}
	return delivered, nil
}

func (s *Subscription) release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	for _, img := range s.images {
		img.mapping.Close()
	}
	s.images = nil
}

// Close removes the subscription.
func (s *Subscription) Close() error {
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return nil
	}
	_, err := s.c.request(cnc.Command{Type: cnc.CmdRemoveSubscription, RegistrationID: s.regID}, func(cnc.Event) (any, error) {
		delete(s.c.subs, s.regID)
		return nil, nil
	})
	s.release()
	return err
}

func (i *Image) String() string { return fmt.Sprintf("image %d session %d", i.id, i.session) }
