package driver

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/xiaobaowan1988/myraft/driver/cnc"
	"github.com/xiaobaowan1988/myraft/driver/counters"
	"github.com/xiaobaowan1988/myraft/driver/logbuf"
	"github.com/xiaobaowan1988/myraft/driver/ring"
	"github.com/xiaobaowan1988/myraft/driver/shm"
)

const (
	ipcChannel        = "ipc"
	heartbeatInterval = int64(time.Millisecond)
	slowTickInterval  = int64(time.Millisecond)
	commandBatch      = 16
)

// Stats is a snapshot of the conductor's bookkeeping, for tests and diagnostics.
type Stats struct {
	Clients, Publications, Subscriptions, Links, Draining int
	ClientsTimedOut, Unblocked, BadCommands               int64
}

// Conductor is the driver's control-plane agent. It answers clients' commands,
// creates and destroys log buffers and counters, links publications to
// subscriptions, advances each publication's position limit, and cleans up after
// clients that stopped talking. It is single-threaded: only DoWork touches its state.
type Conductor struct {
	cfg      Config
	cnc      *cnc.File
	cmds     *ring.ManyToOne
	events   *ring.Transmitter
	counters *counters.Manager
	rng      *rand.Rand

	now           int64
	lastHeartbeat int64
	lastSlowTick  int64

	clients map[int64]*clientState
	regs    map[int64]*registration // by registration id (= the add command's correlation id)
	active  map[int32]*publication  // the publication new adders of a stream share
	pubs    map[*publication]struct{}
	subs    map[int64]*subscription

	ctr     Stats // the running counters
	dirty   bool  // something changed since the last publishStats
	statsMu sync.Mutex
	snap    Stats
}

type clientState struct {
	id            int64
	lastKeepalive int64
}

type regKind int

const (
	regPub regKind = iota
	regSub
)

type registration struct {
	kind     regKind
	clientID int64
	pub      *publication
	sub      *subscription
}

type publication struct {
	stream  int32
	session int32
	name    string
	mapping *shm.Mapping
	log     *logbuf.LogBuffer
	limitID int
	limit   counters.Counter

	refs      map[int64]struct{} // registrations
	links     []*link
	draining  bool
	drainFrom int64

	lastLimit    int64
	lastConsumer int64
	lastProgress int64
}

type subscription struct {
	regID    int64
	clientID int64
	stream   int32
	links    []*link
}

// link joins a subscription to the publication it reads: an image.
type link struct {
	sub     *subscription
	pub     *publication
	imageID int64
	posID   int
	pos     counters.Counter
}

func newConductor(cfg Config, f *cnc.File) (*Conductor, error) {
	cmds, err := f.ToDriver()
	if err != nil {
		return nil, err
	}
	tx, err := f.NewTransmitter()
	if err != nil {
		return nil, err
	}
	cm, err := f.CountersManager()
	if err != nil {
		return nil, err
	}
	return &Conductor{
		cfg: cfg, cnc: f, cmds: cmds, events: tx, counters: cm,
		rng:     rand.New(rand.NewSource(cfg.Seed)),
		clients: map[int64]*clientState{},
		regs:    map[int64]*registration{},
		active:  map[int32]*publication{},
		pubs:    map[*publication]struct{}{},
		subs:    map[int64]*subscription{},
	}, nil
}

func (c *Conductor) Name() string { return "conductor" }

// Stats returns the bookkeeping as of the end of the last duty cycle that changed
// anything. It is safe to call from any goroutine.
func (c *Conductor) Stats() Stats {
	c.statsMu.Lock()
	defer c.statsMu.Unlock()
	return c.snap
}

func (c *Conductor) publishStats() {
	s := c.ctr
	s.Clients, s.Publications, s.Subscriptions = len(c.clients), len(c.pubs), len(c.subs)
	for p := range c.pubs {
		s.Links += len(p.links)
		if p.draining {
			s.Draining++
		}
	}
	c.statsMu.Lock()
	c.snap = s
	c.statsMu.Unlock()
	c.dirty = false
}

// DoWork is one duty cycle. now is in nanoseconds.
func (c *Conductor) DoWork(now int64) int {
	c.now = now
	work := c.cmds.Read(c.onCommand, commandBatch)
	if now-c.lastHeartbeat >= heartbeatInterval {
		c.cmds.SetConsumerHeartbeat(now)
		c.lastHeartbeat = now
	}
	if c.updateLimits() {
		work++
	}
	if now-c.lastSlowTick >= slowTickInterval {
		c.lastSlowTick = now
		work += c.slowTick()
	}
	if c.dirty {
		c.publishStats()
	}
	return work
}

// OnClose releases every log buffer. Files stay on disk for the directory's owner to remove.
func (c *Conductor) OnClose() {
	for p := range c.pubs {
		p.mapping.Close()
	}
	c.pubs = map[*publication]struct{}{}
}

func (c *Conductor) nextID() int64 { return c.cmds.NextCorrelationID() }

func (c *Conductor) emit(e cnc.Event) { c.events.Transmit(e.Type, cnc.EncodeEvent(e)) }

func (c *Conductor) sendError(corr int64, code int32, format string, args ...any) {
	c.emit(cnc.Event{Type: cnc.EvError, CorrelationID: corr, Code: code, Message: fmt.Sprintf(format, args...)})
}

func (c *Conductor) client(id int64) *clientState {
	cl := c.clients[id]
	if cl == nil {
		cl = &clientState{id: id}
		c.clients[id] = cl
	}
	cl.lastKeepalive = c.now
	return cl
}

func (c *Conductor) onCommand(typ int32, payload []byte) {
	c.dirty = true
	cmd, err := cnc.DecodeCommand(typ, payload)
	if err != nil {
		c.ctr.BadCommands++ // nobody to answer: the ids may be garbage
		return
	}
	if typ == cnc.CmdClientClose {
		c.closeClient(cmd.ClientID)
		return
	}
	c.client(cmd.ClientID)
	switch typ {
	case cnc.CmdClientKeepalive:
	case cnc.CmdAddPublication:
		c.addPublication(cmd)
	case cnc.CmdRemovePublication:
		c.removeRegistration(cmd, regPub)
	case cnc.CmdAddSubscription:
		c.addSubscription(cmd)
	case cnc.CmdRemoveSubscription:
		c.removeRegistration(cmd, regSub)
	default:
		c.sendError(cmd.CorrelationID, cnc.ErrCodeGeneric, "unsupported command %d", typ)
	}
}

// --- publications ---------------------------------------------------------------

func (c *Conductor) addPublication(cmd cnc.Command) {
	if cmd.Channel != ipcChannel {
		c.sendError(cmd.CorrelationID, cnc.ErrCodeInvalidChannel, "unsupported channel %q", cmd.Channel)
		return
	}
	pub := c.active[cmd.StreamID]
	if pub == nil {
		var err error
		if pub, err = c.createPublication(cmd.StreamID, cmd.CorrelationID); err != nil {
			c.sendError(cmd.CorrelationID, cnc.ErrCodeGeneric, "cannot create publication: %v", err)
			return
		}
	}
	pub.refs[cmd.CorrelationID] = struct{}{}
	c.regs[cmd.CorrelationID] = &registration{kind: regPub, clientID: cmd.ClientID, pub: pub}
	c.emit(cnc.Event{Type: cnc.EvPublicationReady, CorrelationID: cmd.CorrelationID, RegistrationID: cmd.CorrelationID,
		SessionID: pub.session, StreamID: pub.stream, CounterID: int32(pub.limitID), Name: pub.name})
}

func (c *Conductor) createPublication(stream int32, id int64) (*publication, error) {
	name := fmt.Sprintf("pub-%d.log", id)
	m, err := shm.Create(filepath.Join(c.cfg.Dir, name), logbuf.FileLength(c.cfg.TermLength))
	if err != nil {
		return nil, err
	}
	session, initial := int32(c.rng.Uint32()), int32(c.rng.Uint32())
	lb, err := logbuf.Create(m.Buffer(), logbuf.Params{TermLength: c.cfg.TermLength, MTU: c.cfg.MTU,
		InitialTermID: initial, SessionID: session, StreamID: stream})
	if err != nil {
		m.Close()
		os.Remove(filepath.Join(c.cfg.Dir, name))
		return nil, err
	}
	limitID, err := c.counters.Allocate(CounterPubLimit, id, fmt.Sprintf("pub-lmt: %d stream %d", id, stream))
	if err != nil {
		m.Close()
		os.Remove(filepath.Join(c.cfg.Dir, name))
		return nil, err
	}
	p := &publication{stream: stream, session: session, name: name, mapping: m, log: lb,
		limitID: limitID, limit: c.counters.Counter(limitID), refs: map[int64]struct{}{},
		lastProgress: c.now}
	c.active[stream] = p
	c.pubs[p] = struct{}{}
	for _, s := range c.subs {
		if s.stream == stream {
			c.link(s, p)
		}
	}
	return p, nil
}

// link attaches a subscription to a publication. The subscriber joins at the
// publication's current position: it sees what is published from now on.
func (c *Conductor) link(s *subscription, p *publication) {
	posID, err := c.counters.Allocate(CounterSubPos, s.regID, fmt.Sprintf("sub-pos: %d stream %d session %d", s.regID, s.stream, p.session))
	if err != nil {
		c.sendError(s.regID, cnc.ErrCodeGeneric, "no counter for the image: %v", err)
		return
	}
	join := p.log.Position()
	pos := c.counters.Counter(posID)
	pos.Set(join)
	l := &link{sub: s, pub: p, imageID: c.nextID(), posID: posID, pos: pos}
	p.links = append(p.links, l)
	s.links = append(s.links, l)
	p.lastConsumer, p.lastProgress = join, c.now
	p.log.SetConnected(true)
	c.emit(cnc.Event{Type: cnc.EvAvailableImage, CorrelationID: l.imageID, RegistrationID: s.regID,
		SessionID: p.session, StreamID: p.stream, CounterID: int32(posID), Name: p.name})
}

// unlink detaches l and frees its counter.
func (c *Conductor) unlink(l *link) {
	l.pub.links = removeLink(l.pub.links, l)
	l.sub.links = removeLink(l.sub.links, l)
	c.counters.Free(l.posID)
	if len(l.pub.links) == 0 {
		l.pub.log.SetConnected(false)
	}
}

func removeLink(ls []*link, l *link) []*link {
	for i, x := range ls {
		if x == l {
			return append(ls[:i:i], ls[i+1:]...)
		}
	}
	return ls
}

// --- subscriptions ----------------------------------------------------------------

func (c *Conductor) addSubscription(cmd cnc.Command) {
	if cmd.Channel != ipcChannel {
		c.sendError(cmd.CorrelationID, cnc.ErrCodeInvalidChannel, "unsupported channel %q", cmd.Channel)
		return
	}
	s := &subscription{regID: cmd.CorrelationID, clientID: cmd.ClientID, stream: cmd.StreamID}
	c.subs[s.regID] = s
	c.regs[s.regID] = &registration{kind: regSub, clientID: cmd.ClientID, sub: s}
	// The client creates its subscription when it sees this, so it must precede the images.
	c.emit(cnc.Event{Type: cnc.EvOperationSuccess, CorrelationID: cmd.CorrelationID})
	for p := range c.pubs {
		if p.stream == s.stream && !p.draining {
			c.link(s, p)
		}
	}
}

// --- removal ---------------------------------------------------------------------

func (c *Conductor) removeRegistration(cmd cnc.Command, kind regKind) {
	r := c.regs[cmd.RegistrationID]
	if r == nil || r.kind != kind || r.clientID != cmd.ClientID {
		c.sendError(cmd.CorrelationID, cnc.ErrCodeUnknownRef, "unknown registration %d", cmd.RegistrationID)
		return
	}
	c.removeReg(cmd.RegistrationID, r)
	c.emit(cnc.Event{Type: cnc.EvOperationSuccess, CorrelationID: cmd.CorrelationID})
}

func (c *Conductor) removeReg(id int64, r *registration) {
	delete(c.regs, id)
	switch r.kind {
	case regPub:
		p := r.pub
		delete(p.refs, id)
		if len(p.refs) == 0 && !p.draining {
			p.draining, p.drainFrom = true, c.now
			if c.active[p.stream] == p {
				delete(c.active, p.stream)
			}
			c.maybeFinish(p)
		}
	case regSub:
		s := r.sub
		delete(c.subs, s.regID)
		for _, l := range append([]*link(nil), s.links...) {
			p := l.pub
			c.unlink(l)
			if p.draining {
				c.maybeFinish(p)
			}
		}
	}
}

func (c *Conductor) closeClient(id int64) {
	delete(c.clients, id)
	for regID, r := range c.regs {
		if r.clientID == id {
			c.removeReg(regID, r)
		}
	}
}

// maybeFinish destroys a draining publication once its subscribers have read
// everything, or it has lingered long enough.
func (c *Conductor) maybeFinish(p *publication) {
	if !p.draining {
		return
	}
	done := len(p.links) == 0
	if !done {
		end := p.log.Position()
		done = c.now-p.drainFrom > int64(c.cfg.Linger)
		if !done {
			done = true
			for _, l := range p.links {
				if l.pos.Get() < end {
					done = false
					break
				}
			}
		}
	}
	if !done {
		return
	}
	for _, l := range append([]*link(nil), p.links...) {
		c.emit(cnc.Event{Type: cnc.EvUnavailableImage, CorrelationID: l.imageID, RegistrationID: l.sub.regID, StreamID: p.stream})
		c.unlink(l)
	}
	c.counters.Free(p.limitID)
	p.mapping.Close()
	os.Remove(filepath.Join(c.cfg.Dir, p.name))
	delete(c.pubs, p)
	c.dirty = true
}

// --- periodic work ------------------------------------------------------------------

// updateLimits advances each publication's limit to slowest-subscriber + window. It
// reports whether any limit moved.
func (c *Conductor) updateLimits() bool {
	changed := false
	for p := range c.pubs {
		if len(p.links) == 0 {
			continue
		}
		min := p.links[0].pos.Get()
		for _, l := range p.links[1:] {
			if v := l.pos.Get(); v < min {
				min = v
			}
		}
		if lim := min + int64(c.cfg.PublicationWindow); lim != p.lastLimit {
			p.limit.Set(lim)
			p.lastLimit = lim
			changed = true
		}
	}
	return changed
}

func (c *Conductor) slowTick() int {
	work := 0
	timeout := int64(c.cfg.ClientLivenessTimeout)
	for id, cl := range c.clients {
		if c.now-cl.lastKeepalive > timeout {
			c.ctr.ClientsTimedOut++
			c.dirty = true
			c.closeClient(id)
			work++
		}
	}
	for p := range c.pubs {
		if p.draining {
			c.maybeFinish(p)
			continue
		}
		c.checkForHole(p)
	}
	return work
}

// checkForHole unblocks a publication whose slowest reader has stood still for too
// long in front of unpublished space: a publisher that died mid-write.
func (c *Conductor) checkForHole(p *publication) {
	if len(p.links) == 0 {
		return
	}
	consumer := p.links[0].pos.Get()
	for _, l := range p.links[1:] {
		if v := l.pos.Get(); v < consumer {
			consumer = v
		}
	}
	if consumer != p.lastConsumer || consumer >= p.log.Position() {
		p.lastConsumer, p.lastProgress = consumer, c.now
		return
	}
	if c.now-p.lastProgress > int64(c.cfg.UnblockTimeout) {
		if p.log.Unblock(consumer) {
			c.ctr.Unblocked++
			c.dirty = true
		}
		p.lastProgress = c.now
	}
}
