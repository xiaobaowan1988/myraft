package driver

import (
	"fmt"
	"math/rand"
	"net/netip"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/xiaobaowan1988/myraft/driver/cnc"
	"github.com/xiaobaowan1988/myraft/driver/counters"
	"github.com/xiaobaowan1988/myraft/driver/logbuf"
	"github.com/xiaobaowan1988/myraft/driver/proto"
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
	Images, Endpoints                                     int
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
	q        queue // work posted by the receiver
	snd      *Sender
	rcv      *Receiver

	now           int64
	lastHeartbeat int64
	lastSlowTick  int64

	clients   map[int64]*clientState
	regs      map[int64]*registration // by registration id (= the add command's correlation id)
	active    map[pubKey]*publication // the publication new adders of a stream (and destination) share
	endpoints map[netip.AddrPort]*endpoint
	pubs      map[*publication]struct{}
	subs      map[int64]*subscription

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

type pubKind int

const (
	kindIPC   pubKind = iota // a log shared with subscribers in this driver
	kindNet                  // a log the sender transmits to a remote receiver
	kindImage                // a log the receiver rebuilds from a remote sender
)

type pubKey struct {
	stream int32
	dest   netip.AddrPort // zero for IPC
}

// publication is a log buffer and everything attached to it. Despite the name it
// also stands for an image, the receiving side's log, which has no publishers.
type publication struct {
	kind    pubKind
	stream  int32
	session int32
	name    string
	mapping *shm.Mapping
	log     *logbuf.LogBuffer
	ctrIDs  []int // every counter to free when it is destroyed
	limitID int
	limit   counters.Counter // pub-lmt, for IPC and network publications

	dest    netip.AddrPort // network publication
	snd     *senderPub
	sndPos  counters.Counter
	sndCons counters.Counter

	ep       *endpoint // image
	key      imgKey
	rcvPos   counters.Counter
	imgCons  counters.Counter
	joinPos  int64
	lastCons int64

	refs      map[int64]struct{} // registrations
	links     []*link
	draining  bool
	closing   bool // handed back by its agent; destroyed when the agent confirms
	drainFrom int64

	lastLimit    int64
	lastConsumer int64
	lastProgress int64
}

// endpoint is a bound receiving socket shared by the subscriptions on one address.
type endpoint struct {
	id      int64
	addr    netip.AddrPort
	refs    int
	streams map[int32]int
	images  map[imgKey]*publication
}

type subscription struct {
	regID    int64
	clientID int64
	stream   int32
	kind     int
	ep       *endpoint
	links    []*link
}

// imageRequest is the receiver asking for an image after a Setup.
type imageRequest struct {
	epID  int64
	src   netip.AddrPort
	setup proto.Setup
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
		rng:       rand.New(rand.NewSource(cfg.Seed)),
		clients:   map[int64]*clientState{},
		regs:      map[int64]*registration{},
		active:    map[pubKey]*publication{},
		endpoints: map[netip.AddrPort]*endpoint{},
		q:         newQueue(),
		pubs:      map[*publication]struct{}{},
		subs:      map[int64]*subscription{},
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
		if p.kind == kindImage {
			s.Images++
		}
	}
	s.Endpoints = len(c.endpoints)
	c.statsMu.Lock()
	c.snap = s
	c.statsMu.Unlock()
	c.dirty = false
}

// DoWork is one duty cycle. now is in nanoseconds.
func (c *Conductor) DoWork(now int64) int {
	c.now = now
	work := c.q.drain()
	work += c.cmds.Read(c.onCommand, commandBatch)
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

// OnClose releases every log buffer. Files stay on disk for the directory's owner to
// remove. Sockets belong to the sender and receiver by now, which close them.
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

// --- resources -------------------------------------------------------------------

// newLog creates a log buffer file and keeps the mapping and handle in a publication.
func (c *Conductor) newLog(kind pubKind, id int64, prefix string, prm logbuf.Params) (*publication, error) {
	name := fmt.Sprintf("%s-%d.log", prefix, id)
	path := filepath.Join(c.cfg.Dir, name)
	m, err := shm.Create(path, logbuf.FileLength(prm.TermLength))
	if err != nil {
		return nil, err
	}
	lb, err := logbuf.Create(m.Buffer(), prm)
	if err != nil {
		m.Close()
		os.Remove(path)
		return nil, err
	}
	return &publication{kind: kind, stream: prm.StreamID, session: prm.SessionID, name: name, mapping: m, log: lb,
		refs: map[int64]struct{}{}, lastProgress: c.now}, nil
}

// counter allocates a counter owned by p; it is freed when p is destroyed.
func (c *Conductor) counter(p *publication, typ int32, regID int64, label string) (counters.Counter, int, error) {
	id, err := c.counters.Allocate(typ, regID, label)
	if err != nil {
		return counters.Counter{}, -1, err
	}
	p.ctrIDs = append(p.ctrIDs, id)
	return c.counters.Counter(id), id, nil
}

// abort undoes a half-built publication.
func (c *Conductor) abort(p *publication) {
	for _, id := range p.ctrIDs {
		c.counters.Free(id)
	}
	p.mapping.Close()
	os.Remove(filepath.Join(c.cfg.Dir, p.name))
}

// --- publications ---------------------------------------------------------------

func (c *Conductor) addPublication(cmd cnc.Command) {
	kind, addr, err := parseChannel(cmd.Channel)
	if err != nil || (kind == chanUDP && addr.Port() == 0) {
		c.sendError(cmd.CorrelationID, cnc.ErrCodeInvalidChannel, "bad publication channel %q", cmd.Channel)
		return
	}
	key := pubKey{stream: cmd.StreamID, dest: addr}
	pub := c.active[key]
	if pub == nil {
		if kind == chanIPC {
			pub, err = c.createIPCPublication(cmd.StreamID, cmd.CorrelationID)
		} else {
			pub, err = c.createNetPublication(cmd.StreamID, addr, cmd.CorrelationID)
		}
		if err != nil {
			c.sendError(cmd.CorrelationID, cnc.ErrCodeGeneric, "cannot create publication: %v", err)
			return
		}
		c.active[key] = pub
		c.pubs[pub] = struct{}{}
		if kind == chanIPC {
			for _, s := range c.subs {
				if c.matches(s, pub) {
					c.link(s, pub)
				}
			}
		}
	}
	pub.refs[cmd.CorrelationID] = struct{}{}
	c.regs[cmd.CorrelationID] = &registration{kind: regPub, clientID: cmd.ClientID, pub: pub}
	c.emit(cnc.Event{Type: cnc.EvPublicationReady, CorrelationID: cmd.CorrelationID, RegistrationID: cmd.CorrelationID,
		SessionID: pub.session, StreamID: pub.stream, CounterID: int32(pub.limitID), Name: pub.name})
}

func (c *Conductor) pubParams(stream int32) logbuf.Params {
	return logbuf.Params{TermLength: c.cfg.TermLength, MTU: c.cfg.MTU,
		InitialTermID: int32(c.rng.Uint32()), SessionID: int32(c.rng.Uint32()), StreamID: stream}
}

func (c *Conductor) createIPCPublication(stream int32, id int64) (*publication, error) {
	p, err := c.newLog(kindIPC, id, "pub", c.pubParams(stream))
	if err != nil {
		return nil, err
	}
	var cid int
	if p.limit, cid, err = c.counter(p, CounterPubLimit, id, fmt.Sprintf("pub-lmt: %d stream %d", id, stream)); err != nil {
		c.abort(p)
		return nil, err
	}
	p.limitID = cid
	return p, nil
}

func (c *Conductor) createNetPublication(stream int32, dest netip.AddrPort, id int64) (*publication, error) {
	prm := c.pubParams(stream)
	p, err := c.newLog(kindNet, id, "pub", prm)
	if err != nil {
		return nil, err
	}
	label := func(what string) string { return fmt.Sprintf("%s: %d stream %d -> %v", what, id, stream, dest) }
	var cid int
	if p.limit, cid, err = c.counter(p, CounterPubLimit, id, label("pub-lmt")); err != nil {
		c.abort(p)
		return nil, err
	}
	p.limitID = cid
	posC, _, err1 := c.counter(p, CounterSenderPos, id, label("snd-pos"))
	limC, _, err2 := c.counter(p, CounterSenderLimit, id, label("snd-lmt"))
	consC, _, err3 := c.counter(p, CounterSenderCons, id, label("snd-cons"))
	if err1 != nil || err2 != nil || err3 != nil {
		c.abort(p)
		return nil, ErrNoCounters
	}
	local := netip.AddrPortFrom(netip.IPv4Unspecified(), 0)
	if dest.Addr().Is6() {
		local = netip.AddrPortFrom(netip.IPv6Unspecified(), 0)
	}
	conn, err := c.cfg.ListenPacket(local)
	if err != nil {
		c.abort(p)
		return nil, err
	}
	p.dest, p.sndPos, p.sndCons = dest, posC, consC
	p.snd = &senderPub{conn: conn, dest: dest, log: p.log, posC: posC, limC: limC, consC: consC,
		session: prm.SessionID, stream: stream, initialTerm: prm.InitialTermID,
		termLength: prm.TermLength, mtu: prm.MTU, termShift: uint(log2(prm.TermLength))}
	c.snd.q.post(func() { c.snd.add(p.snd) })
	return p, nil
}

// ErrNoCounters is returned when the counters table is full.
var ErrNoCounters = fmt.Errorf("driver: no free counters")

func (c *Conductor) matches(s *subscription, p *publication) bool {
	if p.draining || s.stream != p.stream {
		return false
	}
	if s.kind == chanIPC {
		return p.kind == kindIPC
	}
	return p.kind == kindImage && p.ep == s.ep
}

// joinPosition is where a subscriber that links to p now starts reading.
func (p *publication) joinPosition() int64 {
	if p.kind == kindImage {
		return p.rcvPos.Get()
	}
	return p.log.Position()
}

// endPosition is how far p's log is filled: what a draining subscriber must reach.
func (p *publication) endPosition() int64 {
	if p.kind == kindImage {
		return p.rcvPos.Get()
	}
	return p.log.Position()
}

// link attaches a subscription to a log. The subscriber joins at the log's current
// position: it sees what arrives from now on.
func (c *Conductor) link(s *subscription, p *publication) {
	posID, err := c.counters.Allocate(CounterSubPos, s.regID, fmt.Sprintf("sub-pos: %d stream %d session %d", s.regID, s.stream, p.session))
	if err != nil {
		c.sendError(s.regID, cnc.ErrCodeGeneric, "no counter for the image: %v", err)
		return
	}
	join := p.joinPosition()
	pos := c.counters.Counter(posID)
	pos.Set(join)
	l := &link{sub: s, pub: p, imageID: c.nextID(), posID: posID, pos: pos}
	p.links = append(p.links, l)
	s.links = append(s.links, l)
	p.lastConsumer, p.lastProgress = join, c.now
	if p.kind == kindIPC {
		p.log.SetConnected(true)
	}
	c.emit(cnc.Event{Type: cnc.EvAvailableImage, CorrelationID: l.imageID, RegistrationID: s.regID,
		SessionID: p.session, StreamID: p.stream, CounterID: int32(posID), Name: p.name})
}

// unlink detaches l and frees its counter.
func (c *Conductor) unlink(l *link) {
	l.pub.links = removeLink(l.pub.links, l)
	l.sub.links = removeLink(l.sub.links, l)
	c.counters.Free(l.posID)
	if len(l.pub.links) == 0 && l.pub.kind == kindIPC {
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
	kind, addr, err := parseChannel(cmd.Channel)
	if err != nil {
		c.sendError(cmd.CorrelationID, cnc.ErrCodeInvalidChannel, "bad subscription channel %q", cmd.Channel)
		return
	}
	s := &subscription{regID: cmd.CorrelationID, clientID: cmd.ClientID, stream: cmd.StreamID, kind: kind}
	name := ""
	if kind == chanUDP {
		ep := c.endpoints[addr]
		if ep == nil || addr.Port() == 0 {
			conn, err := c.cfg.ListenPacket(addr)
			if err != nil {
				c.sendError(cmd.CorrelationID, cnc.ErrCodeGeneric, "cannot bind %v: %v", addr, err)
				return
			}
			ep = &endpoint{id: c.nextID(), addr: conn.LocalAddr(), streams: map[int32]int{}, images: map[imgKey]*publication{}}
			c.endpoints[ep.addr] = ep
			epID := ep.id
			c.rcv.q.post(func() { c.rcv.addEndpoint(epID, conn) })
		}
		ep.refs++
		ep.streams[s.stream]++
		epID, stream := ep.id, s.stream
		c.rcv.q.post(func() { c.rcv.addStream(epID, stream) })
		s.ep = ep
		name = ep.addr.String()
	}
	c.subs[s.regID] = s
	c.regs[s.regID] = &registration{kind: regSub, clientID: cmd.ClientID, sub: s}
	// The client creates its subscription when it sees this, so it must precede the images.
	c.emit(cnc.Event{Type: cnc.EvOperationSuccess, CorrelationID: cmd.CorrelationID, Name: name})
	for p := range c.pubs {
		if c.matches(s, p) {
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
			key := pubKey{stream: p.stream, dest: p.dest}
			if c.active[key] == p {
				delete(c.active, key)
			}
			c.maybeFinish(p)
		}
	case regSub:
		s := r.sub
		delete(c.subs, s.regID)
		for _, l := range append([]*link(nil), s.links...) {
			p := l.pub
			c.unlink(l)
			if p.kind == kindImage && len(p.links) == 0 && !p.draining {
				p.draining, p.drainFrom = true, c.now // nobody left to read it
			}
			if p.draining {
				c.maybeFinish(p)
			}
		}
		if s.ep != nil {
			c.releaseEndpoint(s.ep, s.stream)
		}
	}
}

func (c *Conductor) releaseEndpoint(ep *endpoint, stream int32) {
	epID := ep.id
	c.rcv.q.post(func() { c.rcv.removeStream(epID, stream) })
	if ep.streams[stream]--; ep.streams[stream] <= 0 {
		delete(ep.streams, stream)
	}
	if ep.refs--; ep.refs > 0 {
		return
	}
	for _, p := range ep.images {
		p.draining, p.drainFrom = true, c.now
		c.maybeFinish(p)
	}
	delete(c.endpoints, ep.addr)
	c.rcv.q.post(func() { c.rcv.removeEndpoint(epID) })
}

func (c *Conductor) closeClient(id int64) {
	delete(c.clients, id)
	for regID, r := range c.regs {
		if r.clientID == id {
			c.removeReg(regID, r)
		}
	}
}

// maybeFinish destroys a draining publication once nothing more can be delivered
// through it, or it has lingered long enough.
func (c *Conductor) maybeFinish(p *publication) {
	if !p.draining || p.closing {
		return
	}
	done := c.now-p.drainFrom > int64(c.cfg.Linger)
	if !done {
		switch p.kind {
		case kindNet:
			// Everything the receiver was sent has been consumed, or there is no receiver.
			done = !p.log.IsConnected() || p.sndCons.Get() >= p.log.Position()
		default:
			done = true
			end := p.endPosition()
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
	// The sender and the receiver read and write the log without any lock, so the
	// memory may be unmapped only after they have confirmed they let go of it. The
	// publication stays in the books (closing) until they do.
	p.closing = true
	switch p.kind {
	case kindNet:
		snd := p.snd
		if !c.snd.q.post(func() {
			c.snd.remove(snd)
			c.q.post(func() { c.destroy(p) })
		}) {
			c.destroy(p) // the sender is not running (the driver is shutting down)
		}
	case kindImage:
		epID, key := p.ep.id, p.key
		delete(p.ep.images, p.key)
		if !c.rcv.q.post(func() {
			c.rcv.closeImage(epID, key)
			c.q.post(func() { c.destroy(p) })
		}) {
			c.destroy(p)
		}
	default:
		c.destroy(p)
	}
}

// destroy frees everything a publication owns.
func (c *Conductor) destroy(p *publication) {
	for _, id := range p.ctrIDs {
		c.counters.Free(id)
	}
	p.mapping.Close()
	os.Remove(filepath.Join(c.cfg.Dir, p.name))
	delete(c.pubs, p)
	c.dirty = true
}

// --- images (the receiving side) -----------------------------------------------

// createImage runs on the conductor when the receiver saw a Setup for a stream that
// has subscribers on that endpoint.
func (c *Conductor) createImage(req imageRequest) {
	var ep *endpoint
	for _, e := range c.endpoints {
		if e.id == req.epID {
			ep = e
		}
	}
	s := req.setup
	key := imgKey{s.SessionID, s.StreamID}
	if ep == nil || ep.images[key] != nil {
		return
	}
	var subs []*subscription
	for _, sub := range c.subs {
		if sub.kind == chanUDP && sub.ep == ep && sub.stream == s.StreamID {
			subs = append(subs, sub)
		}
	}
	if len(subs) == 0 {
		return
	}
	prm := logbuf.Params{TermLength: int(s.TermLength), MTU: int(s.MTU), InitialTermID: s.InitialTermID, SessionID: s.SessionID, StreamID: s.StreamID}
	window := min(c.cfg.ReceiverWindow, prm.TermLength/2)
	if window < 2*prm.MTU {
		return // a window that cannot hold two datagrams would stall the sender
	}
	id := c.nextID()
	p, err := c.newLog(kindImage, id, "img", prm)
	if err != nil {
		c.ctr.BadCommands++
		return
	}
	join := int64(s.TermID-s.InitialTermID)<<log2(prm.TermLength) + int64(s.TermOffset)
	label := func(what string) string {
		return fmt.Sprintf("%s: %d stream %d session %d", what, id, s.StreamID, s.SessionID)
	}
	hwm, _, e1 := c.counter(p, CounterRcvHWM, id, label("rcv-hwm"))
	pos, _, e2 := c.counter(p, CounterRcvPos, id, label("rcv-pos"))
	cons, _, e3 := c.counter(p, CounterImageCons, id, label("img-cons"))
	if e1 != nil || e2 != nil || e3 != nil {
		c.abort(p)
		return
	}
	hwm.Set(join)
	pos.Set(join)
	cons.Set(join)
	p.ep, p.key, p.rcvPos, p.imgCons, p.joinPos, p.lastCons = ep, key, pos, cons, join, join
	ep.images[key] = p
	c.pubs[p] = struct{}{}
	c.dirty = true
	h := imageHandle{key: key, src: req.src, log: p.log, hwmC: hwm, rcvPosC: pos, consC: cons, joinPos: join, window: window, imageName: p.name}
	now, epID := c.now, ep.id
	c.rcv.q.post(func() { c.rcv.registerImage(epID, h, now) })
	for _, sub := range subs {
		c.link(sub, p)
	}
}

// imageInactive runs when an image heard nothing from its sender for too long: its
// subscribers may still read what arrived, then it is closed.
func (c *Conductor) imageInactive(epID int64, key imgKey) {
	for _, ep := range c.endpoints {
		if ep.id == epID {
			if p := ep.images[key]; p != nil && !p.draining && !p.closing {
				p.draining, p.drainFrom = true, c.now
				c.maybeFinish(p)
				c.dirty = true
			}
		}
	}
}

// --- periodic work ------------------------------------------------------------------

func minLinkPos(p *publication) int64 {
	min := p.links[0].pos.Get()
	for _, l := range p.links[1:] {
		if v := l.pos.Get(); v < min {
			min = v
		}
	}
	return min
}

// updateLimits advances each publication's limit to slowest-consumer + window, and
// tells each image's receiver how far its subscribers have read. It reports whether
// anything moved.
func (c *Conductor) updateLimits() bool {
	changed := false
	for p := range c.pubs {
		if p.closing {
			continue
		}
		switch p.kind {
		case kindIPC:
			if len(p.links) == 0 {
				continue
			}
			if lim := minLinkPos(p) + int64(c.cfg.PublicationWindow); lim != p.lastLimit {
				p.limit.Set(lim)
				p.lastLimit = lim
				changed = true
			}
		case kindNet:
			if !p.log.IsConnected() {
				continue
			}
			if lim := p.sndCons.Get() + int64(c.cfg.PublicationWindow); lim != p.lastLimit {
				p.limit.Set(lim)
				p.lastLimit = lim
				changed = true
			}
		case kindImage:
			if len(p.links) == 0 {
				continue
			}
			if v := minLinkPos(p); v != p.lastCons {
				p.imgCons.Set(v)
				p.lastCons = v
				changed = true
			}
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
		if p.closing {
			continue
		}
		if p.draining {
			c.maybeFinish(p)
			continue
		}
		if p.kind != kindImage {
			c.checkForHole(p)
		}
	}
	return work
}

// checkForHole unblocks a publication whose slowest reader has stood still for too
// long in front of unpublished space: a publisher that died mid-write. For an IPC
// publication the readers are its subscribers; for a network publication it is the sender.
func (c *Conductor) checkForHole(p *publication) {
	var consumer int64
	switch p.kind {
	case kindIPC:
		if len(p.links) == 0 {
			return
		}
		consumer = minLinkPos(p)
	case kindNet:
		consumer = p.sndPos.Get()
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
