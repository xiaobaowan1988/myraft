package stream

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// PacketConn is the part of *net.UDPConn the node uses; tests wrap it to inject
// loss, duplication and reordering.
type PacketConn interface {
	ReadFromUDP(b []byte) (int, *net.UDPAddr, error)
	WriteToUDP(b []byte, addr *net.UDPAddr) (int, error)
	Close() error
}

// Handler receives what arrives. OnMessage is called from a single goroutine; the
// messages of one stream arrive in order (different streams may interleave). It may
// block, which applies back-pressure to the sender of that stream.
// OnPeerDown is called when the streams to peer were reset because the peer went
// silent or restarted, or when the owner asked with ReportDown; messages that were
// queued for it are gone.
type Handler interface {
	OnMessage(from, stream int, msg []byte)
	OnPeerDown(peer int)
}

// OfferResult is the outcome of Offer, like Aeron's offer return codes.
type OfferResult int

const (
	Offered       OfferResult = iota
	BackPressured             // the stream's send buffer is full; try again later
	NotConnected              // the peer is considered down; the message was not queued
	TooBig                    // larger than MaxMessage
	Closed                    // the node is closed
	BadPeer                   // not a valid peer id, or not a valid stream number
)

func (r OfferResult) String() string {
	return [...]string{"Offered", "BackPressured", "NotConnected", "TooBig", "Closed", "BadPeer"}[r]
}

// Config configures a Node. Zero values get the defaults noted below.
type Config struct {
	ID      int      // this node's id, an index into Peers
	Peers   []string // UDP address of every node, indexed by id (Peers[ID] is where to listen)
	Key     []byte   // shared secret for packet authentication, at least 16 bytes
	Conn    PacketConn
	Streams int // independent streams per peer; default 1. Stream 0 is served first.

	MTU        int // max datagram size; default 1200
	MaxMessage int // largest message Offer accepts; default 1 MiB (must not exceed RecvWindow)
	SendBuffer int // unacknowledged + unsent bytes per stream before Offer back-pressures; default 8 MiB
	RecvWindow int // receive buffer per stream, the flow-control window; default 4 MiB
	InitWindow int // send window used until the peer's first STATUS; default 32 KiB

	Tick           time.Duration // timer loop period; default 5ms
	StatusInterval time.Duration // STATUS cadence while data flows; default 5ms
	Keepalive      time.Duration // STATUS cadence when idle (also the heartbeat); default 50ms
	NakInterval    time.Duration // minimum gap between NAKs for the same hole; default 5ms
	PeerTimeout    time.Duration // silence after which a peer is considered down; default 300ms
	ReportInterval time.Duration // min gap between OnPeerDown calls for a down peer; default 100ms

	InitCwnd, MinCwnd, MaxCwnd int // congestion window in frames, per stream; defaults 10, 2, 256
}

func (c *Config) defaults() {
	setInt := func(p *int, v int) {
		if *p == 0 {
			*p = v
		}
	}
	setDur := func(p *time.Duration, v time.Duration) {
		if *p == 0 {
			*p = v
		}
	}
	setInt(&c.Streams, 1)
	setInt(&c.MTU, 1200)
	setInt(&c.MaxMessage, 1<<20)
	setInt(&c.SendBuffer, 8<<20)
	setInt(&c.RecvWindow, 4<<20)
	setInt(&c.InitWindow, 32<<10)
	setDur(&c.Tick, 5*time.Millisecond)
	setDur(&c.StatusInterval, 5*time.Millisecond)
	setDur(&c.Keepalive, 50*time.Millisecond)
	setDur(&c.NakInterval, 5*time.Millisecond)
	setDur(&c.PeerTimeout, 300*time.Millisecond)
	setDur(&c.ReportInterval, 100*time.Millisecond)
	setInt(&c.InitCwnd, 10)
	setInt(&c.MinCwnd, 2)
	setInt(&c.MaxCwnd, 256)
}

// Stats is a snapshot of the node's counters.
type Stats struct {
	Offered, BackPressured, NotConnected uint64
	Delivered                            uint64 // messages handed to OnMessage
	FramesSent, Retransmits              uint64 // DATA frames written; of which retransmissions
	NaksSent, NaksReceived               uint64
	Resets                               uint64 // outgoing peers reset (silence, peer restart)
	Poisoned                             uint64 // incoming streams dropped for a protocol violation
	StaleStream                          uint64 // DATA addressed to an earlier instance of this process
	DroppedAuth, DroppedMalformed        uint64
}

const (
	burstFrames = 32 // frames written per peer per pass; pacing happens between passes
	minRTO      = 30 * time.Millisecond
	maxRTO      = 250 * time.Millisecond
	minPace     = 50 * time.Microsecond
	maxPace     = 2 * time.Millisecond
)

type frameRec struct {
	start, end int64
	sentAt     time.Time
	retrans    bool
}

type rng struct {
	pos int64
	n   int
}

// outStream is the sending side of one stream to one peer.
type outStream struct {
	session uint64

	// buf holds the stream bytes [base, base+len(buf)): acknowledged bytes are
	// trimmed, the rest is kept for retransmission or still waiting to be sent.
	buf      []byte
	base     int64
	sentPos  int64 // next position to transmit
	ackedPos int64 // peer's contiguous received position (== base)
	limit    int64 // right edge of the peer's window
	inflight []frameRec
	nak      []rng // ranges the peer asked for

	cwnd, ssthresh, caAcked int
	backoff                 int
	srtt                    time.Duration
	lastLoss                time.Time
}

// outPeer is everything we send to one peer: its streams plus peer-level state.
type outPeer struct {
	addr *net.UDPAddr
	ss   []*outStream

	up                                bool
	peerInc                           uint64
	lastHeard, lastStatus, lastReport time.Time
	needReport                        bool
}

type inStream struct {
	session   uint64
	rb        recvBuf
	announced int64 // how far the peer says it has transmitted
	poisoned  bool
	dirty     bool
	nakAt     map[int64]time.Time
}

type inPeer struct{ ss []*inStream }

type outPacket struct {
	b    []byte
	addr *net.UDPAddr
}

type handlerBox struct{ h Handler }

// Node is one participant: a UDP socket with reliable ordered streams in each
// direction to every peer.
type Node struct {
	cfg         Config
	conn        PacketConn
	incarnation uint64
	maxPayload  int

	mu  sync.Mutex
	out []*outPeer // indexed by node id; nil for self
	in  []*inPeer

	h    atomic.Pointer[handlerBox]
	wake chan struct{}
	done chan struct{}
	once sync.Once
	wg   sync.WaitGroup

	offered, backPressured, notConnected, delivered atomic.Uint64
	framesSent, retrans, naksSent, naksRecv         atomic.Uint64
	resets, poisoned, droppedAuth, droppedMalformed atomic.Uint64
	staleStream                                     atomic.Uint64
}

// New creates the node and starts its three goroutines. Until SetHandler is called,
// arriving messages are discarded.
func New(cfg Config) (*Node, error) {
	cfg.defaults()
	np := len(cfg.Peers)
	switch {
	case len(cfg.Key) < 16:
		return nil, errors.New("stream: Key must be at least 16 bytes")
	case np < 2 || np > 65535 || cfg.ID < 0 || cfg.ID >= np:
		return nil, fmt.Errorf("stream: bad ID %d for %d peers", cfg.ID, np)
	case cfg.Streams < 1 || cfg.Streams > 255:
		return nil, errors.New("stream: Streams must be between 1 and 255")
	case cfg.MTU < hdrLen+dataHdrLen+macLen+64:
		return nil, errors.New("stream: MTU too small")
	case cfg.MaxMessage > cfg.RecvWindow:
		return nil, errors.New("stream: MaxMessage must not exceed RecvWindow")
	}
	n := &Node{
		cfg:         cfg,
		incarnation: uint64(time.Now().UnixNano()),
		maxPayload:  cfg.MTU - hdrLen - dataHdrLen - macLen,
		out:         make([]*outPeer, np),
		in:          make([]*inPeer, np),
		wake:        make(chan struct{}, 1),
		done:        make(chan struct{}),
	}
	now := time.Now()
	for i := 0; i < np; i++ {
		if i == cfg.ID {
			continue
		}
		addr, err := net.ResolveUDPAddr("udp", cfg.Peers[i])
		if err != nil {
			return nil, fmt.Errorf("stream: peer %d address: %w", i, err)
		}
		p := &outPeer{addr: addr, up: true, lastHeard: now} // grace period: up until proven silent
		ip := &inPeer{}
		for s := 0; s < cfg.Streams; s++ {
			p.ss = append(p.ss, &outStream{})
			ip.ss = append(ip.ss, &inStream{nakAt: map[int64]time.Time{}})
		}
		n.resetOut(p, now)
		n.out[i], n.in[i] = p, ip
	}
	n.conn = cfg.Conn
	if n.conn == nil {
		laddr, err := net.ResolveUDPAddr("udp", cfg.Peers[cfg.ID])
		if err != nil {
			return nil, fmt.Errorf("stream: own address: %w", err)
		}
		c, err := net.ListenUDP("udp", laddr)
		if err != nil {
			return nil, err
		}
		c.SetReadBuffer(4 << 20) // best effort: absorb bursts (the OS may cap it)
		c.SetWriteBuffer(4 << 20)
		n.conn = c
	}
	n.wg.Add(3)
	go n.readLoop()
	go n.writeLoop()
	go n.timerLoop()
	return n, nil
}

// SetHandler installs the receiver of messages and peer-down events.
func (n *Node) SetHandler(h Handler) { n.h.Store(&handlerBox{h}) }

// Stats returns a snapshot of the counters.
func (n *Node) Stats() Stats {
	return Stats{
		Offered: n.offered.Load(), BackPressured: n.backPressured.Load(), NotConnected: n.notConnected.Load(),
		Delivered: n.delivered.Load(), FramesSent: n.framesSent.Load(), Retransmits: n.retrans.Load(),
		NaksSent: n.naksSent.Load(), NaksReceived: n.naksRecv.Load(),
		Resets: n.resets.Load(), Poisoned: n.poisoned.Load(), StaleStream: n.staleStream.Load(),
		DroppedAuth: n.droppedAuth.Load(), DroppedMalformed: n.droppedMalformed.Load(),
	}
}

// Close stops the node and waits for its goroutines.
func (n *Node) Close() error {
	n.once.Do(func() {
		close(n.done)
		n.conn.Close()
	})
	n.wg.Wait()
	return nil
}

func (n *Node) isDone() bool {
	select {
	case <-n.done:
		return true
	default:
		return false
	}
}

func (n *Node) wakeWriter() {
	select {
	case n.wake <- struct{}{}:
	default:
	}
}

// ReportDown asks for OnPeerDown(peer) to be called (from the timer goroutine, so
// the caller never blocks). Use it when a message to peer had to be dropped.
func (n *Node) ReportDown(peer int) {
	if peer < 0 || peer >= len(n.out) || n.out[peer] == nil {
		return
	}
	n.mu.Lock()
	n.out[peer].needReport = true
	n.mu.Unlock()
}

// resetOut starts fresh outgoing streams to the peer: new sessions, position 0.
// Anything queued or unacknowledged is dropped; the owner recovers it.
func (n *Node) resetOut(p *outPeer, now time.Time) {
	for _, s := range p.ss {
		s.session = max(s.session+1, uint64(now.UnixNano()))
		s.buf, s.base, s.sentPos, s.ackedPos = nil, 0, 0, 0
		s.limit = int64(n.cfg.InitWindow)
		s.inflight, s.nak = nil, nil
		s.cwnd, s.ssthresh, s.caAcked, s.backoff = n.cfg.InitCwnd, n.cfg.MaxCwnd, 0, 0
		s.srtt = 20 * time.Millisecond
		s.lastLoss = time.Time{}
	}
}

// ---- sending ----

// Offer appends msg to the given stream to peer. It never blocks and never touches
// the socket. It returns the position of the message within that stream.
func (n *Node) Offer(peer, stream int, msg []byte) (int64, OfferResult) {
	if peer < 0 || peer >= len(n.out) || n.out[peer] == nil || stream < 0 || stream >= n.cfg.Streams {
		return 0, BadPeer
	}
	if n.isDone() {
		return 0, Closed
	}
	if len(msg) > n.cfg.MaxMessage {
		return 0, TooBig
	}
	n.mu.Lock()
	p := n.out[peer]
	s := p.ss[stream]
	switch {
	case !p.up:
		n.mu.Unlock()
		n.notConnected.Add(1)
		return 0, NotConnected
	case len(s.buf)+4+len(msg) > n.cfg.SendBuffer:
		n.mu.Unlock()
		n.backPressured.Add(1)
		return 0, BackPressured
	}
	pos := s.base + int64(len(s.buf))
	s.buf = binary.BigEndian.AppendUint32(s.buf, uint32(len(msg)))
	s.buf = append(s.buf, msg...)
	n.mu.Unlock()
	n.offered.Add(1)
	n.wakeWriter()
	return pos, Offered
}

func (n *Node) dataPacket(peer int, p *outPeer, stream int, s *outStream, start, end int64) outPacket {
	body := make([]byte, dataHdrLen+int(end-start))
	body[0] = byte(stream)
	binary.BigEndian.PutUint64(body[1:], p.peerInc)
	binary.BigEndian.PutUint64(body[9:], uint64(start))
	copy(body[dataHdrLen:], s.buf[start-s.base:end-s.base])
	n.framesSent.Add(1)
	return outPacket{n.seal(typeData, peer, s.session, body), p.addr}
}

// collect gathers the frames that may be written now: requested retransmissions
// first, then new data as far as the congestion window and the peer's window
// allow. Streams are served in order, so stream 0 gets the frame budget first. At
// most burstFrames per peer; the returned duration is how long the writer should
// wait before asking again (pacing).
func (n *Node) collect() ([]outPacket, time.Duration) {
	var pkts []outPacket
	var pace time.Duration
	n.mu.Lock()
	defer n.mu.Unlock()
	for peer, p := range n.out {
		if p == nil {
			continue
		}
		frames := 0
		for si, s := range p.ss {
			sent := 0
			for len(s.nak) > 0 && frames < burstFrames {
				r := &s.nak[0]
				lo, hi := max(r.pos, s.ackedPos), min(r.pos+int64(r.n), s.sentPos)
				if hi <= lo {
					s.nak = s.nak[1:]
					continue
				}
				end := min(hi, lo+int64(n.maxPayload))
				pkts = append(pkts, n.dataPacket(peer, p, si, s, lo, end))
				n.retrans.Add(1)
				frames++
				sent++
				for i := range s.inflight { // no RTT sample from frames that were resent
					if s.inflight[i].end > lo && s.inflight[i].start < end {
						s.inflight[i].retrans = true
					}
				}
				if end >= hi {
					s.nak = s.nak[1:]
				} else {
					r.pos, r.n = end, int(hi-end)
				}
			}
			qEnd := s.base + int64(len(s.buf))
			now := time.Now()
			// Nothing is sent until we know which process instance the peer is: the
			// stream is created for that instance (see rcvInc).
			for p.peerInc != 0 && frames < burstFrames && len(s.inflight) < s.cwnd && s.sentPos < qEnd && s.sentPos < s.limit {
				end := min(s.sentPos+int64(n.maxPayload), qEnd, s.limit)
				pkts = append(pkts, n.dataPacket(peer, p, si, s, s.sentPos, end))
				s.inflight = append(s.inflight, frameRec{start: s.sentPos, end: end, sentAt: now})
				s.sentPos = end
				frames++
				sent++
			}
			if sent > 0 {
				pace = max(pace, s.srtt*time.Duration(sent)/time.Duration(max(s.cwnd, 1)))
			}
		}
	}
	return pkts, min(max(pace, minPace), maxPace)
}

func (n *Node) write(pkts []outPacket) {
	for _, p := range pkts {
		n.conn.WriteToUDP(p.b, p.addr)
	}
}

func (n *Node) writeLoop() {
	defer n.wg.Done()
	for {
		select {
		case <-n.done:
			return
		case <-n.wake:
		}
		for {
			pkts, pace := n.collect()
			if len(pkts) == 0 {
				break
			}
			n.write(pkts)
			time.Sleep(pace) // pacing: spread the window over the RTT instead of bursting
		}
	}
}

// ---- receiving ----

func (n *Node) readLoop() {
	defer n.wg.Done()
	buf := make([]byte, 64<<10)
	for {
		sz, _, err := n.conn.ReadFromUDP(buf)
		if err != nil {
			if n.isDone() || errors.Is(err, net.ErrClosed) {
				return
			}
			time.Sleep(time.Millisecond)
			continue
		}
		n.handlePacket(buf[:sz])
	}
}

func (n *Node) handlePacket(pkt []byte) {
	h, body, ok := n.open(pkt)
	if !ok {
		return
	}
	if h.to != n.cfg.ID || h.from < 0 || h.from >= len(n.in) || n.in[h.from] == nil {
		n.droppedMalformed.Add(1)
		return
	}
	now := time.Now()
	switch h.typ {
	case typeData:
		n.onData(h, body, now)
	case typeStatus:
		n.onStatus(h, body, now)
	case typeNak:
		n.onNak(h, body, now)
	default:
		n.droppedMalformed.Add(1)
	}
}

// heard records that the peer is alive. Caller holds mu.
func (n *Node) heard(p *outPeer, now time.Time) {
	p.lastHeard = now
	p.up = true
}

// acceptSession reports whether packets of the peer's session are processed. A
// newer session means the peer started a new stream: the old one is abandoned.
func (n *Node) acceptSession(in *inStream, session uint64) bool {
	switch {
	case session == in.session:
		return true
	case session > in.session:
		in.session = session
		in.rb = recvBuf{}
		in.announced, in.poisoned, in.dirty = 0, false, false
		in.nakAt = map[int64]time.Time{}
		return true
	}
	return false // an older stream: stale or replayed
}

// parse extracts the complete messages at the front of the contiguous data. It
// copies them out, and returns where each ends. bad means the stream violates the
// framing (a length beyond MaxMessage).
func (n *Node) parse(in *inStream) (msgs [][]byte, ends []int64, bad bool) {
	data, off := in.rb.data, 0
	for len(data)-off >= 4 {
		l := int(binary.BigEndian.Uint32(data[off:]))
		if l > n.cfg.MaxMessage {
			return nil, nil, true
		}
		if len(data)-off-4 < l {
			break
		}
		msgs = append(msgs, append([]byte(nil), data[off+4:off+4+l]...))
		off += 4 + l
		ends = append(ends, in.rb.base+int64(off))
	}
	return msgs, ends, false
}

func (n *Node) onData(h header, body []byte, now time.Time) {
	if len(body) <= dataHdrLen {
		n.droppedMalformed.Add(1)
		return
	}
	st := int(body[0])
	rcvInc := binary.BigEndian.Uint64(body[1:])
	pos := int64(binary.BigEndian.Uint64(body[9:]))
	payload := body[dataHdrLen:]
	if st >= n.cfg.Streams || pos < 0 {
		n.droppedMalformed.Add(1)
		return
	}
	if rcvInc != n.incarnation {
		n.staleStream.Add(1) // meant for an earlier life of this process
		return
	}
	n.mu.Lock()
	n.heard(n.out[h.from], now)
	in := n.in[h.from].ss[st]
	if !n.acceptSession(in, h.session) || in.poisoned {
		n.mu.Unlock()
		return
	}
	limit := in.rb.base + int64(n.cfg.RecvWindow)
	before := in.rb.contig
	in.rb.insert(pos, payload, limit)
	if in.rb.contig != before {
		in.dirty = true
	}
	msgs, ends, bad := n.parse(in)
	if bad {
		in.poisoned = true
		in.rb = recvBuf{}
		n.poisoned.Add(1)
		n.mu.Unlock()
		return
	}
	session := in.session
	n.mu.Unlock()
	if len(msgs) == 0 {
		return
	}

	// Deliver outside the lock: the handler may block, and that is how a slow
	// consumer pushes back (consumed, hence the window, only advances afterwards).
	if box := n.h.Load(); box != nil {
		for _, m := range msgs {
			box.h.OnMessage(h.from, st, m)
			n.delivered.Add(1)
		}
	}
	n.mu.Lock()
	if in.session == session && !in.poisoned {
		in.rb.consume(int(ends[len(ends)-1] - in.rb.base))
		in.dirty = true
	}
	n.mu.Unlock()
}

func (n *Node) onStatus(h header, body []byte, now time.Time) {
	if len(body) < statusHdrLen || (len(body)-statusHdrLen)%statusEntryLen != 0 ||
		int(body[8]) != (len(body)-statusHdrLen)/statusEntryLen {
		n.droppedMalformed.Add(1)
		return
	}
	inc := binary.BigEndian.Uint64(body)
	count := int(body[8])
	n.mu.Lock()
	defer n.mu.Unlock()
	p, ip := n.out[h.from], n.in[h.from]
	n.heard(p, now)
	// A new process instance at the peer reads our streams from position 0: start new
	// streams rather than continuing ones it knows nothing about.
	if p.peerInc != 0 && inc != p.peerInc {
		n.resetOut(p, now)
		n.resets.Add(1)
	}
	if p.peerInc != inc {
		p.peerInc = inc
		n.wakeWriter() // data may have been waiting for this instance id
	}
	for i := 0; i < count; i++ {
		e := body[statusHdrLen+i*statusEntryLen:]
		st := int(e[0])
		if st >= n.cfg.Streams {
			continue // the peer has more streams than we do
		}
		session := binary.BigEndian.Uint64(e[1:])
		sentPos := int64(binary.BigEndian.Uint64(e[9:]))
		rcvSession := binary.BigEndian.Uint64(e[17:])
		rcvContig := int64(binary.BigEndian.Uint64(e[25:]))
		rcvLimit := int64(binary.BigEndian.Uint64(e[33:]))
		// Receiving side: how far the peer says it has sent, so a lost tail shows up.
		in := ip.ss[st]
		if n.acceptSession(in, session) && sentPos > in.announced {
			in.announced = sentPos
		}
		// Sending side: the peer acknowledges our stream (only if it means this session).
		if s := p.ss[st]; rcvSession == s.session && rcvContig >= 0 && rcvContig <= s.sentPos {
			n.processAck(s, rcvContig, rcvLimit, now)
		}
	}
}

// processAck frees acknowledged bytes, measures RTT and grows the window.
func (n *Node) processAck(s *outStream, contig, limit int64, now time.Time) {
	if limit > s.limit {
		s.limit = limit
	}
	if contig > s.ackedPos {
		popped := 0
		var sample time.Duration
		have := false
		for len(s.inflight) > 0 && s.inflight[0].end <= contig {
			f := s.inflight[0]
			s.inflight = s.inflight[1:]
			popped++
			if !f.retrans {
				sample, have = now.Sub(f.sentAt), true
			}
		}
		if len(s.inflight) == 0 {
			s.inflight = nil
		}
		drop := int(contig - s.base)
		s.buf = s.buf[drop:]
		if len(s.buf) == 0 {
			s.buf = nil
		} else if cap(s.buf) > 4*len(s.buf)+(64<<10) {
			s.buf = append([]byte(nil), s.buf...)
		}
		s.base, s.ackedPos = contig, contig
		s.backoff = 0
		if have {
			s.srtt = (7*s.srtt + sample) / 8
		}
		if popped > 0 {
			if s.cwnd < s.ssthresh {
				s.cwnd += popped // slow start
			} else {
				s.caAcked += popped // congestion avoidance: +1 per window acknowledged
				for s.caAcked >= s.cwnd {
					s.caAcked -= s.cwnd
					s.cwnd++
				}
			}
			s.cwnd = min(s.cwnd, n.cfg.MaxCwnd)
		}
	}
	n.wakeWriter() // the window may have opened
}

// onLoss reacts to a loss signal: at most one window reduction per RTT.
func (n *Node) onLoss(s *outStream, now time.Time, timeout bool) {
	if !timeout && now.Sub(s.lastLoss) < s.srtt {
		return
	}
	s.lastLoss = now
	s.ssthresh = max(s.cwnd/2, n.cfg.MinCwnd)
	if timeout {
		s.cwnd = n.cfg.MinCwnd
	} else {
		s.cwnd = s.ssthresh
	}
}

func (n *Node) onNak(h header, body []byte, now time.Time) {
	if len(body) < nakHdrLen {
		n.droppedMalformed.Add(1)
		return
	}
	st := int(body[0])
	session := binary.BigEndian.Uint64(body[1:])
	count := int(body[9])
	if st >= n.cfg.Streams || count > maxNakRanges || len(body) != nakHdrLen+nakEntryLen*count {
		n.droppedMalformed.Add(1)
		return
	}
	n.mu.Lock()
	p := n.out[h.from]
	n.heard(p, now)
	if s := p.ss[st]; session == s.session {
		n.naksRecv.Add(1)
		for i := 0; i < count && len(s.nak) < 4096; i++ {
			e := body[nakHdrLen+nakEntryLen*i:]
			pos := int64(binary.BigEndian.Uint64(e))
			l := int(binary.BigEndian.Uint32(e[8:]))
			if pos >= 0 && l > 0 {
				s.nak = append(s.nak, rng{pos, l})
			}
		}
		n.onLoss(s, now, false)
	}
	n.mu.Unlock()
	n.wakeWriter()
}

// ---- timers: NAKs, retransmission timeout, STATUS, liveness ----

func (n *Node) timerLoop() {
	defer n.wg.Done()
	tk := time.NewTicker(n.cfg.Tick)
	defer tk.Stop()
	for {
		select {
		case <-n.done:
			return
		case <-tk.C:
			n.tick()
		}
	}
}

func (n *Node) rto(s *outStream) time.Duration {
	d := min(max(2*s.srtt+10*time.Millisecond, minRTO), maxRTO)
	return d << min(s.backoff, 3)
}

func (n *Node) statusPacket(peer int, p *outPeer, ip *inPeer) outPacket {
	body := make([]byte, statusHdrLen+statusEntryLen*len(p.ss))
	binary.BigEndian.PutUint64(body, n.incarnation)
	body[8] = byte(len(p.ss))
	for i, s := range p.ss {
		in := ip.ss[i]
		e := body[statusHdrLen+i*statusEntryLen:]
		e[0] = byte(i)
		binary.BigEndian.PutUint64(e[1:], s.session)
		binary.BigEndian.PutUint64(e[9:], uint64(s.sentPos))
		binary.BigEndian.PutUint64(e[17:], in.session)
		binary.BigEndian.PutUint64(e[25:], uint64(in.rb.contig))
		binary.BigEndian.PutUint64(e[33:], uint64(in.rb.base+int64(n.cfg.RecvWindow)))
	}
	return outPacket{n.seal(typeStatus, peer, 0, body), p.addr}
}

// holes lists the byte ranges of the peer's stream that are missing and due another
// NAK, each at most once per max(NakInterval, 1.5*srtt).
func (n *Node) holes(s *outStream, in *inStream, now time.Time) []rng {
	gap := max(n.cfg.NakInterval, s.srtt*3/2)
	var out []rng
	add := func(start, end int64) {
		if end <= start || len(out) >= maxNakRanges {
			return
		}
		if t, ok := in.nakAt[start]; ok && now.Sub(t) < gap {
			return
		}
		in.nakAt[start] = now
		out = append(out, rng{start, int(min(end-start, math.MaxInt32))})
	}
	cur := in.rb.contig
	for _, sg := range in.rb.ooo {
		add(cur, sg.pos)
		cur = max(cur, sg.end())
	}
	add(cur, in.announced) // the tail the peer says exists but we have not seen
	for start := range in.nakAt {
		if start < in.rb.contig {
			delete(in.nakAt, start)
		}
	}
	return out
}

func (n *Node) nakPacket(peer int, p *outPeer, stream int, in *inStream, rs []rng) outPacket {
	body := make([]byte, nakHdrLen, nakHdrLen+nakEntryLen*len(rs))
	body[0] = byte(stream)
	binary.BigEndian.PutUint64(body[1:], in.session)
	body[9] = byte(len(rs))
	for _, r := range rs {
		body = binary.BigEndian.AppendUint64(body, uint64(r.pos))
		body = binary.BigEndian.AppendUint32(body, uint32(r.n))
	}
	return outPacket{n.seal(typeNak, peer, 0, body), p.addr}
}

func (n *Node) tick() {
	now := time.Now()
	var pkts []outPacket
	var reports []int
	retx := false

	n.mu.Lock()
	for peer, p := range n.out {
		if p == nil {
			continue
		}
		ip := n.in[peer]

		// Liveness: a peer silent for PeerTimeout is down; its streams are reset once.
		silent := now.Sub(p.lastHeard) > n.cfg.PeerTimeout
		if silent && p.up {
			p.up = false
			n.resetOut(p, now)
			n.resets.Add(1)
			p.lastReport = now
			reports = append(reports, peer)
			p.needReport = false
		} else if (silent || p.needReport) && now.Sub(p.lastReport) >= n.cfg.ReportInterval {
			p.lastReport = now
			p.needReport = false
			reports = append(reports, peer)
		}

		dirty := false
		for si, s := range p.ss {
			in := ip.ss[si]
			// Receiver: ask for what is missing.
			if !in.poisoned && (len(in.rb.ooo) > 0 || in.announced > in.rb.contig) {
				if rs := n.holes(s, in, now); len(rs) > 0 {
					n.naksSent.Add(1)
					pkts = append(pkts, n.nakPacket(peer, p, si, in, rs))
				}
			}
			// Sender: retransmission timeout on the oldest unacknowledged frame.
			if len(s.inflight) > 0 && now.Sub(s.inflight[0].sentAt) > n.rto(s) {
				s.nak = append(s.nak, rng{s.ackedPos, n.maxPayload})
				s.inflight[0].sentAt, s.inflight[0].retrans = now, true
				s.backoff++
				n.onLoss(s, now, true)
				retx = true
			}
			if len(s.nak) > 0 {
				retx = true
			}
			dirty = dirty || in.dirty
		}

		// STATUS: acknowledgement, window, what I have sent, and the heartbeat.
		if now.Sub(p.lastStatus) >= n.cfg.Keepalive || (dirty && now.Sub(p.lastStatus) >= n.cfg.StatusInterval) {
			pkts = append(pkts, n.statusPacket(peer, p, ip))
			p.lastStatus = now
			for _, in := range ip.ss {
				in.dirty = false
			}
		}
	}
	n.mu.Unlock()

	n.write(pkts)
	if retx {
		n.wakeWriter()
	}
	if box := n.h.Load(); box != nil {
		for _, peer := range reports {
			box.h.OnPeerDown(peer)
		}
	}
}
