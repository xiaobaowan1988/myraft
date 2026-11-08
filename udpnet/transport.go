package udpnet

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xiaobaowan1988/myraft/raft"
)

// PacketConn is the part of *net.UDPConn the transport uses; tests wrap it to
// inject loss, duplication and reordering.
type PacketConn interface {
	ReadFromUDP(b []byte) (int, *net.UDPAddr, error)
	WriteToUDP(b []byte, addr *net.UDPAddr) (int, error)
	Close() error
}

// Config configures a Transport. Zero values get the defaults noted below.
type Config struct {
	ID    int      // this node's id, an index into Peers
	Peers []string // UDP address of every node, indexed by id (Peers[ID] is where to listen)
	Key   []byte   // shared secret for packet authentication, at least 16 bytes
	Conn  PacketConn
	Codec CommandCodec // default DefaultCodec

	MTU        int // max datagram size; default 1200
	MaxMessage int // largest message accepted by Send; default 64 KiB. It bounds the worst burst; bigger commands cannot be replicated
	SendQueue  int // messages queued per peer before Send drops; default 4096
	RecvBuffer int // reassembly/reorder bytes per peer, also the advertised window; default 4 MiB
	RecvAhead  int // how far past the delivered sequence a packet is accepted; default 4096

	Tick           time.Duration // timer loop period; default 5ms
	StatusInterval time.Duration // STATUS cadence while data flows; default 5ms
	Keepalive      time.Duration // STATUS cadence when idle; default 50ms
	NakInterval    time.Duration // between NAKs for the same message; default 5ms
	MaxNaks        int           // NAKs per missing message before waiting for GapTimeout; default 8
	GapTimeout     time.Duration // give up on a missing message after this long WITHOUT PROGRESS; default 30ms (keep < raft.AppendTimeout)
	MaxRetrans     int           // retransmissions per message before giving up; default 8
	PeerTimeout    time.Duration // silence after which a peer is reported unreachable; default 300ms
	ReportInterval time.Duration // min gap between unreachable reports for a peer; default 100ms

	InitCwnd, MinCwnd, MaxCwnd int // congestion window in PACKETS (datagrams); defaults 10, 2, 256
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
	if c.Codec == nil {
		c.Codec = DefaultCodec{}
	}
	setInt(&c.MTU, 1200)
	setInt(&c.MaxMessage, 64<<10)
	setInt(&c.SendQueue, 4096)
	setInt(&c.RecvBuffer, 4<<20)
	setInt(&c.RecvAhead, 4096)
	setDur(&c.Tick, 5*time.Millisecond)
	setDur(&c.StatusInterval, 5*time.Millisecond)
	setDur(&c.Keepalive, 50*time.Millisecond)
	setDur(&c.NakInterval, 5*time.Millisecond)
	setInt(&c.MaxNaks, 8)
	setDur(&c.GapTimeout, 30*time.Millisecond)
	setInt(&c.MaxRetrans, 8)
	setDur(&c.PeerTimeout, 300*time.Millisecond)
	setDur(&c.ReportInterval, 100*time.Millisecond)
	setInt(&c.InitCwnd, 10)
	setInt(&c.MinCwnd, 2)
	setInt(&c.MaxCwnd, 256)
}

// Stats is a snapshot of the transport's counters.
type Stats struct {
	PacketsSent, Retransmits      uint64 // DATA datagrams written; of which retransmissions
	NaksSent, NaksReceived        uint64
	Delivered                     uint64 // messages handed to the endpoint
	Skipped                       uint64 // missing messages given up on (the loss is left to Raft)
	GaveUp                        uint64 // sent messages abandoned after MaxRetrans
	DroppedQueue                  uint64 // Send refused: queue full
	DroppedEncode                 uint64 // Send refused: could not encode, or too large
	DroppedAuth, DroppedMalformed uint64
	Unreachable                   uint64 // ReportUnreachable calls made
}

const (
	maxBurst = 256 // datagrams per write batch, bounding how long the lock is held
	retxGap  = 2 * time.Millisecond
	minRTO   = 30 * time.Millisecond
	maxRTO   = 250 * time.Millisecond
)

type outMsg struct {
	seq     uint64
	frags   [][]byte
	size    int       // payload bytes
	sentAt  time.Time // first transmission
	lastTx  time.Time
	retrans int
}

// outPeer is the sending side of the stream to one peer.
type outPeer struct {
	addr          *net.UDPAddr
	nextSeq       uint64    // last sequence number assigned
	highestTx     uint64    // highest sequence number transmitted
	ackedSeq      uint64    // peer's cumulative delivered/skipped sequence
	queue         []*outMsg // accepted, waiting for window
	inflight      []*outMsg // transmitted and unacked; consecutive sequence numbers
	inflightBytes int
	inflightPkts  int      // datagrams in flight: what the congestion window limits
	retx          []uint64 // retransmissions requested (NAK, RTO)

	cwnd, ssthresh int // congestion window and slow-start threshold, in packets
	caAcked        int // packets acknowledged since the window last grew (congestion avoidance)
	peerWindow     int // receiver's advertised buffer space, in bytes
	srtt           time.Duration
	lastLoss       time.Time

	lastHeard, lastStatus, lastReport time.Time
	needReport                        bool
}

func (p *outPeer) find(seq uint64) *outMsg {
	if len(p.inflight) == 0 {
		return nil
	}
	first := p.inflight[0].seq
	if seq < first || seq >= first+uint64(len(p.inflight)) {
		return nil
	}
	return p.inflight[seq-first]
}

type reasm struct {
	cnt, got, size int
	frags          [][]byte
}

func (r *reasm) join() []byte {
	b := make([]byte, 0, r.size)
	for _, f := range r.frags {
		b = append(b, f...)
	}
	return b
}

type nakState struct {
	tries int
	last  time.Time
}

// inPeer is the receiving side of the stream from one peer.
type inPeer struct {
	session   uint64 // the sender incarnation we accept; 0 = none yet
	delivered uint64 // highest sequence delivered or given up on
	highest   uint64 // highest sequence known to exist (seen in DATA or announced in STATUS)
	pending   map[uint64]*reasm
	bufBytes  int
	gapSince  time.Time
	nak       map[uint64]*nakState
	dirty     bool // delivered advanced since the last STATUS
}

type outPacket struct {
	b    []byte
	addr *net.UDPAddr
}

type epBox struct{ ep raft.Endpoint }

// Transport implements raft.Transport over UDP.
type Transport struct {
	cfg        Config
	conn       PacketConn
	session    uint64
	maxPayload int
	maxFrags   int

	// dmu serialises "take the next deliverable messages" and "hand them to the
	// endpoint": both the reader and the timer deliver, and the order of delivery
	// must be the order of the sequence numbers. Lock order: dmu, then mu.
	dmu sync.Mutex

	mu  sync.Mutex
	out []*outPeer // indexed by node id; nil for self
	in  []*inPeer

	ep   atomic.Pointer[epBox]
	wake chan struct{}
	done chan struct{}
	once sync.Once
	wg   sync.WaitGroup

	packetsSent, retrans, naksSent, naksRecv, delivered, skipped, gaveUp atomic.Uint64
	droppedQueue, droppedEncode, droppedAuth, droppedMalformed, unreach  atomic.Uint64
}

var _ raft.Transport = (*Transport)(nil)

// New creates the transport and starts its three goroutines. Messages that arrive
// before SetEndpoint are dropped.
func New(cfg Config) (*Transport, error) {
	cfg.defaults()
	n := len(cfg.Peers)
	switch {
	case len(cfg.Key) < 16:
		return nil, errors.New("udpnet: Key must be at least 16 bytes")
	case n < 2 || n > 65535 || cfg.ID < 0 || cfg.ID >= n:
		return nil, fmt.Errorf("udpnet: bad ID %d for %d peers", cfg.ID, n)
	case cfg.MTU < hdrLen+dataHdrLen+macLen+64:
		return nil, errors.New("udpnet: MTU too small")
	}
	t := &Transport{
		cfg:        cfg,
		session:    uint64(time.Now().UnixNano()),
		maxPayload: cfg.MTU - hdrLen - dataHdrLen - macLen,
		out:        make([]*outPeer, n),
		in:         make([]*inPeer, n),
		wake:       make(chan struct{}, 1),
		done:       make(chan struct{}),
	}
	t.maxFrags = cfg.MaxMessage/t.maxPayload + 2
	now := time.Now()
	for i := 0; i < n; i++ {
		if i == cfg.ID {
			continue
		}
		addr, err := net.ResolveUDPAddr("udp", cfg.Peers[i])
		if err != nil {
			return nil, fmt.Errorf("udpnet: peer %d address: %w", i, err)
		}
		t.out[i] = &outPeer{addr: addr, cwnd: cfg.InitCwnd, ssthresh: cfg.MaxCwnd, peerWindow: cfg.RecvBuffer,
			srtt: 20 * time.Millisecond, lastHeard: now}
		t.in[i] = &inPeer{pending: map[uint64]*reasm{}, nak: map[uint64]*nakState{}}
	}
	t.conn = cfg.Conn
	if t.conn == nil {
		laddr, err := net.ResolveUDPAddr("udp", cfg.Peers[cfg.ID])
		if err != nil {
			return nil, fmt.Errorf("udpnet: own address: %w", err)
		}
		c, err := net.ListenUDP("udp", laddr)
		if err != nil {
			return nil, err
		}
		// Best effort: a larger kernel buffer absorbs bursts (the OS may cap it).
		c.SetReadBuffer(4 << 20)
		c.SetWriteBuffer(4 << 20)
		t.conn = c
	}
	t.wg.Add(3)
	go t.readLoop()
	go t.writeLoop()
	go t.timerLoop()
	return t, nil
}

// SetEndpoint connects the transport to the Raft node that receives its messages.
func (t *Transport) SetEndpoint(ep raft.Endpoint) { t.ep.Store(&epBox{ep}) }

// Stats returns a snapshot of the counters.
func (t *Transport) Stats() Stats {
	return Stats{
		PacketsSent: t.packetsSent.Load(), Retransmits: t.retrans.Load(),
		NaksSent: t.naksSent.Load(), NaksReceived: t.naksRecv.Load(),
		Delivered: t.delivered.Load(), Skipped: t.skipped.Load(), GaveUp: t.gaveUp.Load(),
		DroppedQueue: t.droppedQueue.Load(), DroppedEncode: t.droppedEncode.Load(),
		DroppedAuth: t.droppedAuth.Load(), DroppedMalformed: t.droppedMalformed.Load(),
		Unreachable: t.unreach.Load(),
	}
}

// Close stops the transport and waits for its goroutines.
func (t *Transport) Close() error {
	t.once.Do(func() {
		close(t.done)
		t.conn.Close()
	})
	t.wg.Wait()
	return nil
}

func (t *Transport) isDone() bool {
	select {
	case <-t.done:
		return true
	default:
		return false
	}
}

func (t *Transport) wakeWriter() {
	select {
	case t.wake <- struct{}{}:
	default:
	}
}

// ---- sending ----

// Send queues m for delivery to m.To. It never blocks and never touches the
// socket: it encodes, fragments and enqueues, and the write loop does the rest.
// A full queue drops the message and reports the peer unreachable.
func (t *Transport) Send(m raft.Message) {
	to := m.To
	if to < 0 || to >= len(t.out) || t.out[to] == nil || t.isDone() {
		return
	}
	body, err := encodeMessage(m, t.cfg.Codec)
	if err != nil || len(body) > t.cfg.MaxMessage {
		t.droppedEncode.Add(1)
		return
	}
	var frags [][]byte
	for off := 0; off < len(body); off += t.maxPayload {
		frags = append(frags, body[off:min(off+t.maxPayload, len(body))])
	}
	t.mu.Lock()
	p := t.out[to]
	if len(p.queue) >= t.cfg.SendQueue {
		p.needReport = true
		t.mu.Unlock()
		t.droppedQueue.Add(1)
		return
	}
	p.nextSeq++
	p.queue = append(p.queue, &outMsg{seq: p.nextSeq, frags: frags, size: len(body)})
	t.mu.Unlock()
	t.wakeWriter()
}

func (t *Transport) dataPackets(peer int, p *outPeer, m *outMsg) []outPacket {
	pkts := make([]outPacket, 0, len(m.frags))
	for i, f := range m.frags {
		body := make([]byte, dataHdrLen+len(f))
		binary.BigEndian.PutUint64(body, m.seq)
		binary.BigEndian.PutUint16(body[8:], uint16(i))
		binary.BigEndian.PutUint16(body[10:], uint16(len(m.frags)))
		copy(body[dataHdrLen:], f)
		pkts = append(pkts, outPacket{t.seal(typeData, peer, body), p.addr})
	}
	return pkts
}

// collect gathers the datagrams that may be written now: requested
// retransmissions first, then new messages as far as the windows allow.
func (t *Transport) collect() []outPacket {
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	var pkts []outPacket
	for peer, p := range t.out {
		if p == nil {
			continue
		}
		for _, seq := range p.retx {
			m := p.find(seq)
			if m == nil || now.Sub(m.lastTx) < retxGap {
				continue
			}
			m.lastTx = now
			m.retrans++
			t.retrans.Add(1)
			pkts = append(pkts, t.dataPackets(peer, p, m)...)
		}
		p.retx = p.retx[:0]
		for len(p.queue) > 0 && len(pkts) < maxBurst {
			m := p.queue[0]
			// Both windows must have room, except that one message may always be in
			// flight so an oversized one cannot stall.
			if p.inflightPkts > 0 && (p.inflightPkts+len(m.frags) > p.cwnd || p.inflightBytes+m.size > p.peerWindow) {
				break
			}
			p.queue[0] = nil
			p.queue = p.queue[1:]
			if len(p.queue) == 0 {
				p.queue = nil
			}
			m.sentAt, m.lastTx = now, now
			p.inflight = append(p.inflight, m)
			p.inflightBytes += m.size
			p.inflightPkts += len(m.frags)
			p.highestTx = m.seq
			pkts = append(pkts, t.dataPackets(peer, p, m)...)
		}
	}
	return pkts
}

func (t *Transport) write(pkts []outPacket) {
	for _, p := range pkts {
		if _, err := t.conn.WriteToUDP(p.b, p.addr); err == nil {
			t.packetsSent.Add(1)
		}
	}
}

func (t *Transport) writeLoop() {
	defer t.wg.Done()
	for {
		select {
		case <-t.done:
			return
		case <-t.wake:
		}
		for {
			pkts := t.collect()
			if len(pkts) == 0 {
				break
			}
			t.write(pkts)
		}
	}
}

// ---- receiving ----

func (t *Transport) readLoop() {
	defer t.wg.Done()
	buf := make([]byte, 64<<10)
	for {
		n, _, err := t.conn.ReadFromUDP(buf)
		if err != nil {
			if t.isDone() || errors.Is(err, net.ErrClosed) {
				return
			}
			time.Sleep(time.Millisecond)
			continue
		}
		t.handlePacket(buf[:n])
	}
}

func (t *Transport) handlePacket(pkt []byte) {
	h, body, ok := t.open(pkt)
	if !ok {
		return
	}
	if h.to != t.cfg.ID || h.from < 0 || h.from >= len(t.in) || t.in[h.from] == nil {
		t.droppedMalformed.Add(1)
		return
	}
	now := time.Now()
	switch h.typ {
	case typeData:
		t.dmu.Lock()
		t.deliver(h.from, t.onData(h, body, now))
		t.dmu.Unlock()
	case typeStatus:
		t.onStatus(h, body, now)
	case typeNak:
		t.onNak(h, body, now)
	default:
		t.droppedMalformed.Add(1)
	}
}

// acceptSession reports whether packets of the sender's session should be
// processed. A newer session means the sender restarted: its stream starts over.
func (t *Transport) acceptSession(in *inPeer, session uint64) bool {
	switch {
	case session == in.session:
		return true
	case session > in.session:
		in.session = session
		in.delivered, in.highest, in.bufBytes = 0, 0, 0
		in.pending = map[uint64]*reasm{}
		in.nak = map[uint64]*nakState{}
		in.gapSince = time.Time{}
		in.dirty = false
		return true
	}
	return false // an older incarnation: stale or replayed
}

// drain takes every message that is now complete and next in sequence.
func (t *Transport) drain(in *inPeer) [][]byte {
	var out [][]byte
	for {
		r := in.pending[in.delivered+1]
		if r == nil || r.got < r.cnt {
			break
		}
		delete(in.pending, in.delivered+1)
		in.bufBytes -= r.size
		in.delivered++
		in.dirty = true
		delete(in.nak, in.delivered)
		out = append(out, r.join())
	}
	if in.highest <= in.delivered {
		in.gapSince = time.Time{}
	}
	return out
}

// onData stores a fragment and returns the messages that became deliverable.
func (t *Transport) onData(h header, body []byte, now time.Time) [][]byte {
	if len(body) < dataHdrLen {
		t.droppedMalformed.Add(1)
		return nil
	}
	seq := binary.BigEndian.Uint64(body)
	idx := int(binary.BigEndian.Uint16(body[8:]))
	cnt := int(binary.BigEndian.Uint16(body[10:]))
	payload := body[dataHdrLen:]
	if seq == 0 || cnt == 0 || idx >= cnt || cnt > t.maxFrags {
		t.droppedMalformed.Add(1)
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.out[h.from].lastHeard = now
	in := t.in[h.from]
	if !t.acceptSession(in, h.session) {
		return nil
	}
	if seq <= in.delivered {
		in.dirty = true // a duplicate usually means our ack was lost: say it again
		return nil
	}
	if seq > in.delivered+uint64(t.cfg.RecvAhead) {
		return nil
	}
	r := in.pending[seq]
	if r == nil {
		r = &reasm{cnt: cnt, frags: make([][]byte, cnt)}
		in.pending[seq] = r
	} else if r.cnt != cnt {
		t.droppedMalformed.Add(1)
		return nil
	}
	if r.frags[idx] != nil {
		return nil // duplicate fragment
	}
	if in.bufBytes+len(payload) > t.cfg.RecvBuffer {
		return nil // out of buffer space: drop; the sender will back off or retransmit
	}
	r.frags[idx] = append([]byte(nil), payload...)
	r.got++
	r.size += len(payload)
	in.bufBytes += len(payload)
	// A fragment of something we already knew was missing or incomplete is
	// progress for the gap timer (retransmission is working); a brand-new, later
	// message is not, or steady traffic would postpone giving up forever.
	repair := seq <= in.highest
	if seq > in.highest {
		in.highest = seq
	}
	bodies := t.drain(in)
	if in.highest > in.delivered {
		if in.gapSince.IsZero() || repair || len(bodies) > 0 {
			in.gapSince = now
		}
	}
	return bodies
}

// deliver decodes messages in order and hands them to the endpoint. It runs on the
// single reader goroutine without the lock, because Step may block briefly.
func (t *Transport) deliver(from int, bodies [][]byte) {
	for _, b := range bodies {
		m, err := decodeMessage(b, t.cfg.Codec)
		if err != nil || m.From != from || m.To != t.cfg.ID {
			// Undecodable, or the message claims another sender than the
			// authenticated packet header: refuse it.
			t.droppedMalformed.Add(1)
			continue
		}
		if box := t.ep.Load(); box != nil {
			box.ep.Step(m)
		}
		t.delivered.Add(1)
	}
}

func (t *Transport) onStatus(h header, body []byte, now time.Time) {
	if len(body) != statusLen {
		t.droppedMalformed.Add(1)
		return
	}
	highestSent := binary.BigEndian.Uint64(body)
	rcvSession := binary.BigEndian.Uint64(body[8:])
	rcvDelivered := binary.BigEndian.Uint64(body[16:])
	rcvWindow := int(binary.BigEndian.Uint32(body[24:]))
	t.mu.Lock()
	defer t.mu.Unlock()
	out, in := t.out[h.from], t.in[h.from]
	out.lastHeard = now
	// Receiving side: learn what the peer has sent, so a lost tail shows up as a gap.
	if t.acceptSession(in, h.session) {
		if highestSent > in.highest {
			in.highest = min(highestSent, in.delivered+uint64(t.cfg.RecvAhead))
		}
		if in.highest > in.delivered && in.gapSince.IsZero() {
			in.gapSince = now
		}
	}
	// Sending side: the peer acknowledges my stream (only if it means this session).
	if rcvSession == t.session && rcvDelivered <= out.nextSeq {
		t.processAck(out, rcvDelivered, rcvWindow, now)
	}
}

// processAck frees acknowledged messages, measures RTT and grows the window.
func (t *Transport) processAck(out *outPeer, delivered uint64, window int, now time.Time) {
	out.peerWindow = window
	ackedPkts := 0
	var sample time.Duration
	have := false
	for len(out.inflight) > 0 && out.inflight[0].seq <= delivered {
		m := out.inflight[0]
		out.inflight[0] = nil
		out.inflight = out.inflight[1:]
		out.inflightBytes -= m.size
		out.inflightPkts -= len(m.frags)
		ackedPkts += len(m.frags)
		if m.retrans == 0 {
			sample, have = now.Sub(m.sentAt), true
		}
	}
	if len(out.inflight) == 0 {
		out.inflight = nil
	}
	if delivered > out.ackedSeq {
		out.ackedSeq = delivered
	}
	if have {
		out.srtt = (7*out.srtt + sample) / 8
	}
	if ackedPkts > 0 {
		if out.cwnd < out.ssthresh {
			out.cwnd += ackedPkts // slow start
		} else {
			out.caAcked += ackedPkts // congestion avoidance: +1 per window acknowledged
			for out.caAcked >= out.cwnd {
				out.caAcked -= out.cwnd
				out.cwnd++
			}
		}
		out.cwnd = min(out.cwnd, t.cfg.MaxCwnd)
	}
	t.wakeWriter() // the window may have opened
}

// onLoss reacts to a loss signal: at most one window reduction per RTT.
func (t *Transport) onLoss(out *outPeer, now time.Time, timeout bool) {
	if !timeout && now.Sub(out.lastLoss) < out.srtt {
		return
	}
	out.lastLoss = now
	out.ssthresh = max(out.cwnd/2, t.cfg.MinCwnd)
	if timeout {
		out.cwnd = t.cfg.MinCwnd
	} else {
		out.cwnd = out.ssthresh
	}
}

func (t *Transport) onNak(h header, body []byte, now time.Time) {
	if len(body) < 9 {
		t.droppedMalformed.Add(1)
		return
	}
	session := binary.BigEndian.Uint64(body)
	count := int(body[8])
	if count > maxNaks || len(body) != 9+8*count {
		t.droppedMalformed.Add(1)
		return
	}
	t.mu.Lock()
	out := t.out[h.from]
	out.lastHeard = now
	if session == t.session {
		t.naksRecv.Add(1)
		for i := 0; i < count && len(out.retx) < 4096; i++ {
			out.retx = append(out.retx, binary.BigEndian.Uint64(body[9+8*i:]))
		}
		t.onLoss(out, now, false)
	}
	t.mu.Unlock()
	t.wakeWriter()
}

// ---- timers: NAKs, gap give-up, retransmission timeout, STATUS, liveness ----

func (t *Transport) timerLoop() {
	defer t.wg.Done()
	tk := time.NewTicker(t.cfg.Tick)
	defer tk.Stop()
	for {
		select {
		case <-t.done:
			return
		case <-tk.C:
			t.tick()
		}
	}
}

func (t *Transport) rto(out *outPeer) time.Duration {
	return min(max(2*out.srtt+10*time.Millisecond, minRTO), maxRTO)
}

func (t *Transport) statusPacket(peer int, out *outPeer, in *inPeer) outPacket {
	body := make([]byte, statusLen)
	binary.BigEndian.PutUint64(body, out.highestTx)
	binary.BigEndian.PutUint64(body[8:], in.session)
	binary.BigEndian.PutUint64(body[16:], in.delivered)
	binary.BigEndian.PutUint32(body[24:], uint32(max(t.cfg.RecvBuffer-in.bufBytes, 0)))
	return outPacket{t.seal(typeStatus, peer, body), out.addr}
}

func (t *Transport) nakPacket(peer int, out *outPeer, in *inPeer, seqs []uint64) outPacket {
	body := make([]byte, 9, 9+8*len(seqs))
	binary.BigEndian.PutUint64(body, in.session)
	body[8] = byte(len(seqs))
	for _, s := range seqs {
		body = binary.BigEndian.AppendUint64(body, s)
	}
	return outPacket{t.seal(typeNak, peer, body), out.addr}
}

// missing lists up to maxNaks messages that are due another NAK.
func (t *Transport) missing(in *inPeer, now time.Time) []uint64 {
	var seqs []uint64
	last := min(in.highest, in.delivered+uint64(t.cfg.RecvAhead))
	for s := in.delivered + 1; s <= last && len(seqs) < maxNaks; s++ {
		if r := in.pending[s]; r != nil && r.got == r.cnt {
			continue
		}
		st := in.nak[s]
		if st == nil {
			st = &nakState{}
			in.nak[s] = st
		}
		if st.tries < t.cfg.MaxNaks && now.Sub(st.last) >= t.cfg.NakInterval {
			st.tries++
			st.last = now
			seqs = append(seqs, s)
		}
	}
	return seqs
}

// skipGap gives up on every missing message ahead of the first complete buffered one.
func (t *Transport) skipGap(in *inPeer) {
	to := in.highest
	for s := in.delivered + 1; s <= in.highest; s++ {
		if r := in.pending[s]; r != nil && r.got == r.cnt {
			to = s - 1
			break
		}
	}
	for s := in.delivered + 1; s <= to; s++ {
		if r := in.pending[s]; r != nil {
			in.bufBytes -= r.size
			delete(in.pending, s)
		}
		delete(in.nak, s)
	}
	t.skipped.Add(to - in.delivered)
	in.delivered = to
	in.dirty = true
}

type delivery struct {
	peer   int
	bodies [][]byte
}

func (t *Transport) tick() {
	now := time.Now()
	t.dmu.Lock() // the gap give-up below may deliver messages
	var pkts []outPacket
	var deliveries []delivery
	var reports []int
	retx := false

	t.mu.Lock()
	for peer, out := range t.out {
		if out == nil {
			continue
		}
		in := t.in[peer]

		// Receiver: ask for what is missing, and give up on it after GapTimeout.
		if in.highest > in.delivered {
			for i := 0; i < 4; i++ { // up to 4 NAK packets (256 sequences) per tick
				seqs := t.missing(in, now)
				if len(seqs) == 0 {
					break
				}
				t.naksSent.Add(1)
				pkts = append(pkts, t.nakPacket(peer, out, in, seqs))
			}
			if !in.gapSince.IsZero() && now.Sub(in.gapSince) >= t.cfg.GapTimeout {
				t.skipGap(in)
				if bodies := t.drain(in); len(bodies) > 0 {
					deliveries = append(deliveries, delivery{peer, bodies})
				}
				if in.highest > in.delivered {
					in.gapSince = now
				} else {
					in.gapSince = time.Time{}
				}
			}
		}

		// Sender: retransmission timeout on the oldest unacknowledged message.
		if len(out.inflight) > 0 {
			head := out.inflight[0]
			if now.Sub(head.lastTx) > t.rto(out) {
				if head.retrans >= t.cfg.MaxRetrans {
					out.inflight[0] = nil
					out.inflight = out.inflight[1:]
					out.inflightBytes -= head.size
					out.inflightPkts -= len(head.frags)
					t.gaveUp.Add(1)
				} else {
					out.retx = append(out.retx, head.seq)
					retx = true
					t.onLoss(out, now, true)
				}
			}
		}
		if len(out.retx) > 0 {
			retx = true
		}

		// STATUS: acknowledgement, window, what I have sent, and a heartbeat.
		if now.Sub(out.lastStatus) >= t.cfg.Keepalive || (in.dirty && now.Sub(out.lastStatus) >= t.cfg.StatusInterval) {
			pkts = append(pkts, t.statusPacket(peer, out, in))
			out.lastStatus = now
			in.dirty = false
		}

		// Liveness: tell Raft when a peer has gone quiet or its queue overflowed.
		if (now.Sub(out.lastHeard) > t.cfg.PeerTimeout || out.needReport) && now.Sub(out.lastReport) >= t.cfg.ReportInterval {
			reports = append(reports, peer)
			out.lastReport = now
			out.needReport = false
		}
	}
	t.mu.Unlock()

	t.write(pkts)
	if retx {
		t.wakeWriter()
	}
	for _, d := range deliveries {
		t.deliver(d.peer, d.bodies)
	}
	t.dmu.Unlock()
	if box := t.ep.Load(); box != nil {
		for _, peer := range reports {
			t.unreach.Add(1)
			box.ep.ReportUnreachable(peer)
		}
	}
}
