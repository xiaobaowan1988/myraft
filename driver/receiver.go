package driver

import (
	"net/netip"
	"sync/atomic"

	"github.com/xiaobaowan1988/myraft/driver/counters"
	"github.com/xiaobaowan1988/myraft/driver/logbuf"
	"github.com/xiaobaowan1988/myraft/driver/proto"
)

const (
	maxDatagramsPerEndpoint = 32
	imageRequestRetry       = int64(1e9) // a Setup that was not answered by the conductor may ask again after this
)

// ReceiverStats counts what the receiver has done.
type ReceiverStats struct {
	DatagramsReceived, FramesInserted, Dropped, Malformed, StatusSent, ImagesRequested, NaksSent int64
}

type imgKey struct{ session, stream int32 }

// imageHandle is what the conductor gives the receiver for a new image.
type imageHandle struct {
	key       imgKey
	src       netip.AddrPort
	log       *logbuf.LogBuffer
	hwmC      counters.Counter
	rcvPosC   counters.Counter
	consC     counters.Counter
	joinPos   int64
	window    int
	imageName string
}

// recvEndpoint is one bound socket and the images that arrive on it.
type recvEndpoint struct {
	id      int64
	conn    PacketConn
	streams map[int32]int // subscribed streams, with a count of subscriptions
	images  map[imgKey]*recvImage
	pending map[imgKey]int64
}

type recvImage struct {
	imageHandle
	hwm, rebuild int64
	dirty        bool
	lastActivity int64
	lastStatus   int64
	lastStatPos  int64
	statusSent   bool
	cleaned      int32
	inactive     bool
	nakGapStart  int64 // the gap being NAKed, or -1
	gapSince     int64
	lastNak      int64
	nakInterval  int64
	termShift    uint
	initialTerm  int32
}

// Receiver is the agent that takes datagrams off the network and rebuilds streams in
// image log buffers. Subscribers (through the conductor's bookkeeping) read the
// images; the receiver tells senders how far they have got.
type Receiver struct {
	cfg  Config
	cond *Conductor
	q    queue
	eps  map[int64]*recvEndpoint
	buf  []byte
	st   ReceiverStats
}

func newReceiver(cfg Config) *Receiver {
	return &Receiver{cfg: cfg, q: newQueue(), eps: map[int64]*recvEndpoint{}, buf: make([]byte, 65536)}
}

func (r *Receiver) Name() string { return "receiver" }

func (r *Receiver) Stats() ReceiverStats {
	return ReceiverStats{
		DatagramsReceived: atomic.LoadInt64(&r.st.DatagramsReceived), FramesInserted: atomic.LoadInt64(&r.st.FramesInserted),
		Dropped: atomic.LoadInt64(&r.st.Dropped), Malformed: atomic.LoadInt64(&r.st.Malformed),
		StatusSent: atomic.LoadInt64(&r.st.StatusSent), ImagesRequested: atomic.LoadInt64(&r.st.ImagesRequested),
		NaksSent: atomic.LoadInt64(&r.st.NaksSent),
	}
}

func (r *Receiver) OnClose() {
	for _, ep := range r.eps {
		ep.conn.Close()
	}
	r.eps = map[int64]*recvEndpoint{}
}

func (r *Receiver) addEndpoint(id int64, conn PacketConn) {
	r.eps[id] = &recvEndpoint{id: id, conn: conn, streams: map[int32]int{}, images: map[imgKey]*recvImage{}, pending: map[imgKey]int64{}}
}

func (r *Receiver) removeEndpoint(id int64) {
	if ep := r.eps[id]; ep != nil {
		ep.conn.Close()
		delete(r.eps, id)
	}
}

func (r *Receiver) addStream(id int64, stream int32) {
	if ep := r.eps[id]; ep != nil {
		ep.streams[stream]++
	}
}

func (r *Receiver) removeStream(id int64, stream int32) {
	if ep := r.eps[id]; ep != nil {
		if ep.streams[stream]--; ep.streams[stream] <= 0 {
			delete(ep.streams, stream)
		}
	}
}

func (r *Receiver) registerImage(epID int64, h imageHandle, now int64) {
	ep := r.eps[epID]
	if ep == nil {
		return
	}
	delete(ep.pending, h.key)
	img := &recvImage{imageHandle: h, hwm: h.joinPos, rebuild: h.joinPos, lastActivity: now,
		termShift: uint(log2(h.log.TermLength())), initialTerm: h.log.InitialTermID()}
	img.cleaned = int32(h.joinPos >> img.termShift)
	img.nakGapStart = -1
	ep.images[h.key] = img
	h.hwmC.Set(h.joinPos)
	h.rcvPosC.Set(h.joinPos)
}

func (r *Receiver) closeImage(epID int64, key imgKey) {
	if ep := r.eps[epID]; ep != nil {
		delete(ep.images, key)
		delete(ep.pending, key)
	}
}

func log2(n int) int {
	s := 0
	for 1<<s < n {
		s++
	}
	return s
}

func (r *Receiver) DoWork(now int64) int {
	work := r.q.drain()
	for _, ep := range r.eps {
		for i := 0; i < maxDatagramsPerEndpoint; i++ {
			n, from, err := ep.conn.Recv(r.buf)
			if err != nil || n == 0 {
				break
			}
			work++
			atomic.AddInt64(&r.st.DatagramsReceived, 1)
			r.onDatagram(ep, r.buf[:n], from, now)
		}
		for key, img := range ep.images {
			work += r.serviceImage(ep, key, img, now)
		}
	}
	return work
}

func (r *Receiver) onDatagram(ep *recvEndpoint, b []byte, from netip.AddrPort, now int64) {
	kind, err := proto.Kind(b)
	if err != nil {
		atomic.AddInt64(&r.st.Malformed, 1)
		return
	}
	switch kind {
	case proto.TypeSetup:
		r.onSetup(ep, b, from, now)
	case proto.TypeData, proto.TypePad:
		r.onData(ep, b, from, now)
	case proto.TypeHeartbeat:
		hb, err := proto.ParseHeartbeat(b)
		if err != nil {
			atomic.AddInt64(&r.st.Malformed, 1)
			return
		}
		if img := ep.images[imgKey{hb.SessionID, hb.StreamID}]; img != nil && sameAddr(img.src, from) {
			img.lastActivity = now
			if pos := int64(hb.TermID-img.initialTerm)<<img.termShift + int64(hb.TermOffset); pos > img.hwm {
				img.hwm = pos
				img.hwmC.Set(pos)
			}
		}
	default:
		atomic.AddInt64(&r.st.Malformed, 1)
	}
}

func sameAddr(a, b netip.AddrPort) bool {
	return a.Addr().Unmap() == b.Addr().Unmap() && a.Port() == b.Port()
}

func (r *Receiver) onSetup(ep *recvEndpoint, b []byte, from netip.AddrPort, now int64) {
	s, err := proto.ParseSetup(b)
	if err != nil {
		atomic.AddInt64(&r.st.Malformed, 1)
		return
	}
	if ep.streams[s.StreamID] == 0 {
		return // nobody here wants that stream
	}
	key := imgKey{s.SessionID, s.StreamID}
	if img := ep.images[key]; img != nil {
		if sameAddr(img.src, from) {
			img.lastActivity = now
			img.lastStatus = 0 // the sender is still waiting for an answer: our Status may have been lost
		}
		return
	}
	if t, ok := ep.pending[key]; ok && now-t < imageRequestRetry {
		return
	}
	params := logbuf.Params{TermLength: int(s.TermLength), MTU: int(s.MTU), InitialTermID: s.InitialTermID}
	termCount := int64(s.TermID) - int64(s.InitialTermID)
	if params.Validate() != nil || params.MTU > 65000 || termCount < 0 || termCount > 1<<30 ||
		s.TermOffset < 0 || int(s.TermOffset) >= params.TermLength || s.TermOffset%logbuf.FrameAlign != 0 {
		atomic.AddInt64(&r.st.Malformed, 1)
		return
	}
	ep.pending[key] = now
	atomic.AddInt64(&r.st.ImagesRequested, 1)
	req := imageRequest{epID: ep.id, src: from, setup: s}
	r.cond.q.post(func() { r.cond.createImage(req) })
}

func (r *Receiver) onData(ep *recvEndpoint, b []byte, from netip.AddrPort, now int64) {
	h, err := proto.CheckData(b)
	if err != nil {
		atomic.AddInt64(&r.st.Malformed, 1)
		return
	}
	img := ep.images[imgKey{h.SessionID, h.StreamID}]
	if img == nil || !sameAddr(img.src, from) {
		atomic.AddInt64(&r.st.Dropped, 1) // no image yet: the sender's Setup will create one
		return
	}
	img.lastActivity = now
	if h.TermID-img.initialTerm < 0 || int(h.TermOffset) >= img.log.TermLength() {
		atomic.AddInt64(&r.st.Malformed, 1)
		return
	}
	pos := int64(h.TermID-img.initialTerm)<<img.termShift + int64(h.TermOffset)
	span := int64(proto.DataSpan(b))
	// The window limits what is on the wire, not what a padding frame says it covers: a
	// padding frame is 32 bytes however much of the term it fills, and refusing it
	// because of the space it skips would stall the stream at the end of that term.
	wire := int64((len(b) + logbuf.FrameAlign - 1) &^ (logbuf.FrameAlign - 1))
	if pos < img.joinPos || pos+span <= img.rebuild || pos+wire > img.consC.Get()+int64(img.window) {
		atomic.AddInt64(&r.st.Dropped, 1) // before the join point, already have it, or beyond the window
		return
	}
	end, err := img.log.Insert(pos, b)
	if err != nil {
		atomic.AddInt64(&r.st.Malformed, 1)
	} else {
		atomic.AddInt64(&r.st.FramesInserted, 1)
	}
	if end > img.hwm {
		img.hwm = end
		img.hwmC.Set(end)
	}
	img.dirty = true
}

// serviceImage does the per-cycle work of one image: advance the contiguous position,
// clean terms the subscribers have left, report to the sender, and watch its liveness.
func (r *Receiver) serviceImage(ep *recvEndpoint, key imgKey, img *recvImage, now int64) int {
	work := 0
	if img.dirty {
		img.dirty = false
		if end := img.log.ContiguousEnd(img.rebuild, img.hwm); end != img.rebuild {
			img.rebuild = end
			img.rcvPosC.Set(end)
			work++
		}
	}
	cons := img.consC.Get()
	if tc := int32(cons >> img.termShift); tc > img.cleaned {
		for t := img.cleaned + 1; t <= tc; t++ {
			img.log.CleanTerm(t + 1) // the partition of term t+1 last held term t-2
		}
		img.cleaned = tc
		work++
	}
	work += r.detectLoss(ep, key, img, now)
	if !img.inactive && now-img.lastActivity > int64(r.cfg.ImageLivenessTimeout) {
		img.inactive = true
		epID := ep.id
		r.cond.q.post(func() { r.cond.imageInactive(epID, key) })
		work++
	}
	if !img.inactive && (!img.statusSent || now-img.lastStatus >= int64(r.cfg.StatusInterval) || cons-img.lastStatPos >= int64(img.window/4)) {
		term := img.initialTerm + int32(cons>>img.termShift)
		off := int32(cons & int64(img.log.TermLength()-1))
		b := proto.EncodeStatus(proto.Status{SessionID: key.session, StreamID: key.stream, TermID: term, TermOffset: off, Window: int32(img.window)})
		if sent, err := ep.conn.Send(b, img.src); err == nil && sent {
			img.statusSent, img.lastStatus, img.lastStatPos = true, now, cons
			atomic.AddInt64(&r.st.StatusSent, 1)
			work++
		}
	}
	return work
}

const maxNakInterval = int64(200e6) // 200ms

// detectLoss NAKs the gap in front of the contiguous position. A gap exists when the
// highest position seen (data, or the sender's position from a heartbeat) is beyond
// the contiguous one. It ends at the next frame that did arrive, or at the highest
// position when nothing after it did (loss at the tail). A NAK covers one term.
func (r *Receiver) detectLoss(ep *recvEndpoint, key imgKey, img *recvImage, now int64) int {
	if img.inactive || img.hwm <= img.rebuild {
		img.nakGapStart = -1
		return 0
	}
	start := img.rebuild
	if start != img.nakGapStart { // a new gap, or the old one got shorter: wait before asking
		img.nakGapStart, img.gapSince, img.lastNak = start, now, 0
		img.nakInterval = int64(r.cfg.NakRetryInterval)
	}
	if img.lastNak == 0 {
		if now-img.gapSince < int64(r.cfg.NakDelay) {
			return 0
		}
	} else if now-img.lastNak < img.nakInterval {
		return 0
	}
	end := img.log.NextFrameAfter(start, img.hwm)
	if termEnd := (start>>img.termShift + 1) << img.termShift; end > termEnd {
		end = termEnd
	}
	b := proto.EncodeNak(proto.Nak{SessionID: key.session, StreamID: key.stream,
		TermID: img.initialTerm + int32(start>>img.termShift), TermOffset: int32(start & int64(img.log.TermLength()-1)), Length: int32(end - start)})
	sent, err := ep.conn.Send(b, img.src)
	if err != nil || !sent {
		return 0
	}
	img.lastNak = now
	if img.nakInterval *= 2; img.nakInterval > maxNakInterval {
		img.nakInterval = maxNakInterval
	}
	atomic.AddInt64(&r.st.NaksSent, 1)
	return 1
}
