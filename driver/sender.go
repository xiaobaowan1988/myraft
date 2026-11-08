package driver

import (
	"fmt"
	"net/netip"
	"sync/atomic"

	"github.com/xiaobaowan1988/myraft/driver/counters"
	"github.com/xiaobaowan1988/myraft/driver/logbuf"
	"github.com/xiaobaowan1988/myraft/driver/proto"
)

const (
	maxDatagramsPerCycle = 16
	maxControlPerCycle   = 16
)

// SenderStats counts what the sender has done. Read it with atomic loads via Snapshot.
type SenderStats struct {
	DatagramsSent, BytesSent, HeartbeatsSent, SetupsSent, StatusReceived, Malformed, SendBlocked int64
}

// senderPub is the sender's state for one network publication. After the conductor
// hands it over, only the sender touches it (apart from the shared-memory log and
// counters, which are meant to be shared).
type senderPub struct {
	conn  PacketConn
	dest  netip.AddrPort
	log   *logbuf.LogBuffer
	posC  counters.Counter // sent position
	limC  counters.Counter // send limit
	consC counters.Counter // receiver consumption position

	session, stream, initialTerm int32
	termLength, mtu              int
	termShift                    uint

	senderPos   int64
	consumption int64
	senderLimit int64
	connected   bool
	lastStatus  int64
	lastSetup   int64
	lastSend    int64
	started     bool
}

// Sender is the agent that moves published data from log buffers onto the network
// and handles what comes back (Status, and from 21.5 NAK).
type Sender struct {
	cfg  Config
	q    queue
	pubs []*senderPub
	buf  []byte
	st   SenderStats
}

func newSender(cfg Config) *Sender { return &Sender{cfg: cfg, q: newQueue(), buf: make([]byte, 65536)} }

func (s *Sender) Name() string { return "sender" }

// Stats returns a snapshot of the counters; it is safe to call from any goroutine.
func (s *Sender) Stats() SenderStats {
	return SenderStats{
		DatagramsSent: atomic.LoadInt64(&s.st.DatagramsSent), BytesSent: atomic.LoadInt64(&s.st.BytesSent),
		HeartbeatsSent: atomic.LoadInt64(&s.st.HeartbeatsSent), SetupsSent: atomic.LoadInt64(&s.st.SetupsSent),
		StatusReceived: atomic.LoadInt64(&s.st.StatusReceived), Malformed: atomic.LoadInt64(&s.st.Malformed),
		SendBlocked: atomic.LoadInt64(&s.st.SendBlocked),
	}
}

func incr(p *int64) { atomic.AddInt64(p, 1) }

func (s *Sender) add(sp *senderPub) { s.pubs = append(s.pubs, sp) }

func (s *Sender) remove(sp *senderPub) {
	for i, p := range s.pubs {
		if p == sp {
			s.pubs = append(s.pubs[:i:i], s.pubs[i+1:]...)
			break
		}
	}
	sp.conn.Close()
}

func (s *Sender) OnClose() {
	for _, p := range s.pubs {
		p.conn.Close()
	}
	s.pubs = nil
}

func (sp *senderPub) termAndOffset(pos int64) (termID, offset int32) {
	return sp.initialTerm + int32(pos>>sp.termShift), int32(pos & int64(sp.termLength-1))
}

func (s *Sender) DoWork(now int64) int {
	work := s.q.drain()
	for _, sp := range s.pubs {
		if !sp.started {
			sp.started, sp.lastStatus = true, now
		}
		work += s.receiveControl(sp, now)
		if sp.connected && now-sp.lastStatus > int64(s.cfg.ReceiverTimeout) {
			sp.connected = false // the receiver went away: stop sending until a new Setup is answered
			sp.log.SetConnected(false)
			work++
		}
		if !sp.connected {
			if sp.lastSetup == 0 || now-sp.lastSetup >= int64(s.cfg.SetupInterval) {
				work += s.sendSetup(sp, now)
			}
			continue
		}
		work += s.sendData(sp, now)
	}
	return work
}

func (s *Sender) receiveControl(sp *senderPub, now int64) int {
	work := 0
	for i := 0; i < maxControlPerCycle; i++ {
		n, from, err := sp.conn.Recv(s.buf)
		if err != nil || n == 0 {
			break
		}
		work++
		if from.Addr().Unmap() != sp.dest.Addr().Unmap() || from.Port() != sp.dest.Port() {
			incr(&s.st.Malformed) // not from the receiver we send to
			continue
		}
		kind, err := proto.Kind(s.buf[:n])
		if err != nil {
			incr(&s.st.Malformed)
			continue
		}
		switch kind {
		case proto.TypeStatus:
			st, err := proto.ParseStatus(s.buf[:n])
			if err != nil || st.SessionID != sp.session || st.StreamID != sp.stream {
				incr(&s.st.Malformed)
				continue
			}
			s.onStatus(sp, st, now)
		default:
			// NAK handling arrives in stage 21.5; anything else is not for a sender.
			incr(&s.st.Malformed)
		}
	}
	return work
}

func (s *Sender) onStatus(sp *senderPub, st proto.Status, now int64) {
	incr(&s.st.StatusReceived)
	pos := int64(st.TermID-sp.initialTerm)<<sp.termShift + int64(st.TermOffset)
	if pos < 0 || pos > sp.senderPos || int(st.TermOffset) >= sp.termLength {
		incr(&s.st.Malformed) // a receiver cannot have consumed what was never sent
		return
	}
	sp.lastStatus = now
	if !sp.connected {
		sp.connected = true
		sp.consumption = pos
		sp.senderLimit = 0
		sp.limC.Set(0)
		sp.consC.Set(pos)
		sp.log.SetConnected(true)
	} else if pos > sp.consumption {
		sp.consumption = pos
		sp.consC.Set(pos)
	}
	if lim := sp.consumption + int64(st.Window); lim > sp.senderLimit {
		sp.senderLimit = lim
		sp.limC.Set(lim)
	}
}

func (s *Sender) sendSetup(sp *senderPub, now int64) int {
	term, off := sp.termAndOffset(sp.senderPos)
	b := proto.EncodeSetup(proto.Setup{SessionID: sp.session, StreamID: sp.stream, TermID: term, TermOffset: off,
		InitialTermID: sp.initialTerm, TermLength: int32(sp.termLength), MTU: int32(sp.mtu)})
	sent, err := sp.conn.Send(b, sp.dest)
	if err != nil || !sent {
		incr(&s.st.SendBlocked)
		return 0
	}
	sp.lastSetup = now
	incr(&s.st.SetupsSent)
	return 1
}

func (s *Sender) sendData(sp *senderPub, now int64) int {
	work := 0
	for i := 0; i < maxDatagramsPerCycle; i++ {
		avail := sp.senderLimit - sp.senderPos
		if avail < logbuf.FrameAlign {
			break
		}
		frames, adv := sp.log.Scan(sp.senderPos, int(min(int64(sp.mtu), avail)))
		if adv == 0 {
			break
		}
		sent, err := sp.conn.Send(frames, sp.dest)
		if err != nil || !sent {
			incr(&s.st.SendBlocked)
			break
		}
		sp.senderPos += int64(adv)
		sp.posC.Set(sp.senderPos)
		sp.lastSend = now
		incr(&s.st.DatagramsSent)
		atomic.AddInt64(&s.st.BytesSent, int64(len(frames)))
		work++
	}
	if work == 0 && now-sp.lastSend >= int64(s.cfg.HeartbeatInterval) {
		term, off := sp.termAndOffset(sp.senderPos)
		b := proto.EncodeHeartbeat(proto.Heartbeat{SessionID: sp.session, StreamID: sp.stream, TermID: term, TermOffset: off})
		if sent, err := sp.conn.Send(b, sp.dest); err == nil && sent {
			sp.lastSend = now
			incr(&s.st.HeartbeatsSent)
			work++
		}
	}
	return work
}

func (sp *senderPub) String() string {
	return fmt.Sprintf("sender[%d/%d -> %v pos %d]", sp.session, sp.stream, sp.dest, sp.senderPos)
}
