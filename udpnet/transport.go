// Package udpnet runs Raft over a reliable ordered stream: it is the thin adapter
// between raft.Transport / raft.Endpoint and package stream, which does the
// real work of reliability (see there). What it adds is the message encoding and the
// mapping of the stream's events onto what Raft understands:
//
//	Send(m)      -> encode + Offer into the stream to m.To
//	OnMessage    -> decode, check the message's From/To match the stream, Step
//	OnPeerDown   -> Endpoint.ReportUnreachable
//
// Because a stream never loses, duplicates or reorders messages while it lives,
// Raft's replication pipeline is not disturbed by the network; the only failure
// left is the stream ending, which Raft already handles by falling back to probing.
//
// Two streams per peer keep control traffic from being held up by bulk data: entries
// travel on the bulk stream, everything else (heartbeats, votes, replies) on the
// control stream, so a huge command delays only other bulk data, never a heartbeat.
//
// One setting must change when running Raft on a reliable stream: set
// raft.AppendTimeout well above the time it takes to deliver your largest message.
// That timeout exists for transports that lose messages (it must then be shorter
// than the election timeout); here a message in flight is not lost, so a timeout
// that fires early would resend it again and again, piling copies into the stream.
// Keep it only as a safety net (the tests use 10s).
package udpnet

import (
	"sync/atomic"

	"github.com/xiaobaowan1988/myraft/raft"
	"github.com/xiaobaowan1988/myraft/stream"
)

// Config is the stream configuration (addresses, key, windows, timeouts).
type Config = stream.Config

const (
	streamControl = 0 // heartbeats, votes, replies: small and latency-critical
	streamBulk    = 1 // MsgApp carrying log entries: possibly large
)

type epBox struct{ ep raft.Endpoint }

// Transport implements raft.Transport on top of a stream.Node.
type Transport struct {
	n     *stream.Node
	codec CommandCodec
	me    int
	ep    atomic.Pointer[epBox]

	droppedEncode, droppedOffer, droppedBadMessage atomic.Uint64
}

var (
	_ raft.Transport = (*Transport)(nil)
	_ stream.Handler = (*Transport)(nil)
)

// Stats counts what the adapter itself dropped, next to the stream's own counters.
type Stats struct {
	Stream stream.Stats
	// DroppedEncode: Send could not encode the message (e.g. an unregistered command
	// type). DroppedOffer: the stream refused it (back-pressure, peer down, too big).
	// DroppedBadMessage: an arriving message was undecodable or claimed another sender.
	DroppedEncode, DroppedOffer, DroppedBadMessage uint64
}

// New starts a stream node and wraps it. A nil codec means DefaultCodec.
func New(cfg Config, codec CommandCodec) (*Transport, error) {
	if codec == nil {
		codec = DefaultCodec{}
	}
	t := &Transport{codec: codec, me: cfg.ID}
	cfg.Streams = 2 // control and bulk
	n, err := stream.New(cfg)
	if err != nil {
		return nil, err
	}
	t.n = n
	n.SetHandler(t)
	return t, nil
}

// SetEndpoint connects the transport to the Raft node that receives its messages.
func (t *Transport) SetEndpoint(ep raft.Endpoint) { t.ep.Store(&epBox{ep}) }

// Close stops the underlying stream node.
func (t *Transport) Close() error { return t.n.Close() }

// Stats returns the counters.
func (t *Transport) Stats() Stats {
	return Stats{Stream: t.n.Stats(), DroppedEncode: t.droppedEncode.Load(),
		DroppedOffer: t.droppedOffer.Load(), DroppedBadMessage: t.droppedBadMessage.Load()}
}

// Send implements raft.Transport. It never blocks. If the stream cannot take the
// message the message is dropped, which Raft tolerates, and Raft is told the peer
// is unreachable so it restarts replication from a safe point at once instead of
// waiting for a timeout.
func (t *Transport) Send(m raft.Message) {
	body, err := encodeMessage(m, t.codec)
	if err != nil {
		t.droppedEncode.Add(1)
		return
	}
	if _, res := t.n.Offer(m.To, class(m), body); res != stream.Offered {
		t.droppedOffer.Add(1)
		if res == stream.BackPressured || res == stream.NotConnected {
			t.n.ReportDown(m.To)
		}
	}
}

// class chooses the stream: entries are bulk, everything else is control. A
// heartbeat is anchored at matchIndex and does not depend on earlier replication
// messages, so its being overtaken by or overtaking bulk data is harmless.
func class(m raft.Message) int {
	if m.Type == raft.MsgApp && len(m.Entries) > 0 {
		return streamBulk
	}
	return streamControl
}

// OnMessage implements stream.Handler.
func (t *Transport) OnMessage(from, _ int, b []byte) {
	m, err := decodeMessage(b, t.codec)
	if err != nil || m.From != from || m.To != t.me {
		// Undecodable, or the message claims another sender than the authenticated
		// stream it arrived on: refuse it.
		t.droppedBadMessage.Add(1)
		return
	}
	if box := t.ep.Load(); box != nil {
		box.ep.Step(m)
	}
}

// OnPeerDown implements stream.Handler.
func (t *Transport) OnPeerDown(peer int) {
	if box := t.ep.Load(); box != nil {
		box.ep.ReportUnreachable(peer)
	}
}
