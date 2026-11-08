package stream

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
)

// Wire format (big-endian). Every datagram is
//
//	header (16) | body | HMAC-SHA256 tag, truncated to 16 bytes
//
//	header: magic(1) version(1) type(1) flags(1) from(2) to(2) session(8)
//
// session is used by DATA only: the sender's session of that (peer, stream).
//
// Bodies:
//
//	DATA   : stream(1) rcvInc(8) pos(8) payload      bytes [pos, pos+len) of a stream
//	STATUS : incarnation(8) count(1) { stream(1) session(8) sentPos(8)
//	                                   rcvSession(8) rcvContig(8) rcvLimit(8) } * count
//	NAK    : stream(1) rcvSession(8) count(1) { pos(8) len(4) } * count
//
// A node has Config.Streams independent streams to each peer, numbered from 0.
// rcvInc is the process instance the stream was created for; a receiver ignores
// DATA addressed to any other instance (for instance its own previous life).
//
// STATUS describes both directions for every stream. incarnation is the sender
// process's instance id; per stream, session/sentPos describe the sender's
// outgoing stream (how far it has been transmitted, so a lost tail is noticed),
// and rcvSession/rcvContig/rcvLimit acknowledge the *peer's* stream: which session,
// how many bytes arrived contiguously, and the right edge of the window
// (consumed + RecvWindow). A NAK asks for byte ranges of the peer's stream
// (rcvSession names the session it refers to).
const (
	magic   = 0xA7
	version = 3 // incompatible with the single-stream protocol

	typeData   = 1
	typeStatus = 2
	typeNak    = 3

	hdrLen         = 16
	macLen         = 16
	dataHdrLen     = 17
	statusHdrLen   = 9
	statusEntryLen = 41
	nakHdrLen      = 10
	nakEntryLen    = 12
	maxNakRanges   = 32
)

type header struct {
	typ      byte
	from, to int
	session  uint64
}

func (n *Node) mac(b []byte) []byte {
	h := hmac.New(sha256.New, n.cfg.Key)
	h.Write(b)
	return h.Sum(nil)[:macLen]
}

// seal builds a complete datagram for peer `to` on the stream session `session`.
func (n *Node) seal(typ byte, to int, session uint64, body []byte) []byte {
	pkt := make([]byte, hdrLen, hdrLen+len(body)+macLen)
	pkt[0], pkt[1], pkt[2] = magic, version, typ
	binary.BigEndian.PutUint16(pkt[4:], uint16(n.cfg.ID))
	binary.BigEndian.PutUint16(pkt[6:], uint16(to))
	binary.BigEndian.PutUint64(pkt[8:], session)
	pkt = append(pkt, body...)
	return append(pkt, n.mac(pkt)...)
}

// open authenticates and parses a datagram. The returned body aliases pkt.
func (n *Node) open(pkt []byte) (header, []byte, bool) {
	if len(pkt) < hdrLen+macLen || pkt[0] != magic || pkt[1] != version {
		n.droppedMalformed.Add(1)
		return header{}, nil, false
	}
	end := len(pkt) - macLen
	if !hmac.Equal(pkt[end:], n.mac(pkt[:end])) {
		n.droppedAuth.Add(1)
		return header{}, nil, false
	}
	h := header{
		typ:     pkt[2],
		from:    int(binary.BigEndian.Uint16(pkt[4:])),
		to:      int(binary.BigEndian.Uint16(pkt[6:])),
		session: binary.BigEndian.Uint64(pkt[8:]),
	}
	return h, pkt[hdrLen:end], true
}
