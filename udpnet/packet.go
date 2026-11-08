package udpnet

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
// Bodies:
//
//	DATA   : msgSeq(8) fragIdx(2) fragCnt(2) payload
//	STATUS : highestSent(8) rcvSession(8) rcvDelivered(8) rcvWindow(4)
//	NAK    : rcvSession(8) count(1) seq(8)*count
//
// session identifies the sender's incarnation. In STATUS and NAK, rcvSession is the
// session of the *other* node's stream that the sender is acknowledging.
const (
	magic   = 0xA7
	version = 1

	typeData   = 1
	typeStatus = 2
	typeNak    = 3

	hdrLen     = 16
	macLen     = 16
	dataHdrLen = 12
	statusLen  = 28
	maxNaks    = 64 // sequence numbers per NAK packet
)

type header struct {
	typ      byte
	from, to int
	session  uint64
}

func (t *Transport) mac(b []byte) []byte {
	h := hmac.New(sha256.New, t.cfg.Key)
	h.Write(b)
	return h.Sum(nil)[:macLen]
}

// seal builds a complete datagram for peer `to`.
func (t *Transport) seal(typ byte, to int, body []byte) []byte {
	pkt := make([]byte, hdrLen, hdrLen+len(body)+macLen)
	pkt[0], pkt[1], pkt[2] = magic, version, typ
	binary.BigEndian.PutUint16(pkt[4:], uint16(t.cfg.ID))
	binary.BigEndian.PutUint16(pkt[6:], uint16(to))
	binary.BigEndian.PutUint64(pkt[8:], t.session)
	pkt = append(pkt, body...)
	return append(pkt, t.mac(pkt)...)
}

// open authenticates and parses a datagram. The returned body aliases pkt.
func (t *Transport) open(pkt []byte) (header, []byte, bool) {
	if len(pkt) < hdrLen+macLen || pkt[0] != magic || pkt[1] != version {
		t.droppedMalformed.Add(1)
		return header{}, nil, false
	}
	n := len(pkt) - macLen
	if !hmac.Equal(pkt[n:], t.mac(pkt[:n])) {
		t.droppedAuth.Add(1)
		return header{}, nil, false
	}
	h := header{
		typ:     pkt[2],
		from:    int(binary.BigEndian.Uint16(pkt[4:])),
		to:      int(binary.BigEndian.Uint16(pkt[6:])),
		session: binary.BigEndian.Uint64(pkt[8:]),
	}
	return h, pkt[hdrLen:n], true
}
