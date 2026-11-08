// Package logbuf is the log buffer: the shared-memory structure a stream's messages
// live in, written by publishers, read by subscribers (IPC) or by the driver's
// sender (UDP), and rebuilt by the driver's receiver on the other side.
//
// Layout of a log buffer (a file of 3*termLength + MetadataLength bytes):
//
//	| term 0 | term 1 | term 2 | metadata |
//
// A term is an array of frames, each a 32-byte header plus payload, padded to a
// multiple of 32 bytes. Three terms are used in rotation: the one being written, the
// previous one (readers may still be in it) and a clean one ready to be written next.
// A stream position is the number of bytes since the stream began:
//
//	position = (termID - initialTermID) * termLength + termOffset
//
// Writers claim space by atomically adding to the tail of the active term, fill it,
// and publish each frame by storing its length last. Readers only trust frames whose
// length is positive.
//
// Flow-control invariant: the term cleaned for reuse is the one that held the
// term before the previous one, so a reader may lag the writer by less than one
// full term. The caller of Offer enforces this through its position limit: the limit
// must be at most termLength/2 ahead of the slowest reader, and must leave room below
// termLength/2 for one maximum-size message per concurrent publisher (the limit is
// checked before space is claimed, so each publisher can overshoot it once).
//
// Assumption: a publisher finishes writing a frame it has claimed before the other
// publishers fill two more terms. Otherwise its space would be cleaned under it. With
// terms of megabytes this means "before milliseconds pass"; a publisher that dies
// instead is covered by Unblock.
package logbuf

import (
	"errors"
	"fmt"
	"unsafe"

	"github.com/xiaobaowan1988/myraft/driver/shm"
)

const (
	HeaderLength = 32
	FrameAlign   = 32

	TypePad  = 0 // padding: fills the end of a term, or a hole left by a dead writer
	TypeData = 1

	FlagBegin        = 0x80
	FlagEnd          = 0x40
	FlagUnfragmented = FlagBegin | FlagEnd

	MetadataLength = 4096
	MinTermLength  = 4096
	MaxTermLength  = 1 << 30
	partitions     = 3

	maxMessageCap = 16 << 20
)

// Header field offsets.
const (
	offLength    = 0  // int32: frame length incl. header; <0 while being written, 0 if unwritten
	offVerFlType = 4  // int32: version | flags<<8 | type<<16
	offTermOff   = 8  // int32
	offSession   = 12 // int32
	offStream    = 16 // int32
	offTermID    = 20 // int32
	// 24..32 reserved
)

// Metadata field offsets (from the start of the metadata area).
const (
	metaTail       = 0 // 3 × int64, one cache line each: termID<<32 | termOffset
	metaActive     = 192
	metaInitTerm   = 256
	metaMTU        = 260
	metaTermLength = 264
	metaSession    = 268
	metaStream     = 272
	metaMagic      = 280
	magic          = 0x4C4F4742
	version        = 1
)

func init() {
	// Frames are shared as raw memory (and later sent verbatim), so byte order is part
	// of the format: little-endian only.
	x := uint32(1)
	if *(*byte)(unsafe.Pointer(&x)) != 1 {
		panic("logbuf: big-endian hosts are not supported")
	}
}

// Status is the outcome of Offer.
type Status int

const (
	Ok            Status = iota // the message was written
	BackPressured               // the position limit was reached; try again later
	AdminAction                 // the term is being rotated; try again at once
	TooBig                      // larger than MaxMessageLength
)

func (s Status) String() string {
	return [...]string{"Ok", "BackPressured", "AdminAction", "TooBig"}[s]
}

// ErrCorrupt reports a frame that cannot be a valid frame.
var ErrCorrupt = errors.New("logbuf: corrupt frame")

// Params describes a new log buffer.
type Params struct {
	TermLength    int // power of two, MinTermLength..MaxTermLength
	MTU           int // max frame size incl. header; multiple of FrameAlign, at least 2*FrameAlign, at most TermLength/8
	InitialTermID int32
	SessionID     int32
	StreamID      int32
}

func (p Params) validate() error {
	if p.TermLength < MinTermLength || p.TermLength > MaxTermLength || p.TermLength&(p.TermLength-1) != 0 {
		return fmt.Errorf("logbuf: term length %d is not a power of two in [%d, %d]", p.TermLength, MinTermLength, MaxTermLength)
	}
	if p.MTU < 2*FrameAlign || p.MTU%FrameAlign != 0 || p.MTU > p.TermLength/8 {
		return fmt.Errorf("logbuf: MTU %d must be a multiple of %d in [%d, termLength/8]", p.MTU, FrameAlign, 2*FrameAlign)
	}
	return nil
}

// FileLength is the size of the buffer a log with this term length needs.
func FileLength(termLength int) int { return partitions*termLength + MetadataLength }

// LogBuffer is a handle on a log buffer. It is safe for concurrent use by any number
// of publishers; Poll and Scan are meant for one reader each.
type LogBuffer struct {
	terms [partitions]shm.Buffer
	meta  shm.Buffer

	termLength    int
	mtu           int
	initialTermID int32
	sessionID     int32
	streamID      int32
	termShift     uint
}

// Create initialises a freshly zeroed buffer of FileLength(p.TermLength) bytes.
func Create(buf shm.Buffer, p Params) (*LogBuffer, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	if buf.Len() != FileLength(p.TermLength) {
		return nil, fmt.Errorf("logbuf: buffer is %d bytes, want %d", buf.Len(), FileLength(p.TermLength))
	}
	l := open(buf, p.TermLength)
	l.meta.PutInt32(metaInitTerm, p.InitialTermID)
	l.meta.PutInt32(metaMTU, int32(p.MTU))
	l.meta.PutInt32(metaTermLength, int32(p.TermLength))
	l.meta.PutInt32(metaSession, p.SessionID)
	l.meta.PutInt32(metaStream, p.StreamID)
	for i := 0; i < partitions; i++ {
		l.meta.PutInt64(metaTail+i*64, int64(p.InitialTermID+int32(i))<<32)
	}
	l.meta.PutInt32(metaActive, 0)
	l.meta.PutInt32Ordered(metaMagic, magic) // published last
	l.mtu, l.initialTermID, l.sessionID, l.streamID = p.MTU, p.InitialTermID, p.SessionID, p.StreamID
	return l, nil
}

// Wrap attaches to a buffer another party created.
func Wrap(buf shm.Buffer) (*LogBuffer, error) {
	if buf.Len() < MetadataLength+partitions*MinTermLength || (buf.Len()-MetadataLength)%partitions != 0 {
		return nil, fmt.Errorf("logbuf: %d bytes is not a log buffer size", buf.Len())
	}
	termLength := (buf.Len() - MetadataLength) / partitions
	l := open(buf, termLength)
	if l.meta.Int32Volatile(metaMagic) != magic {
		return nil, errors.New("logbuf: not an initialised log buffer")
	}
	if int(l.meta.Int32(metaTermLength)) != termLength {
		return nil, errors.New("logbuf: metadata does not match the buffer size")
	}
	l.mtu = int(l.meta.Int32(metaMTU))
	l.initialTermID = l.meta.Int32(metaInitTerm)
	l.sessionID = l.meta.Int32(metaSession)
	l.streamID = l.meta.Int32(metaStream)
	return l, nil
}

func open(buf shm.Buffer, termLength int) *LogBuffer {
	l := &LogBuffer{termLength: termLength}
	for i := 0; i < partitions; i++ {
		l.terms[i] = buf.Slice(i*termLength, termLength)
	}
	l.meta = buf.Slice(partitions*termLength, MetadataLength)
	for 1<<l.termShift < termLength {
		l.termShift++
	}
	return l
}

func (l *LogBuffer) TermLength() int      { return l.termLength }
func (l *LogBuffer) MTU() int             { return l.mtu }
func (l *LogBuffer) InitialTermID() int32 { return l.initialTermID }
func (l *LogBuffer) SessionID() int32     { return l.sessionID }
func (l *LogBuffer) StreamID() int32      { return l.streamID }

// MaxPayload is the most payload one frame carries.
func (l *LogBuffer) MaxPayload() int { return l.mtu - HeaderLength }

// MaxMessageLength is the largest message Offer accepts (fragmented over several frames if needed).
func (l *LogBuffer) MaxMessageLength() int { return min(l.termLength/8, maxMessageCap) }

func align(n int) int { return (n + FrameAlign - 1) &^ (FrameAlign - 1) }

func (l *LogBuffer) partitionOf(termCount int32) int { return int(uint32(termCount) % partitions) }

// position converts (termID, termOffset) to a stream position.
func (l *LogBuffer) position(termID int32, termOffset int) int64 {
	return int64(termID-l.initialTermID)<<l.termShift + int64(termOffset)
}

func (l *LogBuffer) tailOff(partition int) int { return metaTail + partition*64 }

// Position is the producer position: where the next message will be written.
func (l *LogBuffer) Position() int64 {
	for {
		count := l.meta.Int32Volatile(metaActive)
		raw := l.meta.Int64Volatile(l.tailOff(l.partitionOf(count)))
		termID := int32(raw >> 32)
		if termID-l.initialTermID != count { // being rotated; look again
			continue
		}
		return l.position(termID, min(int(raw&0xffffffff), l.termLength))
	}
}

// framedLength is the space a message of n bytes needs, fragments and padding included.
func (l *LogBuffer) framedLength(n int) int {
	maxPayload := l.MaxPayload()
	if n <= maxPayload {
		return align(HeaderLength + n)
	}
	full, rem := n/maxPayload, n%maxPayload
	total := full * align(HeaderLength+maxPayload)
	if rem > 0 {
		total += align(HeaderLength + rem)
	}
	return total
}

// testBeforePublish, if set by a test, runs after a message is completely written and
// right before the stores that make it visible.
var testBeforePublish func()

// Offer appends msg as one message, split into frames of at most MTU bytes if needed,
// and returns the stream position just after it. It never blocks.
//
// BackPressured means the producer position has reached limit. The check is made
// before the space is claimed, so concurrent publishers can overshoot it by one
// message each. AdminAction means the end of a term was hit and the log is being
// rotated, or another publisher is mid-rotation; call again.
func (l *LogBuffer) Offer(msg []byte, limit int64) (int64, Status) {
	if len(msg) > l.MaxMessageLength() {
		return 0, TooBig
	}
	termCount := l.meta.Int32Volatile(metaActive)
	part := l.partitionOf(termCount)
	raw := l.meta.Int64Volatile(l.tailOff(part))
	termID := int32(raw >> 32)
	if termID-l.initialTermID != termCount {
		return 0, AdminAction // the tail of the new term is not set up yet
	}
	if l.position(termID, min(int(raw&0xffffffff), l.termLength)) >= limit {
		return 0, BackPressured
	}
	required := l.framedLength(len(msg))
	off, st := l.claim(termCount, part, termID, required)
	if st != Ok {
		return 0, st
	}
	l.write(part, termID, off, required, msg)
	return l.position(termID, off+required), Ok
}

// claim reserves required bytes in the partition and returns their offset. It is a
// compare-and-swap loop, not a blind add: the tail carries the term id, so a publisher
// whose view of the active term went stale (it was descheduled between looking and
// claiming, long enough for the partition to be recycled for a later term) fails the
// swap instead of corrupting the tail of a term that is not its own.
//
// If the claim runs past the end of the term, the winner pads the rest of the term,
// rotates the log and reports AdminAction; so does anyone who finds the term full.
func (l *LogBuffer) claim(termCount int32, part int, termID int32, required int) (int, Status) {
	for {
		raw := l.meta.Int64Volatile(l.tailOff(part))
		if int32(raw>>32) != termID {
			return 0, AdminAction
		}
		off := int(raw & 0xffffffff)
		if off >= l.termLength {
			l.rotate(termCount)
			return 0, AdminAction
		}
		if !l.meta.CompareAndSetInt64(l.tailOff(part), raw, raw+int64(required)) {
			continue
		}
		if off+required > l.termLength {
			l.writePadding(part, termID, off, l.termLength-off)
			l.rotate(termCount)
			return 0, AdminAction
		}
		return off, Ok
	}
}

// writePadding turns [off, off+length) of a partition into one padding frame.
func (l *LogBuffer) writePadding(part int, termID int32, off, length int) {
	t := l.terms[part]
	t.PutInt32Ordered(off+offLength, -int32(length))
	l.putHeaderFields(t, off, 0, TypePad, termID)
	t.PutInt32Ordered(off+offLength, int32(length))
}

func (l *LogBuffer) putHeaderFields(t shm.Buffer, off int, flags uint8, typ uint16, termID int32) {
	t.PutInt32(off+offVerFlType, int32(version)|int32(flags)<<8|int32(typ)<<16)
	t.PutInt32(off+offTermOff, int32(off))
	t.PutInt32(off+offSession, l.sessionID)
	t.PutInt32(off+offStream, l.streamID)
	t.PutInt32(off+offTermID, termID)
	t.PutInt64(off+24, 0)
}

// write fills the claimed space with the frames of msg and publishes them. The first
// frame gets a claim marker holding the size of the whole claim (negated), so that a
// reader or the unblocker can tell "being written, this big" from "never started".
// All frame bodies are written first; the lengths are published last, last frame
// first, so a reader scanning forward sees the whole message appear at once.
func (l *LogBuffer) write(part int, termID int32, off, required int, msg []byte) {
	t := l.terms[part]
	maxPayload := l.MaxPayload()
	t.PutInt32Ordered(off+offLength, -int32(required))
	n := max(1, (len(msg)+maxPayload-1)/maxPayload) // number of frames; every frame but the last is MTU bytes
	for i := 0; i < n; i++ {
		chunk := msg[min(len(msg), i*maxPayload):min(len(msg), (i+1)*maxPayload)]
		var flags uint8
		if i == 0 {
			flags |= FlagBegin
		}
		if i == n-1 {
			flags |= FlagEnd
		}
		pos := off + i*l.mtu
		l.putHeaderFields(t, pos, flags, TypeData, termID)
		t.PutBytes(pos+HeaderLength, chunk)
	}
	if testBeforePublish != nil {
		testBeforePublish()
	}
	for i := n - 1; i >= 0; i-- {
		payload := min(maxPayload, len(msg)-i*maxPayload)
		t.PutInt32Ordered(off+i*l.mtu+offLength, int32(HeaderLength+payload))
	}
}

// rotate makes the next term active. Only the caller that wins the compare-and-swap
// prepares the term after it: that partition held the term before the previous one,
// so it is cleaned and given a fresh tail.
func (l *LogBuffer) rotate(termCount int32) {
	if !l.meta.CompareAndSetInt32(metaActive, termCount, termCount+1) {
		return
	}
	nextNext := l.partitionOf(termCount + 2)
	l.terms[nextNext].Zero(0, l.termLength)
	termID := l.initialTermID + termCount + 2
	l.meta.PutInt64Ordered(l.tailOff(nextNext), int64(termID)<<32) // opens it to writers
}

// FrameInfo describes a frame handed to a Handler.
type FrameInfo struct {
	Flags      uint8
	SessionID  int32
	StreamID   int32
	TermID     int32
	TermOffset int
	Position   int64 // stream position just after this frame
}

// Handler receives one frame's payload. The slice is only valid during the call.
type Handler func(payload []byte, info FrameInfo)

func parseVerFlType(v int32) (flags uint8, typ uint16) {
	return uint8(v >> 8), uint16(uint32(v) >> 16)
}

// Poll reads published frames starting at position (which must be a frame boundary),
// calls h for each data frame, and returns the position after the last frame
// consumed. It reads at most fragmentLimit data frames and never crosses the end of
// the term; padding frames are skipped silently and count towards the new position.
func (l *LogBuffer) Poll(position int64, h Handler, fragmentLimit int) (int64, int, error) {
	termCount := int32(position >> l.termShift)
	termID := l.initialTermID + termCount
	t := l.terms[l.partitionOf(termCount)]
	start := int(position & int64(l.termLength-1))
	off, frags := start, 0
	for off < l.termLength && frags < fragmentLimit {
		length := int(t.Int32Volatile(off + offLength))
		if length <= 0 {
			break // not published yet
		}
		if length < HeaderLength || off+length > l.termLength {
			return position + int64(off-start), frags, fmt.Errorf("%w: length %d at offset %d", ErrCorrupt, length, off)
		}
		if t.Int32(off+offTermID) != termID {
			break // left over from an earlier use of this partition
		}
		flags, typ := parseVerFlType(t.Int32(off + offVerFlType))
		frameOff := off
		off += align(length)
		if typ == TypePad {
			continue
		}
		frags++
		h(t.Bytes(frameOff+HeaderLength, length-HeaderLength), FrameInfo{
			Flags: flags, SessionID: t.Int32(frameOff + offSession), StreamID: t.Int32(frameOff + offStream),
			TermID: termID, TermOffset: frameOff, Position: position + int64(off-start),
		})
	}
	return position + int64(off-start), frags, nil
}

// Scan is the sender's view: the published frames from position on, as one
// contiguous slice of at most maxLength bytes that never crosses the end of the
// term, ready to go on the wire as they are. advance is how far the sender's position
// moves after sending them: it is larger than len(frames) when the slice ends in a
// padding frame, of which only the header is sent (the receiver recreates the rest).
func (l *LogBuffer) Scan(position int64, maxLength int) (frames []byte, advance int) {
	termCount := int32(position >> l.termShift)
	t := l.terms[l.partitionOf(termCount)]
	start := int(position & int64(l.termLength-1))
	termID := l.initialTermID + termCount
	off := start
	for off < l.termLength {
		length := int(t.Int32Volatile(off + offLength))
		if length < HeaderLength || off+length > l.termLength || t.Int32(off+offTermID) != termID {
			break
		}
		_, typ := parseVerFlType(t.Int32(off + offVerFlType))
		if typ == TypePad {
			if off == start { // nothing before it: send just the padding header
				return t.Bytes(off, HeaderLength), align(length)
			}
			break
		}
		if off+align(length)-start > maxLength {
			break
		}
		off += align(length)
	}
	if off == start {
		return nil, 0
	}
	return t.Bytes(start, off-start), off - start
}

// Unblock repairs a hole in front of the reader at consumerPosition: space that a
// publisher claimed and then never published, because it died. The hole becomes a
// padding frame so readers move on. It also finishes a rotation that a dead publisher
// started but did not complete. It reports whether it changed anything. Call it only
// when the reader has been stuck for longer than any live publisher could take.
func (l *LogBuffer) Unblock(consumerPosition int64) bool {
	termCount := int32(consumerPosition >> l.termShift)
	termID := l.initialTermID + termCount
	part := l.partitionOf(termCount)
	t := l.terms[part]
	blocked := int(consumerPosition & int64(l.termLength-1))
	raw := l.meta.Int64Volatile(l.tailOff(part))
	if int32(raw>>32) != termID {
		return false
	}
	tailOff := int(raw & 0xffffffff)
	if tailOff >= l.termLength && blocked >= l.termLength {
		return false
	}
	limit := min(tailOff, l.termLength)
	changed := false
	if blocked < limit {
		length := t.Int32Volatile(blocked + offLength)
		switch {
		case length < 0: // marker written, frame never published: the marker says how big the claim was
			l.writePaddingOver(part, termID, blocked, -int(length))
			changed = true
		case length == 0:
			end := limit
			for i := blocked + FrameAlign; i < limit; i += FrameAlign {
				if t.Int32Volatile(i+offLength) != 0 {
					end = i
					break
				}
			}
			l.writePaddingOver(part, termID, blocked, end-blocked)
			changed = true
		}
	}
	// A term that is full but never rotated because its publisher died.
	if tailOff >= l.termLength && l.meta.Int32Volatile(metaActive) == termCount {
		l.rotate(termCount)
		changed = true
	}
	return changed
}

// writePaddingOver turns a hole into one padding frame. The length is stored last
// and ordered, so readers see either the hole or the whole padding frame.
func (l *LogBuffer) writePaddingOver(part int, termID int32, off, length int) {
	t := l.terms[part]
	l.putHeaderFields(t, off, 0, TypePad, termID)
	t.PutInt32Ordered(off+offLength, int32(length))
}
