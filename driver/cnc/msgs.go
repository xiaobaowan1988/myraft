package cnc

import (
	"encoding/binary"
	"errors"
)

// Command message types (client to driver), carried in the to-driver ring.
const (
	CmdAddPublication      int32 = 1
	CmdRemovePublication   int32 = 2
	CmdAddSubscription     int32 = 3
	CmdRemoveSubscription  int32 = 4
	CmdClientKeepalive     int32 = 5
	CmdClientClose         int32 = 6
	CmdAddExclusivePublish int32 = 7
)

// Event message types (driver to clients), carried in the broadcast buffer.
const (
	EvPublicationReady  int32 = 101
	EvOperationSuccess  int32 = 102
	EvError             int32 = 103
	EvAvailableImage    int32 = 104
	EvUnavailableImage  int32 = 105
	EvSubscriptionReady int32 = 106
)

// Error codes in EvError.
const (
	ErrCodeGeneric        int32 = 1
	ErrCodeInvalidChannel int32 = 2
	ErrCodeUnknownRef     int32 = 3
)

var errShort = errors.New("cnc: message too short")

var le = binary.LittleEndian

// Command is a decoded client command. Fields not used by a type are zero.
type Command struct {
	Type           int32
	ClientID       int64
	CorrelationID  int64
	StreamID       int32
	Channel        string
	RegistrationID int64 // for the Remove commands
}

// EncodeCommand serialises c.
func EncodeCommand(c Command) []byte {
	b := make([]byte, 16, 32+len(c.Channel))
	le.PutUint64(b[0:], uint64(c.ClientID))
	le.PutUint64(b[8:], uint64(c.CorrelationID))
	switch c.Type {
	case CmdAddPublication, CmdAddSubscription, CmdAddExclusivePublish:
		b = le.AppendUint32(b, uint32(c.StreamID))
		b = le.AppendUint32(b, uint32(len(c.Channel)))
		b = append(b, c.Channel...)
	case CmdRemovePublication, CmdRemoveSubscription:
		b = le.AppendUint64(b, uint64(c.RegistrationID))
	}
	return b
}

// DecodeCommand parses a command of the given type. It never panics on short or
// inconsistent input: a client in another process wrote it.
func DecodeCommand(typ int32, b []byte) (Command, error) {
	c := Command{Type: typ}
	if len(b) < 16 {
		return c, errShort
	}
	c.ClientID = int64(le.Uint64(b[0:]))
	c.CorrelationID = int64(le.Uint64(b[8:]))
	rest := b[16:]
	switch typ {
	case CmdAddPublication, CmdAddSubscription, CmdAddExclusivePublish:
		if len(rest) < 8 {
			return c, errShort
		}
		c.StreamID = int32(le.Uint32(rest[0:]))
		n := int(le.Uint32(rest[4:]))
		if n < 0 || len(rest)-8 < n {
			return c, errShort
		}
		c.Channel = string(rest[8 : 8+n])
	case CmdRemovePublication, CmdRemoveSubscription:
		if len(rest) < 8 {
			return c, errShort
		}
		c.RegistrationID = int64(le.Uint64(rest))
	case CmdClientKeepalive, CmdClientClose:
	default:
		return c, errors.New("cnc: unknown command type")
	}
	return c, nil
}

// Event is a decoded driver event. Fields not used by a type are zero.
type Event struct {
	Type           int32
	CorrelationID  int64 // the command it answers; for images, the image's own id
	RegistrationID int64 // publication registration, or the subscription an image belongs to
	SessionID      int32
	StreamID       int32
	CounterID      int32  // pub-lmt (publication ready) or the subscriber position (image)
	Name           string // log file name relative to the driver directory; for a subscription's OperationSuccess, the address it is bound to
	Code           int32
	Message        string
}

// EncodeEvent serialises e.
func EncodeEvent(e Event) []byte {
	b := make([]byte, 0, 64+len(e.Name)+len(e.Message))
	b = le.AppendUint64(b, uint64(e.CorrelationID))
	switch e.Type {
	case EvPublicationReady, EvAvailableImage:
		b = le.AppendUint64(b, uint64(e.RegistrationID))
		b = le.AppendUint32(b, uint32(e.SessionID))
		b = le.AppendUint32(b, uint32(e.StreamID))
		b = le.AppendUint32(b, uint32(e.CounterID))
		b = le.AppendUint32(b, uint32(len(e.Name)))
		b = append(b, e.Name...)
	case EvUnavailableImage:
		b = le.AppendUint64(b, uint64(e.RegistrationID))
		b = le.AppendUint32(b, uint32(e.StreamID))
	case EvOperationSuccess:
		b = le.AppendUint32(b, uint32(len(e.Name)))
		b = append(b, e.Name...)
	case EvError:
		b = le.AppendUint32(b, uint32(e.Code))
		b = le.AppendUint32(b, uint32(len(e.Message)))
		b = append(b, e.Message...)
	}
	return b
}

// DecodeEvent parses an event of the given type.
func DecodeEvent(typ int32, b []byte) (Event, error) {
	e := Event{Type: typ}
	if len(b) < 8 {
		return e, errShort
	}
	e.CorrelationID = int64(le.Uint64(b))
	rest := b[8:]
	switch typ {
	case EvPublicationReady, EvAvailableImage:
		if len(rest) < 24 {
			return e, errShort
		}
		e.RegistrationID = int64(le.Uint64(rest[0:]))
		e.SessionID = int32(le.Uint32(rest[8:]))
		e.StreamID = int32(le.Uint32(rest[12:]))
		e.CounterID = int32(le.Uint32(rest[16:]))
		n := int(le.Uint32(rest[20:]))
		if n < 0 || len(rest)-24 < n {
			return e, errShort
		}
		e.Name = string(rest[24 : 24+n])
	case EvUnavailableImage:
		if len(rest) < 12 {
			return e, errShort
		}
		e.RegistrationID = int64(le.Uint64(rest[0:]))
		e.StreamID = int32(le.Uint32(rest[8:]))
	case EvError:
		if len(rest) < 8 {
			return e, errShort
		}
		e.Code = int32(le.Uint32(rest[0:]))
		n := int(le.Uint32(rest[4:]))
		if n < 0 || len(rest)-8 < n {
			return e, errShort
		}
		e.Message = string(rest[8 : 8+n])
	case EvOperationSuccess:
		if len(rest) < 4 {
			return e, errShort
		}
		n := int(le.Uint32(rest))
		if n < 0 || len(rest)-4 < n {
			return e, errShort
		}
		e.Name = string(rest[4 : 4+n])
	case EvSubscriptionReady:
	default:
		return e, errors.New("cnc: unknown event type")
	}
	return e, nil
}
