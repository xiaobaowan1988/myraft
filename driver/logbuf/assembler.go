package logbuf

// Assembler puts the fragments of a message back together. Hand its OnFragment to
// Poll; the wrapped handler is called once per complete message. A message whose
// first fragment was lost (for instance because the publisher died and its hole was
// unblocked) is dropped, never delivered half-way.
type Assembler struct {
	deliver Handler
	buf     []byte
	open    bool
}

func NewAssembler(deliver Handler) *Assembler { return &Assembler{deliver: deliver} }

// OnFragment is a Handler.
func (a *Assembler) OnFragment(payload []byte, info FrameInfo) {
	begin, end := info.Flags&FlagBegin != 0, info.Flags&FlagEnd != 0
	switch {
	case begin && end:
		a.open = false
		a.deliver(payload, info)
	case begin:
		a.buf = append(a.buf[:0], payload...)
		a.open = true
	case a.open:
		a.buf = append(a.buf, payload...)
		if end {
			a.open = false
			a.deliver(a.buf, info)
		}
	}
	// a middle or last fragment with no beginning: drop
}
