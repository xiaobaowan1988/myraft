package driver

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"syscall"
)

// PacketConn is the datagram socket the sender and receiver agents use. Neither call
// blocks, so an agent's duty cycle stays one round of work: Recv returns n == 0 when
// nothing has arrived, and Send reports sent == false when the socket's buffer is full
// (the sender then simply does not advance and tries again next cycle).
type PacketConn interface {
	Recv(buf []byte) (n int, from netip.AddrPort, err error)
	Send(b []byte, to netip.AddrPort) (sent bool, err error)
	LocalAddr() netip.AddrPort
	Close() error
}

// ListenFunc opens a datagram socket bound to addr (port 0 picks a free one).
type ListenFunc func(addr netip.AddrPort) (PacketConn, error)

// ListenUDP is the real thing: a non-blocking UDP socket.
func ListenUDP(addr netip.AddrPort) (PacketConn, error) {
	network := "udp4"
	if addr.Addr().Is6() {
		network = "udp6"
	}
	c, err := net.ListenUDP(network, net.UDPAddrFromAddrPort(addr))
	if err != nil {
		return nil, err
	}
	c.SetReadBuffer(4 << 20)
	c.SetWriteBuffer(4 << 20)
	rc, err := c.SyscallConn()
	if err != nil {
		c.Close()
		return nil, err
	}
	return &udpConn{c: c, rc: rc, local: c.LocalAddr().(*net.UDPAddr).AddrPort()}, nil
}

type udpConn struct {
	c     *net.UDPConn
	rc    syscall.RawConn
	local netip.AddrPort
}

func (u *udpConn) LocalAddr() netip.AddrPort { return u.local }
func (u *udpConn) Close() error              { return u.c.Close() }

func (u *udpConn) Recv(buf []byte) (int, netip.AddrPort, error) {
	var (
		n    int
		from syscall.Sockaddr
		serr error
	)
	err := u.rc.Read(func(fd uintptr) bool {
		n, from, serr = syscall.Recvfrom(int(fd), buf, syscall.MSG_DONTWAIT)
		return true // never wait: the caller polls
	})
	if err != nil {
		return 0, netip.AddrPort{}, err
	}
	switch {
	case serr == nil:
	case errors.Is(serr, syscall.EAGAIN), errors.Is(serr, syscall.EINTR), errors.Is(serr, syscall.ECONNREFUSED):
		return 0, netip.AddrPort{}, nil // nothing to read (a refused earlier send is not news)
	default:
		return 0, netip.AddrPort{}, serr
	}
	switch a := from.(type) {
	case *syscall.SockaddrInet4:
		return n, netip.AddrPortFrom(netip.AddrFrom4(a.Addr), uint16(a.Port)), nil
	case *syscall.SockaddrInet6:
		return n, netip.AddrPortFrom(netip.AddrFrom16(a.Addr), uint16(a.Port)), nil
	}
	return n, netip.AddrPort{}, nil
}

func (u *udpConn) Send(b []byte, to netip.AddrPort) (bool, error) {
	var sa syscall.Sockaddr
	if to.Addr().Is4() || to.Addr().Is4In6() {
		sa = &syscall.SockaddrInet4{Port: int(to.Port()), Addr: to.Addr().Unmap().As4()}
	} else {
		sa = &syscall.SockaddrInet6{Port: int(to.Port()), Addr: to.Addr().As16()}
	}
	var serr error
	err := u.rc.Write(func(fd uintptr) bool {
		serr = syscall.Sendto(int(fd), b, syscall.MSG_DONTWAIT, sa)
		return true
	})
	if err != nil {
		return false, err
	}
	switch {
	case serr == nil:
		return true, nil
	case errors.Is(serr, syscall.EAGAIN), errors.Is(serr, syscall.ENOBUFS):
		return false, nil
	case errors.Is(serr, syscall.ECONNREFUSED), errors.Is(serr, syscall.EINTR):
		return true, nil // the datagram is gone either way; UDP promises nothing
	}
	return false, serr
}

// channel kinds
const (
	chanIPC = iota
	chanUDP
)

// parseChannel understands "ipc" and "udp://<ip>:<port>".
func parseChannel(s string) (kind int, addr netip.AddrPort, err error) {
	if s == ipcChannel {
		return chanIPC, netip.AddrPort{}, nil
	}
	rest, ok := strings.CutPrefix(s, "udp://")
	if !ok {
		return 0, netip.AddrPort{}, fmt.Errorf("unsupported channel %q (want \"ipc\" or \"udp://<ip>:<port>\")", s)
	}
	addr, err = netip.ParseAddrPort(rest)
	if err != nil {
		return 0, netip.AddrPort{}, fmt.Errorf("bad channel %q: %v", s, err)
	}
	return chanUDP, addr, nil
}
