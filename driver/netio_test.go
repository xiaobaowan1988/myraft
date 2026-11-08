package driver

import (
	"net/netip"
	"testing"
	"time"
)

func loopback(t *testing.T) PacketConn {
	t.Helper()
	c, err := ListenUDP(netip.MustParseAddrPort("127.0.0.1:0"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestUDPConnNeverBlocksAndDeliversWhole(t *testing.T) {
	a, b := loopback(t), loopback(t)
	buf := make([]byte, 65536)
	if n, _, err := a.Recv(buf); n != 0 || err != nil {
		t.Fatalf("Recv on an idle socket gave %d, %v", n, err)
	}
	for _, size := range []int{1, 100, 1408, 60000} {
		msg := make([]byte, size)
		for i := range msg {
			msg[i] = byte(i + size)
		}
		if ok, err := a.Send(msg, b.LocalAddr()); !ok || err != nil {
			t.Fatalf("Send of %d bytes: %v, %v", size, ok, err)
		}
		deadline := time.Now().Add(2 * time.Second)
		for {
			n, from, err := b.Recv(buf)
			if err != nil {
				t.Fatal(err)
			}
			if n > 0 {
				if n != size || from != a.LocalAddr() && from.Port() != a.LocalAddr().Port() {
					t.Fatalf("received %d bytes from %v, want %d from %v", n, from, size, a.LocalAddr())
				}
				for i := 0; i < n; i++ {
					if buf[i] != byte(i+size) {
						t.Fatalf("byte %d of a %d byte datagram is wrong", i, size)
					}
				}
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("a %d byte datagram never arrived", size)
			}
			time.Sleep(100 * time.Microsecond)
		}
	}
}

func TestSendToAClosedPortIsNotAnError(t *testing.T) {
	a := loopback(t)
	b := loopback(t)
	dead := b.LocalAddr()
	b.Close()
	for i := 0; i < 20; i++ { // ICMP "port unreachable" may come back; it must not surface as an error
		if _, err := a.Send([]byte("x"), dead); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
		a.Recv(make([]byte, 100))
		time.Sleep(time.Millisecond)
	}
}

func TestClosedConnReportsAnError(t *testing.T) {
	a := loopback(t)
	a.Close()
	if _, _, err := a.Recv(make([]byte, 10)); err == nil {
		t.Fatal("Recv on a closed socket succeeded")
	}
	if _, err := a.Send([]byte("x"), netip.MustParseAddrPort("127.0.0.1:9")); err == nil {
		t.Fatal("Send on a closed socket succeeded")
	}
}

func TestParseChannel(t *testing.T) {
	if k, _, err := parseChannel("ipc"); k != chanIPC || err != nil {
		t.Fatal("ipc")
	}
	k, a, err := parseChannel("udp://127.0.0.1:4000")
	if err != nil || k != chanUDP || a.String() != "127.0.0.1:4000" {
		t.Fatalf("udp: %v %v %v", k, a, err)
	}
	if _, a, err := parseChannel("udp://[::1]:9"); err != nil || a.Port() != 9 {
		t.Fatalf("ipv6: %v %v", a, err)
	}
	for _, bad := range []string{"", "tcp://1.2.3.4:5", "udp://localhost:5", "udp://1.2.3.4", "udp://1.2.3.4:99999", "ipc:x"} {
		if _, _, err := parseChannel(bad); err == nil {
			t.Errorf("%q was accepted", bad)
		}
	}
}

func TestQueue(t *testing.T) {
	q := newQueue()
	var order []int
	q.post(func() { order = append(order, 1) })
	q.post(func() { order = append(order, 2) })
	if q.drain() != 2 || len(order) != 2 || order[0] != 1 || order[1] != 2 {
		t.Fatalf("got %v", order)
	}
	if q.drain() != 0 {
		t.Fatal("drain of an empty queue ran something")
	}
	for i := 0; i < cap(q); i++ {
		if !q.post(func() {}) {
			t.Fatal("post failed before the queue was full")
		}
	}
	if q.post(func() {}) {
		t.Fatal("post succeeded on a full queue")
	}
}
