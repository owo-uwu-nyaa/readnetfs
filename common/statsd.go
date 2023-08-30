package common

import (
	"fmt"
	"net"
	"strings"
	"time"
)

type StatsdConn struct {
	Conn         net.Conn
	statsdSocket net.Conn
}

func WrapStatsdConn(conn net.Conn, statsdSocket net.Conn) *StatsdConn {
	return &StatsdConn{Conn: conn, statsdSocket: statsdSocket}
}

func addrToLoggableIp(addr net.Addr) string {
	var ip string
	switch addr := addr.(type) {
	case *net.UDPAddr:
		ip = addr.IP.String()
	case *net.TCPAddr:
		ip = addr.IP.String()
	}

	return strings.ReplaceAll(ip, ".", "-")
}

func (s StatsdConn) Read(b []byte) (n int, err error) {
	n, err = s.Conn.Read(b)
	if err != nil {
		_, _ = fmt.Fprintf(s.statsdSocket, "net.%s.receive_errors:1|c\n", addrToLoggableIp(s.RemoteAddr()))
	} else {
		_, _ = fmt.Fprintf(s.statsdSocket, "net.%s.received:%d|c\n", addrToLoggableIp(s.RemoteAddr()), n)
	}
	return n, err
}

func (s StatsdConn) Write(b []byte) (n int, err error) {
	n, err = s.Conn.Write(b)
	if err != nil {
		_, _ = fmt.Fprintf(s.statsdSocket, "net.%s.transmit_errors:1|c\n", addrToLoggableIp(s.RemoteAddr()))
	} else {
		_, _ = fmt.Fprintf(s.statsdSocket, "net.%s.transmitted:%d|c\n", addrToLoggableIp(s.RemoteAddr()), n)
	}
	return n, err
}

func (s StatsdConn) Close() error {
	return s.Conn.Close()
}

func (s StatsdConn) LocalAddr() net.Addr {
	return s.Conn.LocalAddr()
}

func (s StatsdConn) RemoteAddr() net.Addr {
	return s.Conn.RemoteAddr()
}

func (s StatsdConn) SetDeadline(t time.Time) error {
	return s.Conn.SetDeadline(t)
}

func (s StatsdConn) SetReadDeadline(t time.Time) error {
	return s.Conn.SetReadDeadline(t)
}

func (s StatsdConn) SetWriteDeadline(t time.Time) error {
	return s.Conn.SetWriteDeadline(t)
}
