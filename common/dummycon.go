package common

import (
	"net"
	"time"
)

type DummyConn struct {
}

func (d DummyConn) Read(b []byte) (n int, err error) {
	return 0, nil
}

func (d DummyConn) Write(b []byte) (n int, err error) {
	return 0, nil
}

func (d DummyConn) Close() error {
	return nil
}

func (d DummyConn) LocalAddr() net.Addr {
	return nil
}

func (d DummyConn) RemoteAddr() net.Addr {
	return nil
}

func (d DummyConn) SetDeadline(t time.Time) error {
	return nil
}

func (d DummyConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (d DummyConn) SetWriteDeadline(t time.Time) error {
	return nil
}
