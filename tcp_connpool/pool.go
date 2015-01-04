package tcp_connpool

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

const (
	MAX_CONN_ERROR = "Maximum connections reached"
	LOG_TAG        = "[conn_pool]"
)

type Error string
type Ping func(net.Conn) error

func (e Error) Error() string {
	return string(e)
}

type debugging bool

func (d debugging) Printf(format string, args ...interface{}) {
	if d {
		log.Printf(format, args...)
	}
}

var debug debugging = false

func EnableDebug(f bool) {
	if f {
		debug = true
	} else {
		debug = false
	}
}

var ErrMaxConn = Error(MAX_CONN_ERROR)
var m *sync.Mutex = new(sync.Mutex)

type TcpConnPool struct {
	name         string
	max_conns    int
	max_idle     int
	conns        int
	dial_timeout int
	free         []net.Conn
	ping         Ping
}

func NewTcpConnPool(name string, max_conns int, max_idle int, dial_timeout int, ping Ping) *TcpConnPool {
	return &TcpConnPool{
		name:         name,
		max_conns:    max_conns,
		max_idle:     max_idle,
		dial_timeout: dial_timeout,
		ping:         ping,
	}
}

func (n *TcpConnPool) GetMaxConns() int {
	return n.max_conns
}

func (n *TcpConnPool) String() string {
	return fmt.Sprintf("<TcpConnPool name:%s conns:%d free:%d max_conns:%d max_idle:%d>",
		n.name, n.conns, len(n.free), n.max_conns, n.max_idle)
}

/**
 * n.conns means all connections
 */
func (n *TcpConnPool) Get(timeout int64) (conn net.Conn, err error) {
	m.Lock()
	defer m.Unlock()

	if n.conns >= n.max_conns && len(n.free) == 0 {
		debug.Printf("%v max conn reached, pool %v", LOG_TAG, n)
		return nil, ErrMaxConn
	}

	new_conn := false
	if len(n.free) > 0 {
		// return the first free connection in the pool
		conn = n.free[0]
		n.free = n.free[1:]
		debug.Printf("%v get exist conn %v, pool %v", LOG_TAG, conn, n)
	} else {
		conn, err = n.open()
		if err != nil {
			return nil, err
		}
		new_conn = true
		debug.Printf("%v open new conn %v, pool %v", LOG_TAG, conn, n)
	}

	if timeout > 0 {
		conn.SetReadDeadline(time.Now().Add(time.Duration(timeout) * time.Millisecond))
		conn.SetWriteDeadline(time.Now().Add(time.Duration(timeout) * time.Millisecond))
	}

	err = n.ping(conn)
	if err != nil {
		if !new_conn && n.conns > 0 {
			n.conns -= 1
		}
		conn.Close()
		return nil, err
	}
	if new_conn {
		n.conns += 1
	}

	return conn, nil
}

func (n *TcpConnPool) open() (conn net.Conn, err error) {
	_, err = net.ResolveTCPAddr("tcp", n.name)
	if err != nil {
		return nil, err
	}
	conn, err = net.DialTimeout("tcp", n.name, time.Duration(n.dial_timeout)*time.Millisecond)
	if err != nil {
		return nil, err
	}
	return conn, err
}

func (n *TcpConnPool) Close(conn net.Conn) error {
	m.Lock()
	defer m.Unlock()

	debug.Printf("%v return %v, pool %v", LOG_TAG, conn, n)
	if conn != nil {
		if len(n.free) >= n.max_idle {
			debug.Printf("%v auto close %v, pool %v", LOG_TAG, conn, n)
			conn.Close()
			n.conns -= 1
		} else {
			n.free = append(n.free, conn)
		}
	} else {
		if n.conns > 0 {
			n.conns -= 1
		}
	}
	return nil
}

func (n *TcpConnPool) Destroy() {
	m.Lock()
	defer m.Unlock()

	for _, conn := range n.free {
		if conn != nil {
			debug.Printf("%v destroy %v, pool %v", LOG_TAG, conn, n)
			conn.Close()
		}
	}
	n = nil
}
