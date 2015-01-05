package conn_pool

import (
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
)

const (
	MAX_CONN_ERROR = "Maximum connections reached"
	LOG_TAG        = "[conn_pool]"
)

var (
	debug      debugging   = false
	ErrMaxConn             = errors.New(MAX_CONN_ERROR)
	m          *sync.Mutex = new(sync.Mutex)
)

type debugging bool

func (t debugging) Printf(format string, args ...interface{}) {
	if t {
		log.Printf(format, args...)
	}
}

func EnableDebug(f bool) {
	if f {
		debug = true
	} else {
		debug = false
	}
}

// ConnPool manages the life cycle of connections
type ConnPool struct {
	// New is used to create a new connection when necessary.
	New func() (io.Closer, error)

	// Ping is use to check the conn fetched from pool
	Ping func(io.Closer) error

	Name     string
	MaxConns int
	MaxIdle  int

	conns int
	free  []io.Closer
}

func NewConnPool(name string, max_conns int, max_idle int) *ConnPool {
	return &ConnPool{
		Name:     name,
		MaxConns: max_conns,
		MaxIdle:  max_idle,
	}
}

func (p *ConnPool) Get() (conn io.Closer, err error) {
	m.Lock()
	defer m.Unlock()

	if p.conns >= p.MaxConns && len(p.free) == 0 {
		debug.Printf("%v max conn reached, pool %v", LOG_TAG, p)
		return nil, ErrMaxConn
	}

	new_conn := false
	if len(p.free) > 0 {
		// return the first free connection in the pool
		conn = p.free[0]
		p.free = p.free[1:]
	} else {
		conn, err = p.New()
		if err != nil {
			return nil, err
		}
		new_conn = true
	}

	err = p.Ping(conn)
	if err != nil {
		if !new_conn && p.conns > 0 {
			p.conns -= 1
		}
		conn.Close()
		return nil, err
	}
	if new_conn {
		p.conns += 1
		debug.Printf("%v open new conn %v, pool %v", LOG_TAG, conn, p)
	} else {
		debug.Printf("%v get exist conn %v, pool %v", LOG_TAG, conn, p)
	}

	return conn, nil
}

func (p *ConnPool) Close(conn io.Closer) error {
	m.Lock()
	defer m.Unlock()

	if conn != nil {
		if len(p.free) >= p.MaxIdle {
			debug.Printf("%v auto close %v, pool %v", LOG_TAG, conn, p)
			conn.Close()
			p.conns -= 1
		} else {
			p.free = append(p.free, conn)
		}
	} else {
		if p.conns > 0 {
			p.conns -= 1
		}
	}
	debug.Printf("%v return %v, pool %v", LOG_TAG, conn, p)
	return nil
}

func (p *ConnPool) Destroy() {
	m.Lock()
	defer m.Unlock()

	for _, conn := range p.free {
		if conn != nil {
			debug.Printf("%v destroy %v, pool %v", LOG_TAG, conn, p)
			conn.Close()
		}
	}
	p = nil
}

func (p *ConnPool) String() string {
	return fmt.Sprintf("<TcpConnPool Name:%s conns:%d free:%d MaxConns:%d MaxIdle:%d>",
		p.Name, p.conns, len(p.free), p.MaxConns, p.MaxIdle)
}
