package dvara

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/facebookgo/rpool"
	"github.com/kongxinchi/dvara/auth"
)

const headerLen = 16

var (
	errZeroMaxConnections          = errors.New("dvara: MaxConnections cannot be 0")
	errZeroMaxPerClientConnections = errors.New("dvara: MaxPerClientConnections cannot be 0")
	errNormalClose                 = errors.New("dvara: normal close")
	errClientReadTimeout           = errors.New("dvara: client read timeout")

	timeInPast = time.Now()
)

// Proxy sends stuff from clients to mongo servers.
type Proxy struct {
	Log            Logger
	ReplicaSet     *ReplicaSet
	ClientListener net.Listener // Listener for incoming client connections
	ProxyAddr      string       // Address for incoming client connections
	MongoAddr      string       // Address for destination Mongo server

	Username	   string
	Password	   string

	wg                      sync.WaitGroup
	closed                  chan struct{}
	serverPool              rpool.Pool
	maxPerClientConnections *maxPerClientConnections
}

// String representation for debugging.
func (p *Proxy) String() string {
	return fmt.Sprintf("proxy %s => mongo %s", p.ProxyAddr, p.MongoAddr)
}

// Start the proxy.
func (p *Proxy) Start() error {
	if p.ReplicaSet.MaxConnections == 0 {
		return errZeroMaxConnections
	}
	if p.ReplicaSet.MaxPerClientConnections == 0 {
		return errZeroMaxPerClientConnections
	}

	p.closed = make(chan struct{})
	p.maxPerClientConnections = newMaxPerClientConnections(p.ReplicaSet.MaxPerClientConnections)
	p.serverPool = rpool.Pool{
		New:               p.newServerConn,
		CloseErrorHandler: p.serverCloseErrorHandler,
		Max:               p.ReplicaSet.MaxConnections,
		MinIdle:           p.ReplicaSet.MinIdleConnections,
		IdleTimeout:       p.ReplicaSet.ServerIdleTimeout,
		ClosePoolSize:     p.ReplicaSet.ServerClosePoolSize,
	}

	go p.clientAcceptLoop()

	return nil
}

// Stop the proxy.
func (p *Proxy) Stop() error {
	return p.stop(false)
}

func (p *Proxy) stop(hard bool) error {
	if err := p.ClientListener.Close(); err != nil {
		return err
	}
	close(p.closed)
	if !hard {
		p.wg.Wait()
	}
	p.serverPool.Close()
	return nil
}

func (p *Proxy) AuthConn(conn net.Conn) error {
	socket := &auth.MongoSocket{
		Log: p.Log,
		Conn: conn,
		Addr: p.MongoAddr,
	}
	err := socket.Login(auth.Credential{Username: p.Username, Password: p.Password, Source: "admin"})
	if err != nil {
		return err
	}
	return nil
}

// Open up a new connection to the server. Retry 7 times, doubling the sleep
// each time. This means we'll a total of 12.75 seconds with the last wait
// being 6.4 seconds.
func (p *Proxy) newServerConn() (io.Closer, error) {
	retrySleep := 50 * time.Millisecond
	for retryCount := 7; retryCount > 0; retryCount-- {
		c, err := net.Dial("tcp", p.MongoAddr)
		if err == nil {
			if len(p.Username) == 0 {
				return c, nil
			}
			err = p.AuthConn(c)
			if err == nil {
				return c, nil
			}
		}
		p.Log.Error(err)

		time.Sleep(retrySleep)
		retrySleep = retrySleep * 2
	}
	return nil, fmt.Errorf("could not connect to %s", p.MongoAddr)
}

// getServerConn gets a server connection from the pool.
func (p *Proxy) getServerConn() (net.Conn, error) {
	c, err := p.serverPool.Acquire()
	if err != nil {
		return nil, err
	}
	return c.(net.Conn), nil
}

func (p *Proxy) serverCloseErrorHandler(err error) {
	p.Log.Error(err)
}

// proxyMessage proxies a message, possibly it's response, and possibly a
// follow up call.
func (p *Proxy) proxyMessage(
	h *messageHeader,
	client net.Conn,
	server net.Conn,
) error {

	p.Log.Debugf("proxying message %s from %s for %s", h, client.RemoteAddr(), p)
	deadline := time.Now().Add(p.ReplicaSet.MessageTimeout)
	server.SetDeadline(deadline)
	client.SetDeadline(deadline)

	// OpQuery may need to be transformed and need special handling in order to
	// make the proxy transparent.
	if h.OpCode == OpQuery {
		return p.ReplicaSet.ProxyQuery.Proxy(h, client, server)
	}

	// For other Ops we proxy the header & raw body over.
	if err := h.WriteTo(server); err != nil {
		p.Log.Error(err)
		return err
	}

	if _, err := io.CopyN(server, client, int64(h.MessageLength-headerLen)); err != nil {
		p.Log.Error(err)
		return err
	}

	// For Ops with responses we proxy the raw response message over.
	if h.OpCode.HasResponse() {
		if err := copyMessage(client, server); err != nil {
			p.Log.Error(err)
			return err
		}
	}

	return nil
}

// clientAcceptLoop accepts new clients and creates a clientServeLoop for each
// new client that connects to the proxy.
func (p *Proxy) clientAcceptLoop() {
	for {
		p.wg.Add(1)
		c, err := p.ClientListener.Accept()
		if err != nil {
			p.wg.Done()
			if strings.Contains(err.Error(), "use of closed network connection") {
				break
			}
			p.Log.Error(err)
			continue
		}
		go p.clientServeLoop(c)
	}
}

// clientServeLoop loops on a single client connected to the proxy and
// dispatches its requests.
func (p *Proxy) clientServeLoop(c net.Conn) {
	remoteIP := c.RemoteAddr().(*net.TCPAddr).IP.String()

	// enforce per-client max connection limit
	if p.maxPerClientConnections.inc(remoteIP) {
		c.Close()
		p.Log.Errorf("rejecting client connection due to max connections limit: %s", remoteIP)
		return
	}

	// turn on TCP keep-alive and set it to the recommended period of 2 minutes
	// http://docs.mongodb.org/manual/faq/diagnostics/#faq-keepalive
	if conn, ok := c.(*net.TCPConn); ok {
		conn.SetKeepAlivePeriod(2 * time.Minute)
		conn.SetKeepAlive(true)
	}

	c = teeIf(fmt.Sprintf("client %s <=> %s", c.RemoteAddr(), p), c)
	p.Log.Infof("client %s connected to %s", c.RemoteAddr(), p)
	defer func() {
		p.Log.Infof("client %s disconnected from %s", c.RemoteAddr(), p)
		p.wg.Done()
		if err := c.Close(); err != nil {
			p.Log.Error(err)
		}
		p.maxPerClientConnections.dec(remoteIP)
	}()

	for {
		h, err := p.idleClientReadHeader(c)
		if err != nil {
			if err != errNormalClose {
				p.Log.Error(err)
			}
			return
		}

		serverConn, err := p.getServerConn()
		if err != nil {
			if err != errNormalClose {
				p.Log.Error(err)
			}
			return
		}

		err = p.proxyMessage(h, c, serverConn)
		if err != nil {
			p.serverPool.Discard(serverConn)
			p.Log.Error(err)
			return
		}

		p.serverPool.Release(serverConn)
	}
}

// We wait for upto ClientIdleTimeout in MessageTimeout increments and keep
// checking if we're waiting to be closed. This ensures that at worse we
// wait for MessageTimeout when closing even when we're idling.
func (p *Proxy) idleClientReadHeader(c net.Conn) (*messageHeader, error) {
	h, err := p.clientReadHeader(c, p.ReplicaSet.ClientIdleTimeout)
	return h, err
}

func (p *Proxy) clientReadHeader(c net.Conn, timeout time.Duration) (*messageHeader, error) {
	type headerError struct {
		header *messageHeader
		error  error
	}
	resChan := make(chan headerError)

	c.SetReadDeadline(time.Now().Add(timeout))
	go func() {
		h, err := readHeader(c)
		resChan <- headerError{header: h, error: err}
	}()

	closed := false
	var response headerError

	select {
	case response = <-resChan:
		// all good
	case <-p.closed:
		closed = true
		c.SetReadDeadline(timeInPast)
		response = <-resChan
	}

	// Successfully read a header.
	if response.error == nil {
		return response.header, nil
	}

	// Client side disconnected.
	if response.error == io.EOF {
		return nil, errNormalClose
	}

	// We hit our ReadDeadline.
	if ne, ok := response.error.(net.Error); ok && ne.Timeout() {
		if closed {
			return nil, errNormalClose
		}
		return nil, errClientReadTimeout
	}

	// Some other unknown error.
	p.Log.Error(response.error)
	return nil, response.error
}

var teeIfEnable = os.Getenv("MONGOPROXY_TEE") == "1"

type teeConn struct {
	context string
	net.Conn
}

func (t teeConn) Read(b []byte) (int, error) {
	n, err := t.Conn.Read(b)
	if n > 0 {
		fmt.Fprintf(os.Stdout, "READ %s: %s %v\n", t.context, b[0:n], b[0:n])
	}
	return n, err
}

func (t teeConn) Write(b []byte) (int, error) {
	n, err := t.Conn.Write(b)
	if n > 0 {
		fmt.Fprintf(os.Stdout, "WRIT %s: %s %v\n", t.context, b[0:n], b[0:n])
	}
	return n, err
}

func teeIf(context string, c net.Conn) net.Conn {
	if teeIfEnable {
		return teeConn{
			context: context,
			Conn:    c,
		}
	}
	return c
}

type maxPerClientConnections struct {
	max    uint
	counts map[string]uint
	mutex  sync.Mutex
}

func newMaxPerClientConnections(max uint) *maxPerClientConnections {
	return &maxPerClientConnections{
		max:    max,
		counts: make(map[string]uint),
	}
}

func (m *maxPerClientConnections) inc(remoteIP string) bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	current := m.counts[remoteIP]
	if current >= m.max {
		return true
	}
	m.counts[remoteIP] = current + 1
	return false
}

func (m *maxPerClientConnections) dec(remoteIP string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	current := m.counts[remoteIP]

	// delete rather than having entries with 0 connections
	if current == 1 {
		delete(m.counts, remoteIP)
	} else {
		m.counts[remoteIP] = current - 1
	}
}
