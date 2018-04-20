package dvara

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"
	"sync/atomic"
)

const headerLen = 16

var (
	errZeroMaxPerClientConnections = errors.New("dvara: MaxPerClientConnections cannot be 0")
	errNormalClose                 = errors.New("dvara: normal close")
	errClientReadTimeout           = errors.New("dvara: client read timeout")
	errServerGoneAway              = errors.New("dvara: mongo server gone away")
	errExchangerAcquire            = errors.New("dvara: exchanger acquire failed")

	timeInPast = time.Now()
	maxRequestId = int32(1024 * 1024)
)

// ProxyQuery sends stuff from clients to mongo servers.
type Proxy struct {
	Log            Logger
	Manager        *ProxyManager
	ClientListener net.Listener // Listener for incoming client connections
	ProxyAddr      string       // Address for incoming client connections
	MongoAddr      string       // Address for destination Mongo server
	Username       string
	Password       string

	wg     sync.WaitGroup
	closed chan struct{}

	maxPerClientConnections *maxPerClientConnections
	maxServerConnections    int32
	maxResponseWait         int32

	acquire chan chan *exchanger
	stopped chan *exchanger

	exchangers []*exchanger
	running    []*exchanger
	waiterCnt  int32
	runningCnt int32
}

// String representation for debugging.
func (p *Proxy) String() string {
	return fmt.Sprintf("proxy %s => mongo %s", p.ProxyAddr, p.MongoAddr)
}

// Start the proxy.
func (p *Proxy) Start() error {
	if p.Manager.MaxPerClientConnections == 0 {
		return errZeroMaxPerClientConnections
	}

	p.closed = make(chan struct{})
	p.maxPerClientConnections = newMaxPerClientConnections(p.Manager.MaxPerClientConnections)
	p.maxServerConnections = p.Manager.MaxServerConnections
	p.maxResponseWait = p.Manager.MaxResponseWait

	p.acquire = make(chan chan *exchanger)
	p.stopped = make(chan *exchanger)
	p.exchangers = make([]*exchanger, 16)

	go p.manage()
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
	return nil
}

// proxyMessage proxies a message, possibly it's response, and possibly a
// follow up call.
func (p *Proxy) proxyMessage(m *requestMessage, client net.Conn) error {

	h := m.getHeader()
	p.Log.Debugf("proxying message %s from %s for %s", h, client.RemoteAddr(), p)

	r := make(chan *exchanger)
	p.acquire <- r
	exchanger := <-r
	if exchanger == nil {
		return errExchangerAcquire
	}

	if !h.OpCode.HasResponse() {
		exchanger.requestChan <- &request{message: m, respChan: nil}
		return nil
	}

	res := make(chan *response)
	exchanger.requestChan <- &request{message: m, respChan: res}
	response := <-res

	if response.error != nil {
		return response.error
	}

	rewriter, err := p.getRewriter(m)
	if err != nil {
		return err
	}
	if rewriter != nil {
		if err = rewriter.Rewrite(response.message); err != nil {
			return err
		}
	}

	deadline := time.Now().Add(p.Manager.MessageTimeout)
	client.SetDeadline(deadline)

	_, err = client.Write(response.message.bytes)
	if err != nil {
		return err
	}
	return nil
}


// add or reduce server connections by current waiter count
func (p *Proxy) balance(runningCnt int32, waiterCnt int32) int32 {

	needServer := (waiterCnt * 2)/p.maxResponseWait + 1
	if needServer > p.maxServerConnections {
		needServer = p.maxServerConnections
	}

	if needServer > runningCnt {
		p.Log.Infof("%d resp waiting, balance exchanger %d => %d", waiterCnt, runningCnt, needServer)
		for i, exc := range p.exchangers {
			if exc == nil {
				exc = NewExchanger(p, i)
				p.exchangers[i] = exc
			}

			if exc.status == Idle {
				if err := exc.run(); err != nil {
					p.Log.Error(err)
					continue
				}
				p.running = append(p.running, exc)
				runningCnt += 1
				if needServer <= runningCnt {
					break
				}
			}
		}
		atomic.StoreInt32(&p.runningCnt, runningCnt)
	}

	if needServer < runningCnt {
		p.Log.Infof("%d resp waiting, balance exchanger %d => %d", waiterCnt, runningCnt, needServer)
		var k int
		for j, e := range p.running {
			e.closing()
			runningCnt -= 1
			k = j
			if needServer >= runningCnt {
				break
			}
		}
		p.running = p.running[k+1:]
		atomic.StoreInt32(&p.runningCnt, runningCnt)
	}

	return runningCnt
}

func (p *Proxy) removeRunning(exc *exchanger) {
	for i, item := range p.running {
		if item.index == exc.index {
			p.running = append(p.running[:i], p.running[i+1:]...)
			break
		}
	}
	atomic.StoreInt32(&p.runningCnt, int32(len(p.running)))
}

func (p *Proxy) manage() {

	var index int32

	maxWaitCnt := int32(p.maxResponseWait * p.maxServerConnections)

	for {

		select {

		case exchangerChan := <-p.acquire:

			runningCnt := atomic.LoadInt32(&p.runningCnt)
			waiterCnt := atomic.LoadInt32(&p.waiterCnt)
			if waiterCnt >= maxWaitCnt {
				exchangerChan <- nil
				continue
			}

			runningCnt = p.balance(runningCnt, waiterCnt)

			index = index % runningCnt
			exchangerChan <- p.running[index]
			index ++

		case exchanger := <- p.stopped:
			s := exchanger.status
			exchanger.stop()
			if s == Running {
				p.removeRunning(exchanger)
			}
		}
	}
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
		m, err := p.idleClientReadMessage(c)
		if err != nil {
			if err != errNormalClose {
				p.Log.Error(err)
			}
			return
		}

		err = p.proxyMessage(m, c)
		if err != nil {
			p.Log.Error(err)
			return
		}
	}
}


func (p *Proxy) idleClientReadMessage(c net.Conn) (*requestMessage, error) {
	msg, err := p.clientReadMessage(c, p.Manager.ClientIdleTimeout)
	if err != nil {
		return nil, err
	}
	return msg, err
}

func (p *Proxy) clientReadMessage(c net.Conn, timeout time.Duration) (*requestMessage, error) {
	type requestError struct {
		message *requestMessage
		error  error
	}
	resChan := make(chan requestError)

	c.SetReadDeadline(time.Now().Add(timeout))
	go func() {
		message, err := readRequestMessage(c)
		resChan <- requestError{message: message, error: err}
	}()

	closed := false
	var response requestError

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
		return response.message, nil
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

func (m *maxPerClientConnections) count() int {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	res := 0
	for _, n := range m.counts {
		res += int(n)
	}
	return res
}
