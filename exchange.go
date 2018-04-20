package dvara

import (
	"net"
	"time"
	"github.com/kongxinchi/dvara/auth"
	"sync/atomic"
)

type EStatus int8

const (
	Idle = EStatus(0)
	Running = EStatus(1)
	Closing = EStatus(2)
)

func (e *exchanger) AuthConn(conn net.Conn) error {
	p := e.proxy

	socket := &auth.MongoSocket{
		Debug: p.Manager.Debug,
		Conn:  conn,
		Addr:  p.MongoAddr,
	}
	err := socket.Login(auth.Credential{Username: p.Username, Password: p.Password, Source: "admin"})
	if err != nil {
		return err
	}
	return nil
}

// getServerConn gets a server connection from the pool.
func (e *exchanger) newServerConn() (net.Conn, error) {
	p := e.proxy

	retrySleep := 50 * time.Millisecond

	for retryCount := 7; retryCount > 0; retryCount-- {
		c, err := net.Dial("tcp", p.MongoAddr)
		if err == nil {
			if len(p.Username) != 0 {
				if err = e.AuthConn(c); err == nil {
					p.Log.Infof("new mongo server connected %s", p.MongoAddr)
					return c, nil
					break
				}
			} else {
				p.Log.Infof("new mongo server connected %s", p.MongoAddr)
				return c, nil
				break
			}
		}

		p.Log.Error(err)
		time.Sleep(retrySleep)
		retrySleep = retrySleep * 2
	}
	return nil, errServerGoneAway
}

type exchanger struct {
	proxy       *Proxy
	status      EStatus
	index       int

	log         Logger
	server      net.Conn
	requestChan chan *request
	requestId   int32
	waiterMap   map[int32]*responseWaiter
	closed      chan int
}

type responseWaiter struct {
	response   chan *response
	responseTo int32  // original request id
}

type request struct {
	message  *requestMessage
	respChan chan *response
}

type response struct {
	message   *responseMessage
	error     error
}

func NewExchanger(proxy *Proxy, index int) *exchanger {
	e := &exchanger{
		proxy:       proxy,
		status:      Idle,
		index:       index,
		log:         proxy.Log,
		server:      nil,
		requestChan: make(chan *request),
		requestId:   0,
		waiterMap:   make(map[int32]*responseWaiter),
		closed:      make(chan int),
	}
	return e
}

func (e *exchanger) run() error {
	conn, err := e.newServerConn()
	if err != nil {
		return err
	}
	e.server = conn
	e.status = Running
	go e.loop()
	return nil
}

func (e *exchanger) closing() {
	e.status = Closing
	e.closed <- 1
}

func (e *exchanger) stop() {
	e.server.Close()
	e.server = nil
	e.status = Idle
}

func (e *exchanger) nextRequestId() int32 {
	if e.requestId >= maxRequestId {
		e.requestId = 1
	} else {
		e.requestId += 1
	}
	return e.requestId
}

func (e *exchanger) addWaiter(requestId int32, waiter *responseWaiter) {
	e.waiterMap[requestId] = waiter
	atomic.AddInt32(&e.proxy.waiterCnt, 1)
}

func (e *exchanger) deleteWaiter(requestId int32) {
	delete(e.waiterMap, requestId)
	atomic.AddInt32(&e.proxy.waiterCnt, -1)
}

func (e *exchanger) getWaiter(requestId int32) (*responseWaiter, bool){
	if waiter, ok := e.waiterMap[requestId]; ok {
		return waiter, ok
	}
	return nil, false
}

// handle loop of response from server
func (e *exchanger) loop() {

	resChan := make(chan response)
	// need listen response
	listening := false
	// wait waiter map empty if closing
	closing := false

	defer func() {
		if listening {
			e.server.SetDeadline(timeInPast)
			<- resChan
		}
		// response all waiting clients
		if len(e.waiterMap) > 0 {
			resp := &response{message:nil, error: errServerGoneAway}
			for k, waiter := range e.waiterMap {
				waiter.response <- resp
				delete(e.waiterMap, k)
			}
			atomic.AddInt32(&e.proxy.waiterCnt, -int32(len(e.waiterMap)))
		}
		e.proxy.stopped <- e
	}()


	for {

		if closing && len(e.waiterMap) == 0 {
			break
		}

		if !listening {
			go func() {
				m, err := readResponseMessage(e.server)
				resChan <- response{message: m, error: err}
			}()
			listening = true
		}


		var response response

		select {
		case request := <- e.requestChan:

			oRequestId := request.message.getHeader().RequestID
			requestId := e.nextRequestId()
			request.message.setRequestId(requestId)

			if request.respChan != nil {
				e.addWaiter(
					requestId,
					&responseWaiter{response: request.respChan, responseTo: oRequestId},
				)
			}

			_, err := e.server.Write(request.message.bytes)
			if err != nil {
				e.log.Error(err)
				break
			}

		case response = <-resChan:

			listening = false

			if response.error != nil {
				e.log.Error(response.error)
				break
			}

			h := response.message.getHeader()

			if waiter, ok := e.getWaiter(h.ResponseTo); ok {
				e.deleteWaiter(h.ResponseTo)
				response.message.setResponseTo(waiter.responseTo)
				waiter.response <- &response
			}

		case <-e.closed:
			closing = true
		}
	}
}




