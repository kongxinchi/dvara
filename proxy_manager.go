package dvara

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"
	"github.com/facebookgo/stackerr"
)

var hardRestart = flag.Bool(
	"hard_restart",
	true,
	"if true will drop clients on restart",
)

var errNoAddrsGiven = errors.New("dvara: no seed addresses given for ProxyManager")

// ProxyManager manages the real => proxy address mapping.
// NewReplicaSet returns the ProxyManager given the list of seed servers. It is
// required for the seed servers to be a strict subset of the actual members if
// they are reachable. That is, if two of the addresses are members of
// different replica sets, it will be considered an error.
type ProxyManager struct {
	Debug      bool

	Log Logger `inject:""`

	IsMasterResponseRewriter *IsMasterResponseRewriter `inject:""`

	// MessageTimeout is used to determine the timeout for a single message to be
	// proxied.
	MessageTimeout time.Duration

	// ClientIdleTimeout is how long until we'll consider a client connection
	// idle and disconnect and release it's resources.
	ClientIdleTimeout time.Duration

	// MaxPerClientConnections is how many client connections are allowed from a
	// single client.
	MaxPerClientConnections uint

	// MaxServerConnections is how many server connections are allowed
	MaxServerConnections    int32

	// MaxResponseWait is how many waiter per exchanger
	MaxResponseWait         int32

	ProxyConfigs []ProxyConf

	realToProxy map[string]string
	proxies     map[string]*Proxy
	restarter   *sync.Once
}

// Start starts proxies to support this ProxyManager.
func (r *ProxyManager) Start() error {

	r.realToProxy = make(map[string]string)
	r.proxies = make(map[string]*Proxy)

	if len(r.ProxyConfigs) == 0 {
		return errNoAddrsGiven
	}

	r.restarter = new(sync.Once)

	for _, pc := range r.ProxyConfigs {
		listener, err := r.newListener(pc.Listen)
		if err != nil {
			return err
		}

		p := &Proxy{
			Log:            r.Log,
			Manager:        r,
			ClientListener: listener,
			ProxyAddr:      pc.Listen,
			MongoAddr:      pc.Server,
			Username:       pc.Username,
			Password:       pc.Password,
		}

		if err := r.add(p); err != nil {
			return err
		}
	}

	var wg sync.WaitGroup
	wg.Add(len(r.proxies))
	errch := make(chan error, len(r.proxies))
	for _, p := range r.proxies {
		go func(p *Proxy) {
			defer wg.Done()
			if err := p.Start(); err != nil {
				r.Log.Error(err)
				errch <- stackerr.Wrap(err)
			}
		}(p)
	}
	wg.Wait()

	select {
	default:
		return nil
	case err := <-errch:
		return err
	}
}

// Stop stops all the associated proxies for this ProxyManager.
func (r *ProxyManager) Stop() error {
	return r.stop(false)
}

func (r *ProxyManager) stop(hard bool) error {
	var wg sync.WaitGroup
	wg.Add(len(r.proxies))
	errch := make(chan error, len(r.proxies))
	for _, p := range r.proxies {
		go func(p *Proxy) {
			defer wg.Done()
			if err := p.stop(hard); err != nil {
				r.Log.Error(err)
				errch <- stackerr.Wrap(err)
			}
		}(p)
	}
	wg.Wait()
	select {
	default:
		return nil
	case err := <-errch:
		return err
	}
}

// Restart stops all the proxies and restarts them. This is used when we detect
// an RS config change, like when an election happens.
func (r *ProxyManager) Restart() {
	r.restarter.Do(func() {
		r.Log.Info("restart triggered")
		if err := r.stop(*hardRestart); err != nil {
			// We log and ignore this hoping for a successful start anyways.
			r.Log.Errorf("stop failed for restart: %s", err)
		} else {
			r.Log.Info("successfully stopped for restart")
		}

		if err := r.Start(); err != nil {
			// We panic here because we can't repair from here and are pretty much
			// fucked.
			panic(fmt.Errorf("start failed for restart: %s", err))
		}

		r.Log.Info("successfully restarted")
	})
}

func (r *ProxyManager) proxyAddr(l net.Listener) string {
	_, port, err := net.SplitHostPort(l.Addr().String())
	if err != nil {
		panic(err)
	}

	return fmt.Sprintf("%s:%s", r.proxyHostname(), port)
}

func (r *ProxyManager) proxyHostname() string {
	const home = "127.0.0.1"

	hostname, err := os.Hostname()
	if err != nil {
		r.Log.Error(err)
		return home
	}

	// The follow logic ensures that the hostname resolves to a local address.
	// If it doesn't we don't use it since it probably wont work anyways.
	hostnameAddrs, err := net.LookupHost(hostname)
	if err != nil {
		r.Log.Error(err)
		return home
	}

	interfaceAddrs, err := net.InterfaceAddrs()
	if err != nil {
		r.Log.Error(err)
		return home
	}

	for _, ia := range interfaceAddrs {
		sa := ia.String()
		for _, ha := range hostnameAddrs {
			// check for an exact match or a match ignoring the suffix bits
			if sa == ha || strings.HasPrefix(sa, ha+"/") {
				return hostname
			}
		}
	}
	r.Log.Warningf("hostname %s doesn't resolve to the current host", hostname)
	return home
}

func (r *ProxyManager) newListener(address string) (net.Listener, error) {
	listener, err := net.Listen("tcp", address)
	if err == nil {
		return listener, nil
	}
	return nil, fmt.Errorf("could not listen: %s", address)
}

// add a proxy/mongo mapping.
func (r *ProxyManager) add(p *Proxy) error {
	if _, ok := r.realToProxy[p.MongoAddr]; ok {
		return fmt.Errorf("mongo %s already exists in ProxyManager", p.MongoAddr)
	}
	r.Log.Infof("added %s", p)
	r.realToProxy[p.MongoAddr] = p.ProxyAddr
	r.proxies[p.ProxyAddr] = p
	return nil
}

// ProxyQuery returns the corresponding proxy address for the given real mongo
// address.
func (r *ProxyManager) Proxy(h string) (string, error) {
	p, ok := r.realToProxy[h]
	if !ok {
		return "", fmt.Errorf("mongo %s is not in ProxyManager", h)
	}
	return p, nil
}
