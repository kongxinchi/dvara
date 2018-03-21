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
	"github.com/facebookgo/stats"
	"github.com/op/go-logging"
)

var hardRestart = flag.Bool(
	"hard_restart",
	true,
	"if true will drop clients on restart",
)

// Logger allows for simple text logging.
type Logger interface {
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
	Warning(args ...interface{})
	Warningf(format string, args ...interface{})
	Info(args ...interface{})
	Infof(format string, args ...interface{})
	Debug(args ...interface{})
	Debugf(format string, args ...interface{})
	IsEnabledFor(level logging.Level) bool
}

var errNoAddrsGiven = errors.New("dvara: no seed addresses given for ReplicaSet")

// ReplicaSet manages the real => proxy address mapping.
// NewReplicaSet returns the ReplicaSet given the list of seed servers. It is
// required for the seed servers to be a strict subset of the actual members if
// they are reachable. That is, if two of the addresses are members of
// different replica sets, it will be considered an error.
type ReplicaSet struct {
	Log        Logger      `inject:""`
	ProxyQuery *ProxyQuery `inject:""`

	// Stats if provided will be used to record interesting stats.
	Stats stats.Client `inject:""`

	// Maximum number of connections that will be established to each mongo node.
	MaxConnections uint

	// MinIdleConnections is the number of idle server connections we'll keep
	// around.
	MinIdleConnections uint

	// ServerIdleTimeout is the duration after which a server connection will be
	// considered idle.
	ServerIdleTimeout time.Duration

	// ServerClosePoolSize is the number of goroutines that will handle closing
	// server connections.
	ServerClosePoolSize uint

	// ClientIdleTimeout is how long until we'll consider a client connection
	// idle and disconnect and release it's resources.
	ClientIdleTimeout time.Duration

	// MaxPerClientConnections is how many client connections are allowed from a
	// single client.
	MaxPerClientConnections uint

	// GetLastErrorTimeout is how long we'll hold on to an acquired server
	// connection expecting a possibly getLastError call.
	GetLastErrorTimeout time.Duration

	// MessageTimeout is used to determine the timeout for a single message to be
	// proxied.
	MessageTimeout time.Duration

	// Name is the name of the replica set to connect to. Nodes that are not part
	// of this replica set will be ignored. If this is empty, the first replica set
	// will be used
	Name string

	// Proxy Map Configs
	ProxyConfigs []ProxyConf

	realToProxy map[string]string
	proxies     map[string]*Proxy
	restarter   *sync.Once
}

// Start starts proxies to support this ReplicaSet.
func (r *ReplicaSet) Start() error {
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
			ReplicaSet:     r,
			ClientListener: listener,
			ProxyAddr:      pc.Listen,
			MongoAddr:      pc.Server,
			Username:		pc.Username,
			Password:		pc.Password,
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

// Stop stops all the associated proxies for this ReplicaSet.
func (r *ReplicaSet) Stop() error {
	return r.stop(false)
}

func (r *ReplicaSet) stop(hard bool) error {
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
func (r *ReplicaSet) Restart() {
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

func (r *ReplicaSet) proxyAddr(l net.Listener) string {
	_, port, err := net.SplitHostPort(l.Addr().String())
	if err != nil {
		panic(err)
	}

	return fmt.Sprintf("%s:%s", r.proxyHostname(), port)
}

func (r *ReplicaSet) proxyHostname() string {
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

func (r *ReplicaSet) newListener(address string) (net.Listener, error) {
	listener, err := net.Listen("tcp", address)
	if err == nil {
		return listener, nil
	}
	return nil, fmt.Errorf("could not listen: %s", address)
}

// add a proxy/mongo mapping.
func (r *ReplicaSet) add(p *Proxy) error {
	if _, ok := r.realToProxy[p.MongoAddr]; ok {
		return fmt.Errorf("mongo %s already exists in ReplicaSet", p.MongoAddr)
	}
	r.Log.Infof("added %s", p)
	r.realToProxy[p.MongoAddr] = p.ProxyAddr
	r.proxies[p.ProxyAddr] = p
	return nil
}

// Proxy returns the corresponding proxy address for the given real mongo
// address.
func (r *ReplicaSet) Proxy(h string) (string, error) {
	p, ok := r.realToProxy[h]
	if !ok {
		return "", fmt.Errorf("mongo %s is not in ReplicaSet", h)
	}
	return p, nil
}


// ProxyMapperError occurs when a known host is being ignored and does not have
// a corresponding proxy address.
type ProxyMapperError struct {
	RealHost string
	State    ReplicaState
}

func (p *ProxyMapperError) Error() string {
	return fmt.Sprintf("error mapping host %s in state %s", p.RealHost, p.State)
}

// uniq takes a slice of strings and returns a new slice with duplicates
// removed.
func uniq(set []string) []string {
	m := make(map[string]struct{}, len(set))
	for _, s := range set {
		m[s] = struct{}{}
	}
	news := make([]string, 0, len(m))
	for s := range m {
		news = append(news, s)
	}
	return news
}
