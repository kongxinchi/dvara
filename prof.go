package dvara

import (
	"net/http"
	"net/http/pprof"
	"net"
	"strings"
)

func (r *ProxyManager) HttpProfListen() {

	m := http.NewServeMux()

	m.Handle("/debug/pprof", debugHandler(pprof.Index))
	m.Handle("/debug/pprof/cmdline", debugHandler(pprof.Cmdline))
	m.Handle("/debug/pprof/profile", debugHandler(pprof.Profile))
	m.Handle("/debug/pprof/symbol", debugHandler(pprof.Symbol))
	m.Handle("/debug/pprof/trace", debugHandler(pprof.Trace))

	http.ListenAndServe(":8000", nil)
}

func debugHandler(handlerFunc http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		host, _, _ := net.SplitHostPort(r.RemoteAddr)
		if host != "127.0.0.1" && !strings.HasPrefix(host, "10.0.") && host != "43.227.252.50" {
			w.WriteHeader(http.StatusForbidden)
			return
		}

		handlerFunc(w, r)
	}
}