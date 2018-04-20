package dvara

import (
	"bytes"
	"strings"

	"github.com/davecgh/go-spew/spew"
	"gopkg.in/mgo.v2/bson"
)

var (
	adminCollectionName = []byte("admin.$cmd\000")
	//cmdCollectionSuffix = []byte(".$cmd\000")
)

func (p *Proxy) getRewriter(m *requestMessage) (responseRewriter, error){

	// OpQuery may need to be transformed and need special handling in order to
	// make the proxy transparent.
	if m.getHeader().OpCode != OpQuery {
		return nil, nil
	}

	fullCollectionName := m.fullCollectionName()

	var rewriter responseRewriter

	if p.Manager.Debug || bytes.HasSuffix(fullCollectionName, adminCollectionName) {

		var q bson.D
		if err := m.readQueryBSON(&q); err != nil {
			p.Log.Error(err)
			return nil, err
		}

		if p.Manager.Debug {
			p.Log.Debugf(
				"buffered OpQuery for %s: \n%s",
				fullCollectionName[:len(fullCollectionName)-1],
				spew.Sdump(q),
			)
		}

		if hasKey(q, "isMaster") {
			rewriter = p.Manager.IsMasterResponseRewriter
		}
	}

	return rewriter, nil
}

// ProxyMapper maps real mongo addresses to their corresponding proxy
// addresses.
type ProxyMapper interface {
	Proxy(h string) (string, error)
}

type responseRewriter interface {
	Rewrite(m *responseMessage) error
}

type isMasterResponse struct {
	Arbiters []string `bson:"arbiters,omitempty"`
	Hosts    []string `bson:"hosts,omitempty"`
	Primary  string   `bson:"primary,omitempty"`
	Me       string   `bson:"me,omitempty"`
	Extra    bson.M   `bson:",inline"`
}

// IsMasterResponseRewriter rewrites the response for the "isMaster" query.
type IsMasterResponseRewriter struct {
	Log                 Logger              `inject:""`
	ProxyMapper         ProxyMapper         `inject:""`
}

func (r *IsMasterResponseRewriter) Rewrite(m *responseMessage) error {
	var q isMasterResponse

	err := m.readDocumentBSON(&q)
	if err != nil {
		return err
	}

	var newHosts []string
	for _, h := range q.Hosts {
		newH, err := r.ProxyMapper.Proxy(h)
		if err != nil {
			return err
		}
		newHosts = append(newHosts, newH)
	}
	q.Hosts = newHosts

	var newArbiters []string
	for _, h := range q.Arbiters {
		newH, err := r.ProxyMapper.Proxy(h)
		if err != nil {
			return err
		}
		newArbiters = append(newArbiters, newH)
	}
	q.Arbiters = newArbiters

	if q.Primary != "" {
		// failure in mapping the primary is fatal
		if q.Primary, err = r.ProxyMapper.Proxy(q.Primary); err != nil {
			return err
		}
	}
	if q.Me != "" {
		// failure in mapping me is fatal
		if q.Me, err = r.ProxyMapper.Proxy(q.Me); err != nil {
			return err
		}
	}
	if err = m.updateDocument(q); err != nil {
		return err
	}
	return nil
}

// case insensitive check for the specified key name in the top level.
func hasKey(d bson.D, k string) bool {
	for _, v := range d {
		if strings.EqualFold(v.Name, k) {
			return true
		}
	}
	return false
}
