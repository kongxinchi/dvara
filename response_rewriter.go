package dvara

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/davecgh/go-spew/spew"
	"gopkg.in/mgo.v2/bson"
)

var (
	proxyAllQueries = flag.Bool(
		"dvara.proxy-all",
		false,
		"if true all queries will be proxied and logger",
	)

	adminCollectionName = []byte("admin.$cmd\000")
	cmdCollectionSuffix = []byte(".$cmd\000")
)

// ProxyQuery proxies an OpQuery and a corresponding response.
type ProxyQuery struct {
	Log                              Logger                            `inject:""`
	IsMasterResponseRewriter         *IsMasterResponseRewriter         `inject:""`
}

// Proxy proxies an OpQuery and a corresponding response.
func (p *ProxyQuery) Proxy(
	h *messageHeader,
	client io.ReadWriter,
	server io.ReadWriter,
) error {

	// MsgHeader int32 x 4
	readLength := 16
	parts := [][]byte{h.ToWire()}

	// ZERO int32
	var flags [4]byte
	if _, err := io.ReadFull(client, flags[:]); err != nil {
		p.Log.Error(err)
		return err
	}
	readLength += 4
	parts = append(parts, flags[:])

	// fullCollectionName cstring
	fullCollectionName, err := readCString(client)
	if err != nil {
		p.Log.Error(err)
		return err
	}
	readLength += len(fullCollectionName)
	parts = append(parts, fullCollectionName)

	var rewriter responseRewriter

	if *proxyAllQueries || bytes.HasSuffix(fullCollectionName, adminCollectionName) {

		// numberToSkip + numberToReturn, int32 x 2
		var twoInt32 [8]byte
		if _, err := io.ReadFull(client, twoInt32[:]); err != nil {
			p.Log.Error(err)
			return err
		}
		readLength += len(twoInt32)
		parts = append(parts, twoInt32[:])

		// query document
		queryDoc, err := readDocument(client)
		if err != nil {
			p.Log.Error(err)
			return err
		}
		readLength += len(queryDoc)
		parts = append(parts, queryDoc)

		var q bson.D
		if err := bson.Unmarshal(queryDoc, &q); err != nil {
			p.Log.Error(err)
			return err
		}

		p.Log.Debugf(
			"buffered OpQuery for %s: \n%s",
			fullCollectionName[:len(fullCollectionName)-1],
			spew.Sdump(q),
		)

		if hasKey(q, "isMaster") {
			rewriter = p.IsMasterResponseRewriter
		}
	}

	pending := int64(h.MessageLength) - int64(readLength)
	if pending > 0 {
		lastBytes := make([]byte, pending)
		if _, err := io.ReadFull(client, lastBytes[:]); err != nil {
			p.Log.Error(err)
			return err
		}
		parts = append(parts, lastBytes[:])
	}

	_, err = server.Write(bytes.Join(parts, []byte("")))
	if err != nil {
		p.Log.Error(err)
		return err
	}

	if rewriter != nil {
		if err := rewriter.Rewrite(client, server); err != nil {
			return err
		}
		return nil
	}

	if err := copyMessage(client, server); err != nil {
		p.Log.Error(err)
		return err
	}

	return nil
}

var errRSChanged = errors.New("dvara: replset config changed")

// ProxyMapper maps real mongo addresses to their corresponding proxy
// addresses.
type ProxyMapper interface {
	Proxy(h string) (string, error)
}

type responseRewriter interface {
	Rewrite(client io.Writer, server io.Reader) error
}

type replyPrefix [20]byte

var emptyPrefix replyPrefix

// ReplyRW provides common helpers for rewriting replies from the server.
type ReplyRW struct {
	Log Logger `inject:""`
}

// ReadOne reads a 1 document response, from the server, unmarshals it into v
// and returns the various parts.
func (r *ReplyRW) ReadOne(server io.Reader, v interface{}) (*messageHeader, replyPrefix, int32, error) {
	h, err := readHeader(server)
	if err != nil {
		r.Log.Error(err)
		return nil, emptyPrefix, 0, err
	}

	if h.OpCode != OpReply {
		err := fmt.Errorf("readOneReplyDoc: expected op %s, got %s", OpReply, h.OpCode)
		return nil, emptyPrefix, 0, err
	}

	var prefix replyPrefix
	if _, err := io.ReadFull(server, prefix[:]); err != nil {
		r.Log.Error(err)
		return nil, emptyPrefix, 0, err
	}

	numDocs := getInt32(prefix[:], 16)
	if numDocs != 1 {
		err := fmt.Errorf("readOneReplyDoc: can only handle 1 result document, got: %d", numDocs)
		return nil, emptyPrefix, 0, err
	}

	rawDoc, err := readDocument(server)
	if err != nil {
		r.Log.Error(err)
		return nil, emptyPrefix, 0, err
	}

	if err := bson.Unmarshal(rawDoc, v); err != nil {
		r.Log.Error(err)
		return nil, emptyPrefix, 0, err
	}

	return h, prefix, int32(len(rawDoc)), nil
}

// WriteOne writes a rewritten response to the client.
func (r *ReplyRW) WriteOne(client io.Writer, h *messageHeader, prefix replyPrefix, oldDocLen int32, v interface{}) error {
	newDoc, err := bson.Marshal(v)
	if err != nil {
		return err
	}

	h.MessageLength = h.MessageLength - oldDocLen + int32(len(newDoc))
	parts := [][]byte{h.ToWire(), prefix[:], newDoc}
	for _, p := range parts {
		if _, err := client.Write(p); err != nil {
			return err
		}
	}

	return nil
}

type isMasterResponse struct {
	Hosts   []string `bson:"hosts,omitempty"`
	Primary string   `bson:"primary,omitempty"`
	Me      string   `bson:"me,omitempty"`
	Extra   bson.M   `bson:",inline"`
}

// IsMasterResponseRewriter rewrites the response for the "isMaster" query.
type IsMasterResponseRewriter struct {
	Log                 Logger              `inject:""`
	ProxyMapper         ProxyMapper         `inject:""`
	ReplyRW             *ReplyRW            `inject:""`
}

// Rewrite rewrites the response for the "isMaster" query.
func (r *IsMasterResponseRewriter) Rewrite(client io.Writer, server io.Reader) error {
	var err error
	var q isMasterResponse
	h, prefix, docLen, err := r.ReplyRW.ReadOne(server, &q)
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
	return r.ReplyRW.WriteOne(client, h, prefix, docLen, q)
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
