package dvara

import (
	"errors"
	"fmt"
	"io"
	"bytes"
	"gopkg.in/mgo.v2/bson"
)

var (
	errWrite = errors.New("incorrect number of bytes written")
)

// Look at http://docs.mongodb.org/meta-driver/latest/legacy/mongodb-wire-protocol/ for the protocol.

// OpCode allow identifying the type of operation:
//
// http://docs.mongodb.org/meta-driver/latest/legacy/mongodb-wire-protocol/#request-opcodes
type OpCode int32

// String returns a human readable representation of the OpCode.
func (c OpCode) String() string {
	switch c {
	default:
		return "UNKNOWN"
	case OpReply:
		return "REPLY"
	case OpMessage:
		return "MESSAGE"
	case OpUpdate:
		return "UPDATE"
	case OpInsert:
		return "INSERT"
	case Reserved:
		return "RESERVED"
	case OpQuery:
		return "QUERY"
	case OpGetMore:
		return "GET_MORE"
	case OpDelete:
		return "DELETE"
	case OpKillCursors:
		return "KILL_CURSORS"
	}
}

// IsMutation tells us if the operation will mutate data. These operations can
// be followed up by a getLastErr operation.
func (c OpCode) IsMutation() bool {
	return c == OpInsert || c == OpUpdate || c == OpDelete
}

// HasResponse tells us if the operation will have a response from the server.
func (c OpCode) HasResponse() bool {
	return c == OpQuery || c == OpGetMore
}

// The full set of known request op codes:
// http://docs.mongodb.org/meta-driver/latest/legacy/mongodb-wire-protocol/#request-opcodes
const (
	OpReply       = OpCode(1)
	OpMessage     = OpCode(1000)
	OpUpdate      = OpCode(2001)
	OpInsert      = OpCode(2002)
	Reserved      = OpCode(2003)
	OpQuery       = OpCode(2004)
	OpGetMore     = OpCode(2005)
	OpDelete      = OpCode(2006)
	OpKillCursors = OpCode(2007)
)

// messageHeader is the mongo MessageHeader
type messageHeader struct {
	// MessageLength is the total message size, including this header
	MessageLength int32
	// RequestID is the identifier for this miessage
	RequestID int32
	// ResponseTo is the RequestID of the message being responded to. used in DB responses
	ResponseTo int32
	// OpCode is the request type, see consts above.
	OpCode OpCode
}

// ToWire converts the messageHeader to the wire protocol
func (m messageHeader) ToWire() []byte {
	var d [headerLen]byte
	b := d[:]
	setInt32(b, 0, m.MessageLength)
	setInt32(b, 4, m.RequestID)
	setInt32(b, 8, m.ResponseTo)
	setInt32(b, 12, int32(m.OpCode))
	return b
}

// FromWire reads the wirebytes into this object
func (m *messageHeader) FromWire(b []byte) {
	m.MessageLength = getInt32(b, 0)
	m.RequestID = getInt32(b, 4)
	m.ResponseTo = getInt32(b, 8)
	m.OpCode = OpCode(getInt32(b, 12))
}

func (m *messageHeader) WriteTo(w io.Writer) error {
	b := m.ToWire()
	n, err := w.Write(b)
	if err != nil {
		return err
	}
	if n != len(b) {
		return errWrite
	}
	return nil
}

// String returns a string representation of the message header. Useful for debugging.
func (m *messageHeader) String() string {
	return fmt.Sprintf(
		"opCode:%s (%d) msgLen:%d reqID:%d respID:%d",
		m.OpCode,
		m.OpCode,
		m.MessageLength,
		m.RequestID,
		m.ResponseTo,
	)
}

type requestMessage struct {
	header                *messageHeader
	fullCollectionNameLen int32
	bytes                 []byte
}

func (r *requestMessage) getHeader() *messageHeader {
	if r.header == nil {
		h := messageHeader{}
		h.FromWire(r.bytes[0:headerLen])
		r.header = &h
	}
	return r.header
}

func (r *requestMessage) setRequestId(rId int32) {
	setInt32(r.bytes, 4, rId)
	if r.header != nil {
		r.header.RequestID = rId
	}
}

func (r *requestMessage) fullCollectionName() []byte {
	if r.fullCollectionNameLen == 0 {
		header := r.getHeader()
		var i int32
		for i = 20; i < header.MessageLength; i ++ {
			if r.bytes[i] == x00 {
				r.fullCollectionNameLen = i + 1 - 20
				break
			}
		}
		if r.fullCollectionNameLen == 0 {
			return nil
		}
	}
	return r.bytes[20:r.fullCollectionNameLen+20]
}

func (r *requestMessage) readQueryBSON(v interface{}) error {
	if r.fullCollectionNameLen == 0 {
		r.fullCollectionName()
	}

	offset := 20 + r.fullCollectionNameLen + 8
	size := getInt32(r.bytes, int(offset))
	doc := r.bytes[offset : offset + size]

	if err := bson.Unmarshal(doc, v); err != nil {
		return err
	}
	return nil
}

type responseMessage struct {
	header                *messageHeader
	bytes                 []byte
}

func (r *responseMessage) getHeader() *messageHeader {
	if r.header == nil {
		h := messageHeader{}
		h.FromWire(r.bytes[0:headerLen])
		r.header = &h
	}
	return r.header
}

func (r *responseMessage) setResponseTo(rId int32) {
	setInt32(r.bytes, 8, rId)
	if r.header != nil {
		r.header.ResponseTo = rId
	}
}

func (r *responseMessage) readDocumentBSON(v interface{}) error {

	offset := headerLen + 20
	size := getInt32(r.bytes, int(offset))
	doc := r.bytes[offset : int32(offset) + size]

	if err := bson.Unmarshal(doc, v); err != nil {
		return err
	}
	return nil
}

func (r *responseMessage) updateDocument(v interface{}) error {

	newDoc, err := bson.Marshal(v)
	if err != nil {
		return err
	}

	newLength := headerLen + 20 + int32(len(newDoc))
	setInt32(r.bytes, 0, newLength)
	if r.header != nil {
		r.header.MessageLength = newLength
	}

	r.bytes = mergeBytes(r.bytes[0: headerLen+20], newDoc)
	return nil
}

func mergeBytes(pBytes ...[]byte) []byte {
	return bytes.Join(pBytes, []byte(""))
}

func readMessageBytes(r io.Reader) ([]byte, error) {
	var sizeRaw [4]byte
	if _, err := io.ReadFull(r, sizeRaw[:]); err != nil {
		return nil, err
	}
	size := getInt32(sizeRaw[:], 0)
	doc := make([]byte, size)
	setInt32(doc, 0, size)
	if _, err := io.ReadFull(r, doc[4:]); err != nil {
		return nil, err
	}
	return doc, nil
}

func readRequestMessage(r io.Reader) (*requestMessage, error) {
	raw, err := readMessageBytes(r)
	if err != nil {
		return nil, err
	}
	msg := requestMessage{bytes: raw}
	return &msg, nil
}

func readResponseMessage(r io.Reader) (*responseMessage, error) {
	raw, err := readMessageBytes(r)
	if err != nil {
		return nil, err
	}
	msg := responseMessage{bytes: raw}
	return &msg, nil
}

const x00 = byte(0)

// all data in the MongoDB wire protocol is little-endian.
// all the read/write functions below are little-endian.
func getInt32(b []byte, pos int) int32 {
	return (int32(b[pos+0])) |
		(int32(b[pos+1]) << 8) |
		(int32(b[pos+2]) << 16) |
		(int32(b[pos+3]) << 24)
}

func setInt32(b []byte, pos int, i int32) {
	b[pos] = byte(i)
	b[pos+1] = byte(i >> 8)
	b[pos+2] = byte(i >> 16)
	b[pos+3] = byte(i >> 24)
}
