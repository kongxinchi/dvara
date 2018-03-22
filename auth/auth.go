// mgo - MongoDB driver for Go
//
// Copyright (c) 2010-2012 - Gustavo Niemeyer <gustavo@niemeyer.net>
//
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// 1. Redistributions of source code must retain the above copyright notice, this
//    list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright notice,
//    this list of conditions and the following disclaimer in the documentation
//    and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
// ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
// (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
// LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
// ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package auth

import (
	"crypto/md5"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"gopkg.in/mgo.v2/bson"
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
}

// Credential holds details to authenticate with a MongoDB server.
type Credential struct {
	// Username and Password hold the basic details for authentication.
	// Password is optional with some authentication mechanisms.
	Username string
	Password string

	// Source is the database used to establish credentials and privileges
	// with a MongoDB server. Defaults to the default database provided
	// during dial, or "admin" if that was unset.
	Source string

	// Service defines the service name to use when authenticating with the GSSAPI
	// mechanism. Defaults to "mongodb".
	Service string

	// ServiceHost defines which hostname to use when authenticating
	// with the GSSAPI mechanism. If not specified, defaults to the MongoDB
	// server's address.
	ServiceHost string

	// Mechanism defines the protocol for credential negotiation.
	// Defaults to "MONGODB-CR".
	Mechanism string
}

func saslNewScram(cred Credential) *saslScram {
	credsum := md5.New()
	credsum.Write([]byte(cred.Username + ":mongo:" + cred.Password))
	client := NewClient(sha1.New, cred.Username, hex.EncodeToString(credsum.Sum(nil)))
	return &saslScram{cred: cred, client: client}
}

type saslScram struct {
	cred   Credential
	client *Client
}

func (s *saslScram) Close() {}

func (s *saslScram) Step(serverData []byte) (clientData []byte, done bool, err error) {
	more := s.client.Step(serverData)
	return s.client.Out(), !more, s.client.Err()
}


type saslCmd struct {
	Start          int    `bson:"saslStart,omitempty"`
	Continue       int    `bson:"saslContinue,omitempty"`
	ConversationId int    `bson:"conversationId,omitempty"`
	Mechanism      string `bson:"mechanism,omitempty"`
	Payload        []byte
}

type saslResult struct {
	Ok    bool `bson:"ok"`
	NotOk bool `bson:"code"` // Server <= 2.3.2 returns ok=1 & code>0 on errors (WTF?)
	Done  bool

	ConversationId int `bson:"conversationId"`
	Payload        []byte
	ErrMsg         string
}

func (socket *MongoSocket) Login(cred Credential) error {
	if cred.Mechanism == "" {
		cred.Mechanism = "SCRAM-SHA-1"
	}
	return socket.loginSASL(cred)
}

func (socket *MongoSocket) loginSASL(cred Credential) error {
	sasl := saslNewScram(cred)
	defer sasl.Close()

	start := 1
	cmd := saslCmd{}
	res := saslResult{}
	for {
		payload, done, err := sasl.Step(res.Payload)
		if err != nil {
			return err
		}
		if done && res.Done {
			break
		}

		cmd = saslCmd{
			Start:          start,
			Continue:       1 - start,
			ConversationId: res.ConversationId,
			Mechanism:      cred.Mechanism,
			Payload:        payload,
		}
		start = 0
		err = socket.loginRun(cred.Source, &cmd, &res, func() error {
			// See the comment on lock for why this is necessary.
			if !res.Ok || res.NotOk {
				return fmt.Errorf("server returned error on SASL authentication step: %s", res.ErrMsg)
			}
			return nil
		})
		if err != nil {
			return err
		}
		if done && res.Done {
			break
		}
	}

	return nil
}

func (socket *MongoSocket) loginRun(db string, query, result interface{}, f func() error) error {
	var replyErr error

	op := queryOp{}
	op.query = query
	op.collection = db + ".$cmd"
	op.limit = -1
	op.replyFunc = func(err error, reply *replyOp, docNum int, docData []byte) {

		if err != nil {
			replyErr = err
			return
		}

		err = bson.Unmarshal(docData, result)
		if err != nil {
			replyErr = err
		} else {
			// Must handle this within the read loop for the socket, so
			// that concurrent login requests are properly ordered.
			replyErr = f()
		}
	}

	err := socket.Query(&op)
	if err != nil {
		return err
	}
	return replyErr
}