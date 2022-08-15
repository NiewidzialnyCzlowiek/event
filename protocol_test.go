package event

import (
	"bytes"
	"encoding/gob"
	"net"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSerializedStream(t *testing.T) {
	message := "Heelo world!\n"
	server, client := net.Pipe()
	serverConn := NewSerializedConn(server)
	clientConn := NewSerializedConn(client)
	go func() {
		err := clientConn.Write(&message)
		if err != nil {
			t.Errorf("Error when sending the message: %s", err.Error())
		}
	}()

	rec := ""
	err := serverConn.Read(&rec)
	assert.NoError(t, err, "Unexpected error.")
	assert.Equal(t, message, rec, "Invalid message.")
}

// func TestSerializedSize(t *testing.T) {
// 	serializer := NewSerializer()
// 	ctrl := CtrlMsg{}
// 	e := Event{}
// 	_ = Serialize(&ctrl, &e, serializer)
// 	t.Logf("Serialized ctrl message size: %d", len(e.AppData))
// 	t.Fail()
// }

func TestSerialize(t *testing.T) {
	s := NewSerializer()
	d := testData{
		Author:  "BS",
		Message: "Hello from BS",
	}
	e := Event{Type: 1}
	err := Serialize(&d, &e, s)
	assert.NoError(t, err, "Unexpected error")
	assert.NotEmpty(t, e.AppData, "The encoded data is not present")
}

func TestDeserialize(t *testing.T) {
	s := NewSerializer()
	d := testData{
		Author:  "BS",
		Message: "Hello from BS",
	}
	buf := new(bytes.Buffer)
	gob.NewEncoder(buf).Encode(d)
	e := Event{1, buf.Bytes()}

	res := new(testData)
	err := Deserialize(e, res, s)
	assert.NoError(t, err, "Unexpected error occured")
	assert.Equal(t, d.Author, res.Author, "Deserialized data was corrupted")
	assert.Equal(t, d.Message, res.Message, "Deserialized data was corrupted")
}

type testData struct {
	Author  string
	Message string
}

func TestTypeEncodedOnlyOnce(t *testing.T) {
	s := NewSerializer()
	td := testData{
		Author:  "BS",
		Message: "Hello from BS",
	}
	e1 := Event{Type: 1}
	e2 := Event{Type: 1}

	Serialize(&td, &e1, s)
	Serialize(&td, &e2, s)

	assert.Less(t, len(e2.AppData), len(e1.AppData), "Second encoding should be shorter (type definition skipped)")
}

func TestHash(t *testing.T) {
	addrPort, err := netip.ParseAddrPort("127.0.0.1:3000")
	hash := Hash(addrPort)
	assert.NoError(t, err, "Unexpected error")
	assert.EqualValues(t, 4055534008, hash, "Incorrect hash")
}

func TestHashAddr(t *testing.T) {
	addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:3000")
	hash, err := HashAddr(addr)
	assert.NoError(t, err, "Unexpected error")
	assert.EqualValues(t, 4055534008, hash, "Incorrect hash")
}
