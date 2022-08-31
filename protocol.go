package event

import (
	"bytes"
	"encoding"
	"encoding/gob"
	"hash/crc32"
	"io"
	"net"
	"net/netip"
)

// CtrlMsgType indicates how the message should be interpreted
// by the node. The CtrlMsgType defines the command and
// the CtrlMsg body holds the arguments for the command.
type CtrlMsgType = int

// The set of available control message types.
const (
	CtrlSubscribe CtrlMsgType = iota
	CtrlUnsubscribe
	CtrlConnectTo
	CtrlRegister
)

const CtrlRespOffset CtrlMsgType = 10000
const (
	CtrlSubscribeResp   CtrlMsgType = CtrlSubscribe + CtrlRespOffset
	CtrlUnsubscribeResp             = CtrlUnsubscribe + CtrlRespOffset
	CtrlConnectToResp               = CtrlConnectTo + CtrlRespOffset
	CtrlRegisterResp                = CtrlRegister + CtrlRespOffset
)

// Represents a service protocol message
type CtrlMsg struct {
	// SerialNumber specifies an id unique for the message
	// sender. Together with the sender address it creates
	// an unique identifier for the message.
	SerialNumber uint64
	// Type specifies the type of the message and should be
	// one of Ctrl... constants defined in this file.
	Type CtrlMsgType
	// AppPort is the application protocol port of the CtrlMsg sender.
	// It is used to calculate the peer unique identifier when required.
	AppPort uint16
	// Addr represents a node in a form of the address and port.
	// This field is used in CtrlConnectTo message to indicate where
	// the receiver node should connect to.
	Addr netip.AddrPort
	// EventType indicates what event the message refers to.
	// It is used in CtrlSubscribe and CtrlUnsubscribe messages.
	EventType EventType
}

type IdCtrlMsg struct {
	Source   net.Addr
	SourceId uint32
	Msg      CtrlMsg
}

type EventType = int
type Event struct {
	Type    EventType
	AppData []byte
}

type targettedEvent struct {
	Broadcast bool
	Event     Event
}

type SerializedConn struct {
	io.ReadWriteCloser
	enc gob.Encoder
	dec gob.Decoder
}

func NewSerializedConn(conn io.ReadWriteCloser) *SerializedConn {
	gob.Register(Event{})
	sc := SerializedConn{
		conn,
		*gob.NewEncoder(conn),
		*gob.NewDecoder(conn)}
	return &sc
}

func (c *SerializedConn) Write(e any) error {
	return c.enc.Encode(e)
}

func (c *SerializedConn) Read(e any) error {
	err := c.dec.Decode(e)
	if err != nil {
		return err
	}
	return nil
}

// Serializer allows to serialize structs into []byte
// It should be used to convert user defined structs into
// Event payloads and deserialize app data from received events.
// *Serializer is not thread safe
type Serializer struct {
	encBuf *bytes.Buffer
	decBuf *bytes.Buffer
	enc    gob.Encoder
	dec    gob.Decoder
}

func NewSerializer() *Serializer {
	ser := Serializer{
		encBuf: new(bytes.Buffer),
		decBuf: new(bytes.Buffer),
	}
	ser.enc = *gob.NewEncoder(ser.encBuf)
	ser.dec = *gob.NewDecoder(ser.decBuf)
	return &ser
}

func (s *Serializer) Register(values []any) {
	for _, v := range values {
		err := s.enc.Encode(v)
		if err != nil {
			panic(err)
		}
		data := s.encBuf.Bytes()
		s.encBuf.Reset()
		_, err = s.decBuf.Write(data)
		if err != nil {
			panic(err)
		}
		err = s.dec.Decode(v)
		if err != nil {
			panic(err)
		}
		s.decBuf.Reset()
	}
}

// Serialize fills target's appData field with the serialized data
// using the provided serializer
func Serialize[T any](data *T, target *Event, ser *Serializer) error {
	ser.encBuf.Reset()
	err := ser.enc.Encode(data)
	if err != nil {
		return err
	}
	target.AppData = make([]byte, ser.encBuf.Len())
	_, err = ser.encBuf.Read(target.AppData)
	if err != nil {
		return err
	}
	return nil
}

// Deserialize extracts the appData from data and deserializes it into target
// using the provided serializer
func Deserialize[T any](data Event, target *T, ser *Serializer) error {
	ser.decBuf.Reset()
	ser.decBuf.Write(data.AppData)
	return ser.dec.Decode(target)
}

func NewEvent[T any](t EventType, data *T, ser *Serializer) *Event {
	e := Event{Type: t}
	Serialize(data, &e, ser)
	return &e
}

func Hash(data encoding.BinaryMarshaler) PeerId {
	bytes, _ := data.MarshalBinary()
	return crc32.ChecksumIEEE(bytes[:])
}

func HashAddr(addr net.Addr) (PeerId, error) {
	ap, err := netip.ParseAddrPort(addr.String())
	if err != nil {
		return 0, err
	}
	return Hash(ap), nil
}

func HashAddrStr(addr string) (PeerId, error) {
	ap, err := netip.ParseAddrPort(addr)
	if err != nil {
		return 0, err
	}
	return Hash(ap), nil
}
