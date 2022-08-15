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

type CtrlMsgType = int
type CtrlMsg struct {
	Type      CtrlMsgType
	AppPort   uint16
	Addr      netip.AddrPort
	EventType EventType
}

const (
	CtrlSubscribe CtrlMsgType = iota
	CtrlUnsubscribe
	CtrlConnectTo
	CtrlRegister
)

type IdCtrlMsg struct {
	Source net.Addr
	Msg    CtrlMsg
}

type EventType = int
type Event struct {
	Type    EventType
	AppData []byte
}

// type SerializedConn interface {
// 	Write(any) error
// 	Read(any) error
// 	Close() error
// }

type SerializedConn struct {
	io.ReadWriteCloser
	enc gob.Encoder
	dec gob.Decoder
}

func NewSerializedConn(conn io.ReadWriteCloser) *SerializedConn {
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

// Serialize fills target's appData field with the serialized data
// using the provided serializer
func Serialize[T any](data *T, target *Event, ser *Serializer) error {
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
