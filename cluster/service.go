package cluster

import (
	"bytes"
	"encoding/gob"
	"errors"
	"net"
	"net/netip"

	"github.com/niewidzialnyczlowiek/event"
	"go.uber.org/zap"
)

var (
	errCannotReceiveSvc = errors.New("cannot deserialize ctrl message")
)

type ServiceIO struct {
	listener net.PacketConn
	sendBuf  bytes.Buffer
	recBuf   bytes.Buffer
	enc      *gob.Encoder
	dec      *gob.Decoder
	peers    map[event.PeerId]net.Addr
	CtrlSink chan event.IdCtrlMsg
	msgSize  int
	log      *zap.SugaredLogger
}

func NewServiceIO(addr net.Addr, seeds []net.Addr, lf event.LoggerFactory) *ServiceIO {
	l, err := net.ListenPacket("udp", addr.String())
	if err != nil {
		return nil
	}
	pm := map[uint32]net.Addr{}
	selfId, _ := event.HashAddr(l.LocalAddr())
	pm[selfId] = l.LocalAddr()
	for i := range seeds {
		peer := seeds[i]
		hash, err := event.HashAddr(peer)
		if err != nil {
			continue
		}
		pm[hash] = peer
	}
	recB := new(bytes.Buffer)
	sndB := new(bytes.Buffer)
	gob.NewEncoder(sndB).Encode(event.CtrlMsg{})
	msgSize := sndB.Len()
	sndB.Reset()
	return &ServiceIO{
		listener: l,
		sendBuf:  *sndB,
		recBuf:   *recB,
		enc:      gob.NewEncoder(sndB),
		dec:      gob.NewDecoder(recB),
		peers:    pm,
		CtrlSink: make(chan event.IdCtrlMsg, 10),
		msgSize:  msgSize,
		log:      lf.NewLogger(),
	}
}

func (sc *ServiceIO) receiveAndPass() {
	go func() {
		for {
			msg, err := sc.receive()
			if err != nil {
				sc.log.Warnf("Error when receiving ctrl message: %s", err.Error())
			}
			sc.CtrlSink <- *msg
		}
	}()
}

func (sio *ServiceIO) receive() (*event.IdCtrlMsg, error) {
	buf := make([]byte, 0, 128)
	n, from, err := sio.listener.ReadFrom(buf)
	if n != sio.msgSize || err != nil {
		return nil, errCannotReceiveSvc
	}
	bb := bytes.NewBuffer(buf)
	ctrl := event.CtrlMsg{}
	err = gob.NewDecoder(bb).Decode(&ctrl)
	if err != nil {
		return nil, err
	}
	fromAddr, _, _ := net.SplitHostPort(from.String())
	appAddr := netip.MustParseAddr(fromAddr)
	id := event.Hash(netip.AddrPortFrom(appAddr, ctrl.AppPort))
	_, peerRegistered := sio.peers[id]
	if !peerRegistered {
		sio.peers[id] = from
		sio.log.Debugf("Received message from a new peer. Registered peer with id %d and addr %s", id, from)
	}
	return &event.IdCtrlMsg{
		Source: from,
		Msg:    ctrl,
	}, nil
}

func (sio *ServiceIO) AddPeer(appAddr, serviceAddr string) {
	id, err := event.HashAddrStr(appAddr)
	if err != nil {
		sio.log.Warnf("Cannot hash peer address %s: %s", appAddr, err.Error())
		return
	}
	udpAddr, err := net.ResolveUDPAddr("udp", serviceAddr)
	if err != nil {
		sio.log.Warnf("Cannot resolve udp address %s: %s", serviceAddr, err.Error())
		return
	}
	sio.peers[id] = udpAddr
	sio.log.Debugf("Added new service connetion with id %d to %s", id, udpAddr.String())
}
