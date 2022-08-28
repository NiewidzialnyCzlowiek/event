package cluster

import (
	"bytes"
	"encoding/gob"
	"errors"
	"net"
	"net/netip"
	"time"

	"github.com/niewidzialnyczlowiek/event"
	"github.com/niewidzialnyczlowiek/event/slices"
	"go.uber.org/zap"
)

var (
	errCannotReceiveSvc = errors.New("cannot read ctrl message from service socket")
)

type ctrlMsgToSend struct {
	rec net.Addr
	msg event.CtrlMsg
}

type serviceIO struct {
	listener    net.PacketConn
	sendBuf     bytes.Buffer
	recBuf      bytes.Buffer
	enc         *gob.Encoder
	dec         *gob.Decoder
	peers       map[event.PeerId]net.Addr
	CtrlSink    chan event.IdCtrlMsg
	sendQueue   chan ctrlMsgToSend
	msgToResend []ctrlMsgToSend
	respSink    chan event.IdCtrlMsg
	msgSize     int
	log         *zap.SugaredLogger
}

func NewServiceIO(addr net.Addr, seeds []net.Addr, lf event.LoggerFactory) *serviceIO {
	log := lf.NewLogger()
	l, err := net.ListenPacket("udp", addr.String())
	if err != nil {
		log.Warnf("Cannot launch service listener on %s", addr.String())
		return nil
	}
	pm := map[uint32]net.Addr{}
	selfId, _ := event.HashAddr(l.LocalAddr())
	pm[selfId] = addr
	for i := range seeds {
		hash, err := event.HashAddr(seeds[i])
		if err != nil {
			continue
		}
		pm[hash] = seeds[i]
	}
	recB := new(bytes.Buffer)
	sndB := new(bytes.Buffer)
	gob.NewEncoder(sndB).Encode(event.CtrlMsg{})
	msgSize := sndB.Len()
	sndB.Reset()
	return &serviceIO{
		listener:    l,
		sendBuf:     *sndB,
		recBuf:      *recB,
		enc:         gob.NewEncoder(sndB),
		dec:         gob.NewDecoder(recB),
		peers:       pm,
		CtrlSink:    make(chan event.IdCtrlMsg, 10),
		sendQueue:   make(chan ctrlMsgToSend, 50),
		msgToResend: []ctrlMsgToSend{},
		respSink:    make(chan event.IdCtrlMsg),
		msgSize:     msgSize,
		log:         log,
	}
}

func (sio *serviceIO) run() {
	sio.receiveAndPass()
	sio.resendMessages()
	sio.runSender()
}

func (sc *serviceIO) receiveAndPass() {
	go func() {
		for {
			msg, err := sc.receive()
			if err != nil {
				sc.log.Warnf("Error when receiving ctrl message: %s", err.Error())
			} else {
				if msg.Msg.Type < event.CtrlRespOffset {
					sc.reply(*msg)
					sc.CtrlSink <- *msg
				} else {
					sc.msgToResend = slices.Filter(sc.msgToResend, func(c ctrlMsgToSend) bool {
						return c.msg.SerialNumber != msg.Msg.SerialNumber || (c.msg.Type+event.CtrlRespOffset) != msg.Msg.Type || c.rec.String() != msg.Source.String()
					})
				}
			}
		}
	}()
}

func (sio *serviceIO) resendMessages() {
	go func() {
		for _, cmts := range sio.msgToResend {
			sio.sendQueue <- cmts
		}
		time.Sleep(time.Second * time.Duration(5))
	}()
}

func (sio *serviceIO) runSender() {
	buf := bytes.NewBuffer(make([]byte, 0, sio.msgSize))
	var serial uint64 = 1
	go func() {
		for toSend := range sio.sendQueue {
			if toSend.msg.SerialNumber == 0 {
				toSend.msg.SerialNumber = serial
				serial += 1
			}
			buf.Reset()
			enc := gob.NewEncoder(buf)
			enc.Encode(toSend.msg)
			n, err := sio.listener.WriteTo(buf.Next(sio.msgSize), toSend.rec)
			if n != sio.msgSize && n != 0 {
				sio.log.Warnf("Failed to write all the message bytes. Wanted to write %d but written %d", sio.msgSize, n)
			}
			if err != nil {
				sio.log.Errorf("Error while writing service message: %s", err.Error())
			}
			sio.msgToResend = append(sio.msgToResend, toSend)
		}
	}()
}

func (sio *serviceIO) broadcast(msg event.CtrlMsg) {
	for _, a := range sio.peers {
		sio.send(a, msg)
	}
}

func (sio *serviceIO) send(addr net.Addr, msg event.CtrlMsg) {
	go func() {
		m := ctrlMsgToSend{
			rec: addr,
			msg: msg,
		}
		sio.sendQueue <- m
	}()
}

func (sio *serviceIO) reply(msg event.IdCtrlMsg) {
	replyTo := msg.Source
	resp := event.CtrlMsg{
		SerialNumber: msg.Msg.SerialNumber,
		Type:         msg.Msg.Type + event.CtrlRespOffset,
	}
	sio.send(replyTo, resp)
}

func (sio *serviceIO) receive() (*event.IdCtrlMsg, error) {
	buf := make([]byte, sio.msgSize)
	n, from, err := sio.listener.ReadFrom(buf)
	if err != nil {
		return nil, err
	}
	if n != sio.msgSize {
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
		Source:   from,
		SourceId: id,
		Msg:      ctrl,
	}, nil
}

func (sio *serviceIO) AddPeer(appAddr, serviceAddr string) {
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
