package event

import (
	"net"

	"github.com/niewidzialnyczlowiek/event/slices"
)

type PeerId = uint32

type Sender struct {
	subscriptions map[EventType][]PeerId
	appConns      map[PeerId]*SerializedConn
	EventSource   chan Event
	CtrlMsgSource chan CtrlMsg
	log           LoggerImpl
}

func NewSender(lf LoggerFactory) *Sender {
	return &Sender{
		subscriptions: map[int][]uint32{},
		appConns:      make(map[uint32]*SerializedConn),
		EventSource:   make(chan Event, 10),
		CtrlMsgSource: make(chan CtrlMsg, 10),
		log:           lf.NewLogger(),
	}
}

func (s *Sender) Run() {
	go func() {
		for e := range s.EventSource {
			s.send(&e)
		}
	}()
}

func (s *Sender) Connect(appAddr string) error {
	appConn, err := net.Dial("tcp", appAddr)
	if err != nil {
		return err
	}
	err = s.addPeer(appConn)
	if err != nil {
		return err
	}
	s.log.Infof("Connected to %s", appConn.RemoteAddr().String())
	return nil
}

func (s *Sender) addPeer(appConn net.Conn) error {
	addr := appConn.RemoteAddr()
	id, err := HashAddr(addr)
	if err != nil {
		return err
	}
	s.appConns[id] = NewSerializedConn(appConn)
	return nil
}

func (s *Sender) RegisterSub(t EventType, addr net.Addr) {
	id, err := HashAddr(addr)
	if err != nil {
		s.log.Warnf("Failed registering sub. Cannot hash address %s", addr.String())
		return
	}
	peers, ok := s.subscriptions[t]
	if !ok {
		peers = make([]PeerId, 0, 10)
	}
	if _, cont := slices.Contains(peers, id); !cont {
		peers = append(peers, id)
		s.subscriptions[t] = peers
	}
}

func (s *Sender) UnregisterSub(t EventType, addr net.Addr) {
	id, err := HashAddr(addr)
	if err != nil {
		s.log.Warnf("Cannot hash address %s", addr.String())
		return
	}
	if subs, ok := s.subscriptions[t]; ok {
		if _, cont := slices.Contains(subs, id); cont {
			s.subscriptions[t] = slices.Filter(subs, func(a PeerId) bool { return a != id })
		}
	}
}

func (s *Sender) send(e *Event) {
	// peers := s.subscriptions[e.Type]
	// for _, id := range peers {
	// 	_ = s.conns[id].Write(e)
	// }
	for i := range s.appConns {
		err := s.appConns[i].Write(e)
		if err != nil {
			s.log.Debugf("Error while sending message: %s", err.Error())
		}
	}
}
