package event

import (
	"net"

	"github.com/niewidzialnyczlowiek/event/slices"
)

type PeerId = uint32

// Sender is an interface for the event publishing element
// of the event system node. It broadcasts the events
// to other nodes in the cluster.
type Sender interface {
	// EventSource returns the channel from which the sender
	// picks up events to publish
	EventSource() chan targettedEvent
	// Run activates a background worker that receives events
	// from the channel accesed by Sender.EventSource()
	// and sends them to the subscribed nodes.
	Run()
	// Stop halts the background worker and closes all
	// the established connections
	Stop()
	// Connect establishes a connection to other event system node
	// (specifically to the other node's Listener)
	Connect(addr string) error
	// RegisterSub registers a subscription for the
	// specified address. The cluster.Node structure uses the
	// register/unregister functionality to optimise the
	// number of messages passed through the network.
	RegisterSub(t EventType, addr net.Addr)
	// UnregisterSub removes the subscription on the
	// specified event. After unregistering a subscription
	// the node at addr will no longer get notified about
	// the event with type t.
	UnregisterSub(t EventType, addr net.Addr)
	GetPeers() []net.Addr
}

type TcpSender struct {
	subscriptions map[EventType][]PeerId
	appConns      map[PeerId]*SerializedConn
	peers         map[PeerId]net.Addr
	eventSource   chan targettedEvent
	log           LoggerImpl
}

func NewTcpSender(lf LoggerFactory) *TcpSender {
	return &TcpSender{
		subscriptions: map[int][]uint32{},
		appConns:      make(map[uint32]*SerializedConn),
		peers:         map[uint32]net.Addr{},
		eventSource:   make(chan targettedEvent, 10),
		log:           lf.NewLogger(),
	}
}

func (s *TcpSender) EventSource() chan targettedEvent {
	return s.eventSource
}

func (s *TcpSender) Run() {
	go func() {
		for e := range s.eventSource {
			s.send(&e)
		}
	}()
}

func (s *TcpSender) Connect(appAddr string) error {
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

func (s *TcpSender) addPeer(appConn net.Conn) error {
	addr := appConn.RemoteAddr()
	id, err := HashAddr(addr)
	if err != nil {
		return err
	}
	s.appConns[id] = NewSerializedConn(appConn)
	s.peers[id] = addr
	return nil
}

func (s *TcpSender) RegisterSub(t EventType, addr net.Addr) {
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

func (s *TcpSender) UnregisterSub(t EventType, addr net.Addr) {
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

func (s *TcpSender) Stop() {
	for sender := range s.appConns {
		s.appConns[sender].Close()
	}
}

func (s *TcpSender) GetPeers() []net.Addr {
	peersAddr := make([]net.Addr, len(s.appConns))
	i := 0
	for _, addr := range s.peers {
		peersAddr[i] = addr
		i++
	}
	return peersAddr
}

func (s *TcpSender) send(e *targettedEvent) {
	if e.Broadcast {
		for i := range s.appConns {
			go func(conn *SerializedConn) {
				err := conn.Write(e.Event)
				if err != nil {
					s.log.Debugf("Error while sending message: %s", err.Error())
				}
			}(s.appConns[i])
		}
	} else {
		var v *SerializedConn
		var ok bool
		for _, v = range s.appConns {
			ok = true
			break
		}
		if ok {
			v.Write(e.Event)
		}
	}
}
