package cluster

import (
	"net"
	"net/netip"

	"github.com/niewidzialnyczlowiek/event"
	"go.uber.org/zap"
)

// Cluster consists of two types of nodes.
// SeedNodes listen to connections and spread
// the information about all cluster nodes' network addresses
type NodeRole = int

const (
	SeedNode NodeRole = iota
	NormalNode
)

// Config is a single node configuration
type Config struct {
	Role            NodeRole
	Addr            string
	ServicePort     string
	ApplicationPort string
	SeedsAddr       []string
	LogerFactory    event.LoggerFactory
}

// Node represents a single node in the event system cluster
type Node struct {
	config   Config
	listener event.Listener
	service  *ServiceIO
	seeds    []uint32
	log      *zap.SugaredLogger
	sender   *event.Sender
	handlers *event.AppHandlers
}

// Creates a new node
// A node represents a single node in the event system cluster.
// To launch the system you have to activate the node using the Run function.
func NewNode(config Config, handlers *event.AppHandlers) (*Node, error) {
	appAddr := net.JoinHostPort(config.Addr, config.ApplicationPort)
	serviceAddr := net.JoinHostPort(config.Addr, config.ServicePort)
	listenAppAddr, err := net.ResolveTCPAddr("tcp", appAddr)
	if err != nil {
		return nil, err
	}
	listenServiceAddr, err := net.ResolveTCPAddr("tcp", serviceAddr)
	if err != nil {
		return nil, err
	}
	seeds := make([]net.Addr, 0, 2)
	for i := range config.SeedsAddr {
		seed := config.SeedsAddr[i]
		sa := net.UDPAddrFromAddrPort(netip.MustParseAddrPort(seed))
		seeds = append(seeds, sa)
	}
	listener := event.NewTcpListener(listenAppAddr, config.LogerFactory)
	sender := event.NewSender(config.LogerFactory)
	handlers.Plug(listener, sender)
	node := &Node{
		config,
		listener,
		NewServiceIO(listenServiceAddr, seeds, config.LogerFactory),
		[]uint32{},
		config.LogerFactory.NewLogger(),
		sender,
		handlers,
	}
	return node, nil
}

// Activates the node and runs the algorithm provided in event handlers
// It launches goroutines to handle incomming connections
// and save them in the node data structures
// After initializing all the necessary components the node starts to connect with
// the peers. After connecting with the specified number of nodes it starts
// the algorithm by publishing event.Init to itself (to local algorithm).
// To properly initialize
func (n *Node) Run() {
	n.handlers.Activate()
	n.listener.Run()
	n.sender.Run()
	go n.runServiceLayer()
}

func (n *Node) RegisterSub(e event.EventType, from string) error {
	addr, err := net.ResolveTCPAddr("tcp", from)
	if err != nil {
		return err
	}
	n.sender.RegisterSub(e, addr)
	return nil
}

// Connects a peer to the node
func (n *Node) Connect(addr, appPort, servicePort string) error {
	err := n.sender.Connect(net.JoinHostPort(addr, appPort))
	if err != nil {
		return err
	}
	return nil
}

func (n *Node) Handle(e event.Event) {
	n.handlers.EventSource <- e
}

func (n *Node) Stop() {
	n.log.Infof("Stopping node %s\n", n.config.Addr)
	n.listener.Stop()
}

func (n *Node) runServiceLayer() {
	n.service.receiveAndPass()
	for msg := range n.service.CtrlSink {
		addr, _, _ := net.SplitHostPort(msg.Source.String())
		appAddr := hostPortToNetAddr(addr, msg.Msg.AppPort)
		switch msg.Msg.Type {
		case event.CtrlConnectTo:

		case event.CtrlRegister:
			if n.config.Role == SeedNode {
				n.registerNewPeer(appAddr, msg.Msg)
			}
		case event.CtrlSubscribe:
			n.sender.RegisterSub(msg.Msg.EventType, appAddr)
		case event.CtrlUnsubscribe:
			n.sender.UnregisterSub(msg.Msg.EventType, appAddr)
		default:
			n.log.Errorf("Unknown message type: %d", msg.Msg.Type)
		}
	}
}

func (n *Node) registerNewPeer(addr net.Addr, data event.CtrlMsg) {

}

func hostPortToNetAddr(host string, port uint16) net.Addr {
	addrPort := netip.AddrPortFrom(netip.MustParseAddr(host), port)
	return net.TCPAddrFromAddrPort(addrPort)
}