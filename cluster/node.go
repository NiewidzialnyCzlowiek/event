package cluster

import (
	"errors"
	"net"
	"net/netip"
	"strconv"

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

var (
	errCannotCreateServiceIO = errors.New("cannot instantiate ServiceIO")
)

// Config is a single node configuration
type Config struct {
	// Role specifies if this node will serve the role of a seed
	// or normal node. Seed is responsible for distributing
	// the data about new nodes in the cluster.
	Role NodeRole
	// Addr specifies the interface on which the service
	// and application listeners will be ran
	Addr string
	// ServicePort defines the port on which the service protocol
	// will send and receive messages
	ServicePort string
	// ApplicationPort defines the port on which the application
	// protocol listener will be ran
	ApplicationPort string
	// SeedsAddr defines a list of seeds addresses with their
	// service ports defined. There should be two or more
	// seed nodes in the cluster.
	SeedsAddr []string
	// LoggerFactory specifies the logger factory used to
	// instantiate loggers in the node and it's internal
	// components
	LogerFactory event.LoggerFactory
}

// Node represents a single node in the event system cluster
type Node struct {
	config   Config
	listener event.Listener
	service  *serviceIO
	seeds    []uint32
	log      *zap.SugaredLogger
	sender   event.Sender
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
	listenServiceAddr, err := net.ResolveUDPAddr("udp", serviceAddr)
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
	sender := event.NewTcpSender(config.LogerFactory)
	handlers.Plug(listener, sender)
	serviceIo := NewServiceIO(listenServiceAddr, seeds, config.LogerFactory)
	if serviceIo == nil {
		return nil, errCannotCreateServiceIO
	}
	node := &Node{
		config,
		listener,
		serviceIo,
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
// the peers.
func (n *Node) Run() {
	n.handlers.Activate()
	n.listener.Run()
	n.sender.Run()
	n.service.run()
	n.handleCtrlMessages()
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

// Handle puts the event into the AppHandlers event source.
// This method blocks the execution but only until there is
// space in the event buffer. It does not block until the
// handler is executed.
func (n *Node) Handle(e event.Event) {
	n.handlers.EventSource <- e
}

func (n *Node) Stop() {
	n.log.Infof("Stopping node %s\n", n.config.Addr)
	n.listener.Stop()
}

func (n *Node) handleCtrlMessages() {
	go func() {
		for msg := range n.service.CtrlSink {
			addr, svcPort, _ := net.SplitHostPort(msg.Source.String())
			appAddr := hostPortToNetAddr(addr, msg.Msg.AppPort)
			switch msg.Msg.Type {
			case event.CtrlConnectTo:
				err := n.Connect(addr, strconv.Itoa(int(msg.Msg.AppPort)), svcPort)
				if err != nil {
					n.log.Warnf("Cannot connect to the node at %s, %s", addr, err.Error())
				} else {
					n.log.Infof("Connected to peer at %s", addr)
				}

			case event.CtrlRegister:
				if n.config.Role == SeedNode {
					n.registerPeerInCluster(appAddr, msg)
				}

			case event.CtrlSubscribe:
				n.sender.RegisterSub(msg.Msg.EventType, appAddr)

			case event.CtrlUnsubscribe:
				n.sender.UnregisterSub(msg.Msg.EventType, appAddr)

			default:
				n.log.Errorf("Unknown message type: %d", msg.Msg.Type)
			}
		}
	}()
}

func (n *Node) registerPeerInCluster(appAddr net.Addr, msg event.IdCtrlMsg) {
	n.service.AddPeer(appAddr.String(), msg.Source.String())
	peers := n.sender.GetPeers()
	for _, peer := range peers {
		n.service.send(msg.Source, event.CtrlMsg{
			Type: event.CtrlConnectTo,
			Addr: netip.MustParseAddrPort(peer.String()),
		})
	}
	n.service.broadcast(event.CtrlMsg{
		Type: event.CtrlConnectTo,
		Addr: netip.MustParseAddrPort(appAddr.String()),
	})
}

func hostPortToNetAddr(host string, port uint16) net.Addr {
	addrPort := netip.AddrPortFrom(netip.MustParseAddr(host), port)
	return net.TCPAddrFromAddrPort(addrPort)
}
