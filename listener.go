package event

import (
	"net"

	"go.uber.org/zap"
)

type Listener interface {
	EventSink() chan Event
	Run()
	Stop()
}

type TcpListener struct {
	appAddr     net.Addr
	listener    net.Listener
	appConns    map[PeerId]*SerializedConn
	appConnSink chan net.Conn
	eventSink   chan Event
	log         LoggerImpl
}

func NewTcpListener(appAddr net.Addr, lf LoggerFactory) *TcpListener {
	return &TcpListener{
		appAddr:     appAddr,
		listener:    nil,
		appConns:    make(map[uint32]*SerializedConn),
		appConnSink: make(chan net.Conn),
		eventSink:   make(chan Event, 10),
		log:         lf.NewLogger(),
	}
}

func (l *TcpListener) EventSink() chan Event {
	return l.eventSink
}

func (l *TcpListener) Stop() {
	if l.listener != nil {
		l.listener.Close()
	}
}

func (l *TcpListener) Run() {
	l.acceptNewConns()
	l.handleNewConns()
}

func (l *TcpListener) acceptNewConns() {
	runListener(l.appAddr, l.appConnSink, l.log)
}

func runListener(addr net.Addr, connSink chan net.Conn, log *zap.SugaredLogger) {
	appListener, err := net.Listen("tcp", addr.String())
	if err != nil {
		log.Errorf("Cannot start app listener on %s: %s", addr.String(), err.Error())
		return
	}
	go func() {
		for {
			appConn, err := appListener.Accept()
			if err != nil {
				log.Errorf("Stopping app listener: %s", err.Error())
				return
			}
			pushConn(appConn, connSink)
		}
	}()
}

func (l *TcpListener) handleNewConns() {
	saveAndRunConn(l.appConnSink, l.appConns, l.eventSink, l.log)
}

func saveAndRunConn[T any](connSource chan net.Conn, connMap map[PeerId]*SerializedConn, msgSink chan T, log *zap.SugaredLogger) {
	go func() {
		for conn := range connSource {
			pid, err := HashAddr(conn.RemoteAddr())
			if err != nil {
				continue
			}
			_, knowsPeer := connMap[pid]
			if knowsPeer {
				_ = connMap[pid].Close()
			}
			connMap[pid] = NewSerializedConn(conn)
			go readAndPass(connMap[pid], msgSink, log)
		}
	}()
}

func readAndPass[T any](conn *SerializedConn, sink chan T, log *zap.SugaredLogger) {
	for {
		e := new(T)
		err := conn.Read(&e)
		if err != nil {
			log.Warnf("Cannot decode message: %s. Stopping listener", err.Error())
			return
		}
		sink <- *e
	}
}

func pushConn(conn net.Conn, sink chan<- net.Conn) {
	go func() {
		sink <- conn
	}()
}
