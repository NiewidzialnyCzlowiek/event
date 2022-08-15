package event

import (
	"errors"
)

var (
	ErrorAlreadyActive = errors.New("handlers have already been activated")
	ErrorNoSource      = errors.New("the EventSource was not set properly")
	ErrorNoSink        = errors.New("the EventSink was not set properly")
)

// EventHandler represents a function that is launched as a response
// to the occouring events. EventHandler receives an Event and returns
// the []Event. The output events are then published to the peers in the cluster.
//
// EventHandlers are registered in Handlers using the Handlers.Register() method.
//
type EventHandler = func(e Event) []Event

// AppHandlers represent the algorithm in a form of event handlers.
// You can use the handlers separately and connect them to
// event source manually or use the cluster.Node type that
// automates the communication between cluster nodes.
//
// Use the Handler.Register method to register event handlers.
//
// You must provide handlers for the event.Init and event.PoisonPill.
// This events represent the initialization and the finalization
// of the algorithm. The event.Init event is published only to the
// local algorithm by the platform. The event.PoisonPill has to be
// published by the user by returing it from an EventHandler.
type AppHandlers struct {
	f           map[EventType]EventHandler
	log         LoggerImpl
	EventSource chan Event
	EventSink   chan Event
	concurrent  bool
	active      bool
}

// NewHandlers returns empty Handlers object with the specified logger.
// The concurrent parameter specifies if the event handlers should be
// executed one by one or concurrently just after the event is received.
func NewHandlers(lf LoggerFactory, concurrent bool) *AppHandlers {
	return &AppHandlers{
		f:           make(map[int]EventHandler),
		log:         lf.NewLogger(),
		EventSource: nil,
		EventSink:   nil,
		concurrent:  concurrent,
		active:      false,
	}
}

// Plug connects the handlers to events passed from the Listener
// and passes the result of event handler invocations to the Sender
func (h *AppHandlers) Plug(l Listener, s *Sender) {
	h.EventSource = l.EventSink()
	h.EventSink = s.EventSource
}

func (hs *AppHandlers) handle(e Event) {
	h, ok := hs.f[e.Type]
	if !ok {
		hs.log.Warnf("No event handler for event %d\n", e.Type)
		return
	}
	res := h(e)
	go passToSink(res, hs.EventSink)
}

func passToSink(events []Event, sink chan Event) {
	for i := range events {
		sink <- events[i]
	}
}

func (hs *AppHandlers) Activate() error {
	if hs.active {
		return ErrorAlreadyActive
	}
	if hs.EventSource == nil {
		return ErrorNoSource
	}
	if hs.EventSink == nil {
		return ErrorNoSink
	}
	go func() {
		hs.log.Infof("Activating event handlers")
		for e := range hs.EventSource {
			hs.log.Debugf("Received event to handle with type %d", e.Type)
			if hs.concurrent {
				go hs.handle(e)
			} else {
				hs.handle(e)
			}
		}
		hs.log.Infof("Stopping event handlers")
	}()
	hs.active = true
	return nil
}

func (hs *AppHandlers) Register(t EventType, h EventHandler) {
	if hs.f == nil {
		hs.f = make(map[EventType]EventHandler)
	}
	hs.f[t] = h
}
