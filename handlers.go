package event

import (
	"errors"
)

var (
	errorAlreadyActive = errors.New("handlers have already been activated")
	errorNoSource      = errors.New("the EventSource was not set properly")
	errorNoSink        = errors.New("the EventSink was not set properly")
)

type Publisher interface {
	ToOne(e Event)
	ToAll(e Event)
}

type bufferedPublisher struct {
	events []targettedEvent
}

func (bp *bufferedPublisher) ToOne(e Event) {
	bp.events = append(bp.events, targettedEvent{
		Broadcast: false,
		Event:     e,
	})
}

func (bp *bufferedPublisher) ToAll(e Event) {
	bp.events = append(bp.events, targettedEvent{
		Broadcast: true,
		Event:     e,
	})
}

// EventHandler represents a function that is launched
// as a response to the occouring events. EventHandler
// receives an Event to handle and a Publisher. The Publisher
// interface exposes functions that allow the user
// to publish events as part of event handling.
//
// EventHandlers are registered in Handlers using the Handlers.Register() method.
type EventHandler = func(e Event, publisher Publisher)

// AppHandlers represent the algorithm in a form of event handlers.
// You can use the handlers separately and connect them to
// event source manually or use the cluster.Node type that
// automates the communication between cluster nodes.
//
// Use the AppHandlers.Register method to register event handlers.
type AppHandlers struct {
	f           map[EventType]EventHandler
	log         LoggerImpl
	EventSource chan Event
	EventSink   chan targettedEvent
	concurrent  bool
	active      bool
}

// NewAppHandlers returns empty Handlers object with the specified logger.
// The concurrent parameter specifies if the event handlers should be
// executed one by one or concurrently just after the event is received.
func NewAppHandlers(lf LoggerFactory, concurrent bool) *AppHandlers {
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
func (h *AppHandlers) Plug(l Listener, s Sender) {
	h.EventSource = l.EventSink()
	h.EventSink = s.EventSource()
}

func (hs *AppHandlers) handle(e Event) {
	h, ok := hs.f[e.Type]
	if !ok {
		hs.log.Warnf("No event handler for event %d\n", e.Type)
		return
	}
	buff := new(bufferedPublisher)
	h(e, buff)
	if len(buff.events) > 0 {
		go passToSink(buff.events, hs.EventSink)
	}
}

func passToSink(events []targettedEvent, sink chan targettedEvent) {
	for i := range events {
		sink <- events[i]
	}
}

func (hs *AppHandlers) Activate() error {
	if hs.active {
		return errorAlreadyActive
	}
	if hs.EventSource == nil {
		return errorNoSource
	}
	if hs.EventSink == nil {
		return errorNoSink
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

func (hs *AppHandlers) Unregister(t EventType) {
	delete(hs.f, t)
}
