package event

import (
	"errors"
)

var (
	errorAlreadyActive = errors.New("handlers have already been activated")
	errorNoSource      = errors.New("the EventSource was not set properly")
	errorNoSink        = errors.New("the EventSink was not set properly")
)

// Publisher allows the user to publish events inside
// event handlers.
// The events are published after the handler exits
type Publisher interface {
	// ToAll publishes the event to the network. It guarantees
	// that the order in which the events are published is the
	// preserved upon delivery.
	ToAll(e Event)
	// ToSelf publishes the event only to the node that
	// executes the event where the Publisher is used.
	ToSelf(e Event)
	// BackgroundTask asynchronously executes the task
	// and publishes the notification event to self when
	// the task finishes. The task can be any function that
	// accepts no parameters and returns no value.
	//
	// The task is ran outside the event handlers path of
	// execution and is fully asynchronous. If the task needs
	// to access any variable that the event handler accesses,
	// this action must be protected with a mutual exclusion
	// mechanism.
	BackgroundTask(finishNotification Event, task func())
}

type taskRecord struct {
	notification Event
	task         func()
}

type bufferedPublisher struct {
	events          []targettedEvent
	toSelf          []Event
	backgroundTasks []taskRecord
}

func newBufferedPublisher() *bufferedPublisher {
	return &bufferedPublisher{
		events:          []targettedEvent{},
		toSelf:          []Event{},
		backgroundTasks: []taskRecord{},
	}
}

func (bp *bufferedPublisher) ToAll(e Event) {
	bp.events = append(bp.events, targettedEvent{
		Broadcast: true,
		Event:     e,
	})
}

func (bp *bufferedPublisher) ToSelf(e Event) {
	bp.toSelf = append(bp.toSelf, e)
}

func (bp *bufferedPublisher) BackgroundTask(finishNotification Event, task func()) {
	bp.backgroundTasks = append(bp.backgroundTasks, taskRecord{
		notification: finishNotification,
		task:         task,
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
	buff := newBufferedPublisher()
	h(e, buff)
	if len(buff.events) > 0 {
		go func() {
			if len(buff.events) > 1 {
				hs.log.Warnf("Two events passed")
			}
			for i := range buff.events {
				hs.EventSink <- buff.events[i]
			}
		}()
	}
	if len(buff.toSelf) > 0 {
		go func(toSelf []Event) {
			for i := range toSelf {
				hs.EventSource <- toSelf[i]
			}
		}(buff.toSelf)
	}
	if len(buff.backgroundTasks) > 0 {
		for i := range buff.backgroundTasks {
			runTask(buff.backgroundTasks[i], hs.EventSource)
		}
	}
}

func runTask(task taskRecord, notificationSink chan Event) {
	go func() {
		task.task()
		notificationSink <- task.notification
	}()
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
