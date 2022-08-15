package event

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRegister(t *testing.T) {
	h := newHandlers()
	handl := func(e Event) []Event { t.Log(e.AppData); return nil }
	h.Register(1, handl)
	_, ok := h.f[1]
	if !ok {
		t.Errorf("Registered function is not saved in Handlers\n")
	}
}

func TestHandle(t *testing.T) {
	h := newHandlers()
	handl := func(e Event) []Event {
		return []Event{e}
	}

	ty := 10
	mess := "Hello"

	h.Register(ty, handl)
	h.Activate()

	go func() {
		h.EventSource <- Event{ty, []byte(mess)}
	}()
	res := <-h.EventSink

	assert.Equal(t, ty, res.Type, "Invalid event type")
	assert.Equal(t, mess, string(res.AppData), "Invalid event type")
}

func newHandlers() *AppHandlers {
	return &AppHandlers{
		log:         NewDefaultLoggerFactory().NewLogger(),
		EventSource: make(chan Event),
		EventSink:   make(chan Event),
		concurrent:  false,
	}
}

func TestNoSourceError(t *testing.T) {
	h := AppHandlers{EventSink: make(chan Event)}
	err := h.Activate()
	assert.EqualError(t, err, ErrorNoSource.Error())
}

func TestNoSinkError(t *testing.T) {
	h := AppHandlers{EventSource: make(chan Event)}
	err := h.Activate()
	assert.EqualError(t, err, ErrorNoSink.Error())
}

func TestAlreadyActiveError(t *testing.T) {
	h := AppHandlers{
		active:      true,
		EventSource: make(chan Event),
		EventSink:   make(chan Event)}
	err := h.Activate()
	assert.EqualError(t, err, ErrorAlreadyActive.Error())
}
