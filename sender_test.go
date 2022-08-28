package event

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRegisterSub(t *testing.T) {
	sender := NewTcpSender(NewDefaultLoggerFactory())
	addr := new(TestAddr)
	sender.RegisterSub(1, addr)
	peers, ok := sender.subscriptions[1]
	assert.True(t, ok)
	assert.Equal(t, 1, len(peers), "There should be one registered peer")
}

func TestUnregisterSub(t *testing.T) {
	sender := NewTcpSender(NewDefaultLoggerFactory())
	addr := new(TestAddr)
	id, _ := HashAddr(addr)
	sender.subscriptions[1] = []PeerId{10, id, 30}
	sender.UnregisterSub(1, addr)
	peers, ok := sender.subscriptions[1]
	assert.True(t, ok)
	assert.ElementsMatch(t, []PeerId{10, 30}, peers)
}

type TestAddr struct{}

func (*TestAddr) Network() string {
	return "tcp"
}

func (*TestAddr) String() string {
	return "127.0.0.1:3000"
}
