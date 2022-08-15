package main

import (
	"fmt"
	"time"

	"github.com/niewidzialnyczlowiek/event"
	"github.com/niewidzialnyczlowiek/event/cluster"
)

const (
	EventHello = iota
	EventGoodbye
	EventInit
)

type eventHelloData struct {
	Name     string
	LastName string
	Age      uint8
}

type eventGoodbyeData struct {
	Gb string
}

func setupNode(sp, ap string) *cluster.Node {
	commonSer := event.NewSerializer()
	lf := event.NewDefaultLoggerFactory()
	l := lf.NewLogger()
	h := event.NewHandlers(lf, false)
	h.Register(EventInit, func(e event.Event) []event.Event {
		hello := eventHelloData{
			Name:     "Bob",
			LastName: "Marley",
			Age:      50,
		}
		resp := event.Event{}
		err := event.Serialize(&hello, &resp, commonSer)
		if err != nil {
			l.Infof("Cannot serialize message: %s", err.Error())
		}
		return []event.Event{resp}
	})

	h.Register(EventHello, func(e event.Event) []event.Event {
		var m eventHelloData
		err := event.Deserialize(e, &m, commonSer)
		if err != nil {
			l.Infof("Error while deserializing message %s", err)
			return []event.Event{}
		}
		l.Infof("Received HelloEvent with name: %s %s and age %d", m.Name, m.LastName, m.Age)
		resp := event.Event{Type: EventGoodbye}
		d := eventGoodbyeData{"Goodbye"}
		err = event.Serialize(&d, &resp, commonSer)
		if err != nil {
			l.Infof("Error while serializing message %s", err)
			return []event.Event{}
		}
		return []event.Event{resp}
	})

	h.Register(EventGoodbye, func(e event.Event) []event.Event {
		var m eventGoodbyeData
		err := event.Deserialize(e, &m, commonSer)
		if err != nil {
			l.Infof("Error while deserializing message %s", err.Error())
			return []event.Event{}
		}
		l.Infof("Received EventGoodbye with message %s", m.Gb)
		return []event.Event{}
	})

	config := cluster.Config{
		Role:            cluster.NormalNode,
		Addr:            "127.0.0.1",
		ServicePort:     sp,
		ApplicationPort: ap,
		SeedsAddr:       []string{"127.0.0.1:4000"},
		LogerFactory:    event.NewDefaultLoggerFactory(),
	}
	n, err := cluster.NewNode(config, h)
	if err != nil {
		l.Infof("Cannot create node: %s", err.Error())
		return nil
	}
	return n
}

func main() {
	n1 := setupNode("3000", "3001")
	n2 := setupNode("4000", "4001")
	n1.Run()
	n2.Run()
	time.Sleep(time.Second * time.Duration(1))
	err := n1.Connect("127.0.0.1", "4001", "4000")
	if err != nil {
		fmt.Printf("Connection error: %s", err.Error())
	}
	err = n2.Connect("127.0.0.1", "3001", "3000")
	if err != nil {
		fmt.Printf("Connection error: %s", err.Error())
	}
	time.Sleep(time.Second * time.Duration(2))
	n1.Handle(event.Event{
		Type:    EventInit,
		AppData: []byte{},
	})
	time.Sleep(time.Second * time.Duration(300))
	// n1.Stop()
	// n2.Stop()
}
