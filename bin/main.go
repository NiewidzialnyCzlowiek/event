package main

import (
	"time"

	"github.com/niewidzialnyczlowiek/event/examples"
)

func main() {
	examples.RunStaticVotingExample()
	time.Sleep(time.Second * 160)
}
