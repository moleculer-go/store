package mocks

import (
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/payload"
	. "github.com/onsi/gomega"
)

type Adapter interface {
	Connect() error
	Insert(params moleculer.Payload) moleculer.Payload
	RemoveAll() moleculer.Payload
}

func ConnectAndLoadUsers(adapter Adapter) (moleculer.Payload, moleculer.Payload, moleculer.Payload) {
	err := adapter.Connect()
	if err != nil {
		panic(err)
	}
	adapter.RemoveAll()
	johnSnow := adapter.Insert(payload.New(map[string]interface{}{
		"name":     "John",
		"lastname": "Snow",
		"age":      25,
	}))
	Expect(johnSnow.IsError()).Should(BeFalse())
	marie := adapter.Insert(payload.New(map[string]interface{}{
		"name":     "Marie",
		"lastname": "Claire",
		"age":      75,
		"master":   johnSnow.Get("id").String(),
	}))
	Expect(marie.IsError()).Should(BeFalse())

	johnTravolta := adapter.Insert(payload.New(map[string]interface{}{
		"name":     "John",
		"lastname": "Travolta",
		"age":      65,
		"master":   johnSnow.Get("id").String(),
		"friends":  []string{johnSnow.Get("id").String(), marie.Get("id").String()},
	}))

	adapter.Insert(payload.New(map[string]interface{}{
		"name":     "Julian",
		"lastname": "Assange",
		"age":      46,
	}))

	adapter.Insert(payload.New(map[string]interface{}{
		"name":     "Peter",
		"lastname": "Pan",
		"age":      13,
	}))

	adapter.Insert(payload.New(map[string]interface{}{
		"name":     "Stone",
		"lastname": "Man",
		"age":      13,
	}))

	Expect(johnTravolta.IsError()).Should(BeFalse())
	return johnSnow, marie, johnTravolta
}
