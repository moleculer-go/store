package store

import "github.com/moleculer-go/moleculer"

//NotDefinedAdapter throws at all methods saying that "Adapter Not Defined!"
type NotDefinedAdapter struct {
}

var msg = "Moleculer DB adapter not defined!"

func (adapter *NotDefinedAdapter) Connect() error {
	panic(msg)
}
func (adapter *NotDefinedAdapter) Disconnect() error {
	panic(msg)
}
func (adapter *NotDefinedAdapter) Find(params moleculer.Payload) moleculer.Payload {
	panic(msg)
}

func (adapter *NotDefinedAdapter) FindOne(params moleculer.Payload) moleculer.Payload {
	panic(msg)
}
func (adapter *NotDefinedAdapter) FindById(params moleculer.Payload) moleculer.Payload {
	panic(msg)
}
func (adapter *NotDefinedAdapter) FindByIds(params moleculer.Payload) moleculer.Payload {
	panic(msg)
}
func (adapter *NotDefinedAdapter) Count(params moleculer.Payload) moleculer.Payload {
	panic(msg)
}
func (adapter *NotDefinedAdapter) Insert(params moleculer.Payload) moleculer.Payload {
	panic(msg)
}
func (adapter *NotDefinedAdapter) Update(params moleculer.Payload) moleculer.Payload {
	panic(msg)
}
func (adapter *NotDefinedAdapter) UpdateById(params moleculer.Payload) moleculer.Payload {
	panic(msg)
}
func (adapter *NotDefinedAdapter) RemoveById(params moleculer.Payload) moleculer.Payload {
	panic(msg)
}
