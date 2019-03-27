package db_test

import (
	"github.com/hashicorp/go-memdb"
	"github.com/moleculer-go/moleculer"
	db "github.com/moleculer-go/moleculer-db"
	"github.com/moleculer-go/moleculer-db/mocks"
	"github.com/moleculer-go/moleculer/broker"
	"github.com/moleculer-go/moleculer/transit/memory"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
)

var logLevel = "Error"

var userDbSchema = &memdb.DBSchema{
	Tables: map[string]*memdb.TableSchema{
		"user": &memdb.TableSchema{
			Name: "user",
			Indexes: map[string]*memdb.IndexSchema{
				"id": &memdb.IndexSchema{
					Name:    "id",
					Unique:  true,
					Indexer: &db.PayloadIndex{Field: "id"},
				},
				"name": &memdb.IndexSchema{
					Name:    "name",
					Unique:  false,
					Indexer: &db.PayloadIndex{Field: "name"},
				},
				"all": &memdb.IndexSchema{
					Name:    "all",
					Unique:  false,
					Indexer: &db.PayloadIndex{Field: "all"},
				},
			},
		},
	},
}

var _ = Describe("Moleculer DB Integration Tests", func() {

	Describe("populates", func() {
		var johnSnow, maria, johnT moleculer.Payload
		mem := &memory.SharedMemory{}
		bkr := broker.New(&moleculer.Config{
			DiscoverNodeID: func() string { return "node_populates" },
			LogLevel:       logLevel,
			TransporterFactory: func() interface{} {
				transport := memory.Create(log.WithField("transport", "memory"), mem)
				return &transport
			},
		})
		adapter := &db.MemoryAdapter{
			Table:  "user",
			Schema: userDbSchema,
		}
		userService := moleculer.Service{
			Name:   "user",
			Mixins: []moleculer.Mixin{db.Service(adapter)},
		}

		BeforeSuite(func() {
			bkr.AddService(userService)
			bkr.Start()
			johnSnow, maria, johnT = mocks.ConnectAndLoadUsers(adapter)
		})

		AfterSuite(func() {
			bkr.Stop()
		})

		It("get should populate friends field", func() {
			user := <-bkr.Call("user.get", johnSnow.Get("id").String())

			Expect(user.Get("friends").Exists()).Should(BeTrue())
			Expect(user.Get("friends").Len()).Should(Equal(2))
			Expect(user.Get("friends").Array()[0].Get("id").String()).Should(Equal(maria.Get("id").String()))
			Expect(user.Get("friends").Array()[0].Get("name").String()).Should(Equal(maria.Get("name").String()))

			Expect(user.Get("friends").Array()[1].Get("id").String()).Should(Equal(johnT.Get("id").String()))
			Expect(user.Get("friends").Array()[1].Get("name").String()).Should(Equal(johnT.Get("name").String()))
		})

	})

})
