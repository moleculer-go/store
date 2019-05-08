package sqlite_test

import (
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
	"github.com/moleculer-go/moleculer/payload"
	"github.com/moleculer-go/stores"
	"github.com/moleculer-go/stores/sqlite"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type M map[string]interface{}

var _ = Describe("Sqlite Integration Test", func() {
	var bkr *broker.ServiceBroker
	adapter := &sqlite.Adapter{
		URI:   "file:memory:?mode=memory",
		Table: "users",
		Columns: []sqlite.Column{
			{
				Name: "username",
				Type: "string",
			},
			{
				Name: "name",
				Type: "string",
			},
			{
				Name: "status",
				Type: "integer",
			},
		},
	}

	var marie, john moleculer.Payload

	BeforeEach(func() {
		bkr := broker.New()
		bkr.Publish(moleculer.ServiceSchema{
			Name: "users",
			Settings: map[string]interface{}{
				"fields":    []string{"id", "username", "name"},
				"populates": map[string]interface{}{"friends": "users.get"},
			},
			Mixins: []moleculer.Mixin{db.Mixin(adapter)},
			Started: func(moleculer.BrokerContext, moleculer.ServiceSchema) {
				marie = adapter.Insert(payload.New(M{
					"name":     "Marie",
					"username": "marie_jane",
					"status":   200,
				}))
				john = adapter.Insert(payload.New(M{
					"name":     "John",
					"username": "john_snow",
					"status":   50,
				}))
			},
		})
		bkr.Start()
	})
	AfterEach(func() {
		bkr.Stop()
	})

	It("should create a record", func() {
		user := <-bkr.Call("users.create", map[string]interface{}{
			"username": "john",
			"name":     "John Doe",
			"status":   1,
		})
		Expect(user.Get("username").String()).Should(Equal("john"))
		Expect(user.Get("name").String()).Should(Equal("John Doe"))
		Expect(user.Get("status").String()).Should(Equal(1))
	})

	It("should find all users", func() {
		users := <-bkr.Call("users.find", map[string]interface{}{})
		Expect(users.Len()).Should(Equal(2))
	})

	It("should get a users", func() {
		user := <-bkr.Call("users.get", marie.Get("id"))
		Expect(user.Get("name").String()).Should(Equal("Marie"))

		user = <-bkr.Call("users.get", john.Get("id"))
		Expect(user.Get("name").String()).Should(Equal("John"))
	})
})
