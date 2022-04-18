package sqlite_test

import (
	"fmt"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
	"github.com/moleculer-go/moleculer/payload"
	"github.com/moleculer-go/store"
	"github.com/moleculer-go/store/sqlite"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type M map[string]interface{}

var _ = Describe("Sqlite Integration Test", func() {
	var bkr *broker.ServiceBroker
	var marie, john moleculer.Payload

	BeforeEach(func() {
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
				{
					Name: "someBytes",
					Type: "[]byte",
				},
			},
		}
		bkr = broker.New(&moleculer.Config{LogLevel: "error"})
		bkr.Publish(moleculer.ServiceSchema{
			Name: "users",
			Settings: map[string]interface{}{
				"fields":    []string{"id", "username", "name"},
				"populates": map[string]interface{}{"friends": "users.get"},
			},
			Mixins: []moleculer.Mixin{store.Mixin(adapter)},
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
		Expect(user.Get("status").Int()).Should(Equal(1))
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

	It("should list all users", func() {
		<-bkr.Call("users.create", map[string]interface{}{
			"username": "miko",
			"name":     "Miko",
			"status":   100,
		})

		<-bkr.Call("users.create", map[string]interface{}{
			"username": "gilbert",
			"name":     "Gilbert",
			"status":   150,
		})

		users := <-bkr.Call("users.list", map[string]interface{}{
			"page":     1,
			"pageSize": 2,
		})
		fmt.Println("")
		fmt.Println("users.list rows: ", users.Get("rows"))
		Expect(users.Get("rows").Len()).Should(Equal(2))
		Expect(users.Get("rows").Array()[0].Get("name").String()).Should(Equal("Marie"))
		Expect(users.Get("rows").Array()[1].Get("name").String()).Should(Equal("John"))

		users = <-bkr.Call("users.list", map[string]interface{}{
			"page":     2,
			"pageSize": 2,
		})
		fmt.Println("")
		fmt.Println("users.list rows: ", users.Get("rows"))
		Expect(users.Get("rows").Len()).Should(Equal(2))
		Expect(users.Get("rows").Array()[0].Get("name").String()).Should(Equal("Miko"))
		Expect(users.Get("rows").Array()[1].Get("name").String()).Should(Equal("Gilbert"))

	})

	It("should store and retried array of bytes []byte", func() {
		<-bkr.Call("users.create", map[string]interface{}{
			"username":  "testBytes",
			"name":      "bytes",
			"someBytes": []byte("message stored as []byte"),
		})

		users := <-bkr.Call("users.find", map[string]interface{}{
			"search":       "testBytes",
			"searchFields": []string{"username"},
			"fields":       []string{"someBytes", "name"},
		})
		Expect(users.Len()).Should(Equal(1))
		testBytes := users.First()
		Expect(testBytes.Get("name").String()).Should(Equal("bytes"))
		Expect(testBytes.Get("someBytes").Exists()).Should(BeTrue())
		bytes := testBytes.Get("someBytes").ByteArray()
		Expect(string(bytes)).Should(Equal("message stored as []byte"))
	})

})
