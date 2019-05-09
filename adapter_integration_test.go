package store_test

import (
	"os"
	"time"

	"github.com/moleculer-go/cupaloy/v2"
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
	store "github.com/moleculer-go/store"
	"github.com/moleculer-go/store/mocks"
	"github.com/moleculer-go/store/mongo"
	"github.com/moleculer-go/store/sqlite"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var snap = cupaloy.New(cupaloy.FailOnUpdate(os.Getenv("UPDATE_SNAPSHOTS") == ""))

var logLevel = "error"

var mongoTestsHost = "mongodb://" + os.Getenv("MONGO_TEST_HOST")

var _ = Describe("Moleculer DB Integration Tests", func() {

	//cleanResult remove dyanmic fields from the payload.
	cleanResult := func(p moleculer.Payload) moleculer.Payload {
		return p.Remove("id", "_id", "master", "friends")
	}

	testPopulates := func(label string, createAdapter func() store.Adapter) {
		label = label + " populates"
		Context(label, func() {
			var johnSnow, maria, johnT moleculer.Payload
			var bkr *broker.ServiceBroker

			BeforeEach(func() {
				bkr = broker.New(&moleculer.Config{
					DiscoverNodeID: func() string { return "node_populates" },
					LogLevel:       logLevel,
				})
				adapter := createAdapter()
				userService := moleculer.ServiceSchema{
					Name: "user",
					Settings: map[string]interface{}{
						"populates": map[string]interface{}{
							"friends": "user.get",
							"master":  "user.get",
						},
					},
					Mixins: []moleculer.Mixin{store.Mixin(adapter)},
				}
				bkr.Publish(userService)
				bkr.Start()
				johnSnow, maria, johnT = mocks.LoadUsers(adapter)
			})

			AfterEach(func() {
				bkr.Stop()
			})

			It("get should populate friends field", func() {
				user := <-bkr.Call("user.get", map[string]interface{}{
					"id":       johnT.Get("id").String(),
					"populate": []string{"friends"},
				})
				Expect(user.Error()).Should(BeNil())
				Expect(user.Get("name").String()).Should(Equal(johnT.Get("name").String()))
				Expect(user.Get("friends").Exists()).Should(BeTrue())
				Expect(user.Get("friends").Len()).Should(Equal(2))
				Expect(user.Get("friends").Array()[0].Get("id").String()).Should(Equal(johnSnow.Get("id").String()))
				Expect(user.Get("friends").Array()[0].Get("name").String()).Should(Equal(johnSnow.Get("name").String()))

				Expect(user.Get("friends").Array()[1].Get("id").String()).Should(Equal(maria.Get("id").String()))
				Expect(user.Get("friends").Array()[1].Get("name").String()).Should(Equal(maria.Get("name").String()))
			})

			It("get should populate master field", func() {
				user := <-bkr.Call("user.get", map[string]interface{}{
					"ids":      []string{maria.Get("id").String()},
					"populate": []string{"master"},
				})
				Expect(user.Len()).Should(Equal(1))

				Expect(user.Error()).Should(BeNil())
				Expect(user.First().Get("name").String()).Should(Equal(maria.Get("name").String()))
				Expect(user.First().Get("master").Exists()).Should(BeTrue())
				Expect(user.First().Get("master").Get("id").String()).Should(Equal(johnSnow.Get("id").String()))
				Expect(user.First().Get("master").Get("name").String()).Should(Equal(johnSnow.Get("name").String()))
			})

			It("get should populate master and friends field", func() {
				user := <-bkr.Call("user.get", map[string]interface{}{
					"ids":      []string{johnT.Get("id").String()},
					"populate": []string{"master", "friends"},
				})
				Expect(user.Len()).Should(Equal(1))

				Expect(user.Error()).Should(BeNil())
				user = user.First()
				Expect(user.Get("name").String()).Should(Equal(johnT.Get("name").String()))
				Expect(user.Get("master").Exists()).Should(BeTrue())
				Expect(user.Get("master").Get("id").String()).Should(Equal(johnSnow.Get("id").String()))
				Expect(user.Get("master").Get("name").String()).Should(Equal(johnSnow.Get("name").String()))

				Expect(user.Get("friends").Exists()).Should(BeTrue())
				Expect(user.Get("friends").Len()).Should(Equal(2))
				Expect(user.Get("friends").Array()[0].Get("id").String()).Should(Equal(johnSnow.Get("id").String()))
				Expect(user.Get("friends").Array()[0].Get("name").String()).Should(Equal(johnSnow.Get("name").String()))

				Expect(user.Get("friends").Array()[1].Get("id").String()).Should(Equal(maria.Get("id").String()))
				Expect(user.Get("friends").Array()[1].Get("name").String()).Should(Equal(maria.Get("name").String()))
			})

		})
	}

	testActions := func(label string, createAdapter func() store.Adapter) {
		label = label + "-actions"
		Context(label, func() {
			var johnSnow, marie, johnT moleculer.Payload
			bkr := broker.New(&moleculer.Config{
				DiscoverNodeID: func() string { return "node_find" },
				LogLevel:       logLevel,
			})
			adapter := createAdapter()
			userService := moleculer.ServiceSchema{
				Name:   "user",
				Mixins: []moleculer.Mixin{store.Mixin(adapter)},
			}

			BeforeEach(func() {
				bkr.Publish(userService)
				bkr.Start()
				johnSnow, marie, johnT = mocks.LoadUsers(adapter)
				Expect(johnSnow.Error()).Should(BeNil())
				Expect(marie.Error()).Should(BeNil())
				Expect(johnT.Error()).Should(BeNil())
			})

			AfterEach(func() {
				bkr.Stop()
			})

			It("find records and match with snapshot", func() {
				rs := <-bkr.Call("user.find", map[string]interface{}{})
				Expect(rs.Error()).Should(BeNil())
				Expect(snap.SnapshotMulti(label+"-find-result", cleanResult(rs))).Should(Succeed())
			})

			It("list records and match with snapshot", func() {
				rs := <-bkr.Call("user.list", map[string]interface{}{})
				Expect(rs.Error()).Should(BeNil())
				Expect(snap.SnapshotMulti(label+"-list-atts", cleanResult(rs.Remove("rows")))).Should(Succeed())
				Expect(snap.SnapshotMulti(label+"-list-rows", cleanResult(rs.Get("rows").Remove("id")))).Should(Succeed())
			})

			It("create a record and match with snapshot", func() {
				rs := <-bkr.Call("user.create", map[string]interface{}{"name": "Ze", "lastname": "DoCaixao"})
				Expect(rs.Error()).Should(BeNil())
				Expect(snap.SnapshotMulti(label+"-created-record", cleanResult(rs))).Should(Succeed())
				fr := <-bkr.Call("user.get", map[string]interface{}{"id": rs.Get("id").String()})
				Expect(snap.SnapshotMulti(label+"-created-find", cleanResult(fr))).Should(Succeed())
			})

			It("update a record and match with snapshot", func() {
				rs := <-bkr.Call("user.update", map[string]interface{}{"id": johnSnow.Get("id").String(), "name": "Ze", "lastname": "DasCouves"})
				Expect(rs.Error()).Should(BeNil())
				Expect(snap.SnapshotMulti(label+"-updated-record", cleanResult(rs))).Should(Succeed())
				fr := <-bkr.Call("user.get", map[string]interface{}{"id": johnSnow.Get("id").String()})
				Expect(snap.SnapshotMulti(label+"-updated-find", cleanResult(fr))).Should(Succeed())
			})

			It("remove a record and match with snapshot", func() {
				rs := <-bkr.Call("user.remove", map[string]interface{}{"id": johnT.Get("id").String()})
				Expect(rs.Error()).Should(BeNil())
				Expect(snap.SnapshotMulti(label+"-removed-record", cleanResult(rs))).Should(Succeed())
				fr := <-bkr.Call("user.get", map[string]interface{}{"id": johnT.Get("id").String()})
				Expect(snap.SnapshotMulti(label+"-removed-find", cleanResult(fr))).Should(Succeed())
			})

		})
	}

	testPopulates("Mongo-Adapter", func() store.Adapter {
		return &mongo.MongoAdapter{
			MongoURL:   mongoTestsHost,
			Timeout:    time.Second * 5,
			Database:   "tests",
			Collection: "user",
		}
	})

	testActions("Mongo-Adapter", func() store.Adapter {
		return &mongo.MongoAdapter{
			MongoURL:   mongoTestsHost,
			Timeout:    time.Second * 5,
			Database:   "tests",
			Collection: "user",
		}
	})

	var cols = []sqlite.Column{
		{
			Name: "name",
			Type: "string",
		},
		{
			Name: "lastname",
			Type: "string",
		},
		{
			Name: "age",
			Type: "integer",
		},
		{
			Name: "master",
			Type: "string",
		},
		{
			Name: "friends",
			Type: "[]string",
		},
	}

	testPopulates("Mongo-Adapter", func() store.Adapter {
		return &sqlite.Adapter{
			URI:     "file:memory:?mode=memory",
			Table:   "users_populates",
			Columns: cols,
		}
	})

	testActions("SQLite-Adapter", func() store.Adapter {
		return &sqlite.Adapter{
			URI:     "file:memory:?mode=memory",
			Table:   "users_actions",
			Columns: cols,
		}
	})

})
