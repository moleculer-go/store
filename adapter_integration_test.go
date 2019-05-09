package stores_test

import (
	"fmt"
	"os"
	"time"

	"github.com/moleculer-go/cupaloy"
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
	"github.com/moleculer-go/stores"
	"github.com/moleculer-go/stores/mocks"
	"github.com/moleculer-go/stores/mongo"
	"github.com/moleculer-go/stores/sqlite"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var logLevel = "Error"

var mongoTestsHost = "mongodb://" + os.Getenv("MONGO_TEST_HOST")

var _ = Describe("Moleculer DB Integration Tests", func() {

	fmt.Println("Env SNAPSHOT_UPDATE: ", os.Getenv("SNAPSHOT_UPDATE"))
	failOnUpdate := os.Getenv("SNAPSHOT_UPDATE") == ""
	fmt.Println("failOnUpdate: ", failOnUpdate)
	var snap = cupaloy.New(cupaloy.FailOnUpdate(failOnUpdate))

	//cleanResult remove dyanmic fields from the payload.
	cleanResult := func(p moleculer.Payload) moleculer.Payload {
		return p.Remove("id", "_id", "master", "friends")
	}

	// testPopulates := func(label string, createAdapter func() stores.Adapter) {
	// 	Describe(label+" populates", func() {
	// 		var johnSnow, maria, johnT moleculer.Payload
	// 		bkr := broker.New(&moleculer.Config{
	// 			DiscoverNodeID: func() string { return "node_populates" },
	// 			LogLevel:       logLevel,
	// 		})
	// 		adapter := createAdapter()
	// 		userService := moleculer.ServiceSchema{
	// 			Name: "user",
	// 			Settings: map[string]interface{}{
	// 				"populates": map[string]interface{}{
	// 					"friends": "user.get",
	// 					"master":  "user.get",
	// 				},
	// 			},
	// 			Mixins: []moleculer.Mixin{stores.Mixin(adapter)},
	// 		}

	// 		BeforeEach(func() {
	// 			bkr.Publish(userService)
	// 			bkr.Start()
	// 			johnSnow, maria, johnT = mocks.ConnectAndLoadUsers(adapter)
	// 		})

	// 		AfterEach(func() {
	// 			bkr.Stop()
	// 		})

	// 		It("get should populate friends field", func() {
	// 			user := <-bkr.Call("user.get", map[string]interface{}{
	// 				"id":       johnT.Get("id").String(),
	// 				"populate": []string{"friends"},
	// 			})
	// 			Expect(user.Error()).Should(BeNil())
	// 			Expect(user.Get("name").String()).Should(Equal(johnT.Get("name").String()))
	// 			Expect(user.Get("friends").Exists()).Should(BeTrue())
	// 			Expect(user.Get("friends").Len()).Should(Equal(2))
	// 			Expect(user.Get("friends").Array()[0].Get("id").String()).Should(Equal(johnSnow.Get("id").String()))
	// 			Expect(user.Get("friends").Array()[0].Get("name").String()).Should(Equal(johnSnow.Get("name").String()))

	// 			Expect(user.Get("friends").Array()[1].Get("id").String()).Should(Equal(maria.Get("id").String()))
	// 			Expect(user.Get("friends").Array()[1].Get("name").String()).Should(Equal(maria.Get("name").String()))
	// 		})

	// 		It("get should populate master field", func() {
	// 			user := <-bkr.Call("user.get", map[string]interface{}{
	// 				"ids":      []string{maria.Get("id").String()},
	// 				"populate": []string{"master"},
	// 			})
	// 			Expect(user.Len()).Should(Equal(1))

	// 			Expect(user.Error()).Should(BeNil())
	// 			Expect(user.First().Get("name").String()).Should(Equal(maria.Get("name").String()))
	// 			Expect(user.First().Get("master").Exists()).Should(BeTrue())
	// 			Expect(user.First().Get("master").Get("id").String()).Should(Equal(johnSnow.Get("id").String()))
	// 			Expect(user.First().Get("master").Get("name").String()).Should(Equal(johnSnow.Get("name").String()))
	// 		})

	// 		It("get should populate master and friends field", func() {
	// 			user := <-bkr.Call("user.get", map[string]interface{}{
	// 				"ids":      []string{johnT.Get("id").String()},
	// 				"populate": []string{"master", "friends"},
	// 			})
	// 			Expect(user.Len()).Should(Equal(1))

	// 			Expect(user.Error()).Should(BeNil())
	// 			user = user.First()
	// 			Expect(user.Get("name").String()).Should(Equal(johnT.Get("name").String()))
	// 			Expect(user.Get("master").Exists()).Should(BeTrue())
	// 			Expect(user.Get("master").Get("id").String()).Should(Equal(johnSnow.Get("id").String()))
	// 			Expect(user.Get("master").Get("name").String()).Should(Equal(johnSnow.Get("name").String()))

	// 			Expect(user.Get("friends").Exists()).Should(BeTrue())
	// 			Expect(user.Get("friends").Len()).Should(Equal(2))
	// 			Expect(user.Get("friends").Array()[0].Get("id").String()).Should(Equal(johnSnow.Get("id").String()))
	// 			Expect(user.Get("friends").Array()[0].Get("name").String()).Should(Equal(johnSnow.Get("name").String()))

	// 			Expect(user.Get("friends").Array()[1].Get("id").String()).Should(Equal(maria.Get("id").String()))
	// 			Expect(user.Get("friends").Array()[1].Get("name").String()).Should(Equal(maria.Get("name").String()))
	// 		})

	// 	})
	// }

	testActions := func(label string, createAdapter func() stores.Adapter) {
		Context(label+" mixin actions", func() {
			var johnSnow, marie, johnT moleculer.Payload
			bkr := broker.New(&moleculer.Config{
				DiscoverNodeID: func() string { return "node_find" },
				LogLevel:       logLevel,
			})
			adapter := createAdapter()
			userService := moleculer.ServiceSchema{
				Name:   "user",
				Mixins: []moleculer.Mixin{stores.Mixin(adapter)},
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
				Expect(snap.SnapshotMulti(label+"-list-result", cleanResult(rs))).Should(Succeed())
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

	// testPopulates("Mongo-Adapter", func() stores.Adapter {
	// 	return &mongo.MongoAdapter{
	// 		MongoURL:   mongoTestsHost,
	// 		Timeout:    time.Second * 5,
	// 		Database:   "tests",
	// 		Collection: "user",
	// 	}
	// })

	testActions("Mongo-Adapter", func() stores.Adapter {
		return &mongo.MongoAdapter{
			MongoURL:   mongoTestsHost,
			Timeout:    time.Second * 5,
			Database:   "tests",
			Collection: "user",
		}
	})

	testActions("SQLite-Adapter", func() stores.Adapter {
		return &sqlite.Adapter{
			URI:   "file:memory:?mode=memory",
			Table: "users",
			Columns: []sqlite.Column{
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
			},
		}
	})

})
