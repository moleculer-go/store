package db_test

import (
	"os"
	"time"

	snap "github.com/moleculer-go/cupaloy"
	"github.com/moleculer-go/moleculer"
	db "github.com/moleculer-go/moleculer-db"
	"github.com/moleculer-go/moleculer-db/mocks"
	"github.com/moleculer-go/moleculer/broker"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var logLevel = "Error"

var mongoTestsHost = "mongodb://" + os.Getenv("MONGO_TEST_HOST")

var _ = Describe("Moleculer DB Integration Tests", func() {

	Describe("populates", func() {
		var johnSnow, maria, johnT moleculer.Payload
		bkr := broker.New(&moleculer.Config{
			DiscoverNodeID: func() string { return "node_populates" },
			LogLevel:       logLevel,
		})
		adapter := &db.MongoAdapter{
			MongoURL:   mongoTestsHost,
			Timeout:    time.Second * 5,
			Database:   "tests",
			Collection: "user",
		}
		userService := moleculer.Service{
			Name:   "user",
			Mixins: []moleculer.Mixin{db.Mixin(adapter)},
		}

		BeforeEach(func() {
			bkr.AddService(userService)
			bkr.Start()
			johnSnow, maria, johnT = mocks.ConnectAndLoadUsers(adapter)
		})

		AfterEach(func() {
			bkr.Stop()
		})

		It("get should populate friends field", func() {

			user := <-bkr.Call("user.get", map[string]interface{}{
				"id": johnT.Get("id").String(),
				"populates": map[string]interface{}{
					"friends": "user.get",
				},
			})
			Expect(user.IsError()).Should(BeFalse())
			Expect(user.Get("name").String()).Should(Equal(johnT.Get("name").String()))
			Expect(user.Get("friends").Exists()).Should(BeTrue())
			Expect(user.Get("friends").Len()).Should(Equal(2))
			Expect(user.Get("friends").Array()[0].Get("id").String()).Should(Equal(johnSnow.Get("id").String()))
			Expect(user.Get("friends").Array()[0].Get("name").String()).Should(Equal(johnSnow.Get("name").String()))

			Expect(user.Get("friends").Array()[1].Get("id").String()).Should(Equal(maria.Get("id").String()))
			Expect(user.Get("friends").Array()[1].Get("name").String()).Should(Equal(maria.Get("name").String()))
		})

		It("get should populate friends field", func() {
			user := <-bkr.Call("user.get", map[string]interface{}{
				"ids": []string{maria.Get("id").String()},
				"populates": map[string]interface{}{
					"master": "user.get",
				},
			})
			Expect(user.Len()).Should(Equal(1))

			Expect(user.IsError()).Should(BeFalse())
			Expect(user.First().Get("name").String()).Should(Equal(maria.Get("name").String()))
			Expect(user.First().Get("master").Exists()).Should(BeTrue())
			Expect(user.First().Get("master").Get("id").String()).Should(Equal(johnSnow.Get("id").String()))
			Expect(user.First().Get("master").Get("name").String()).Should(Equal(johnSnow.Get("name").String()))
		})

	})

	//cleanResult remove dyanmic fields from the payload.
	cleanResult := func(p moleculer.Payload) moleculer.Payload {
		return p.Remove("id", "_id", "master", "friends")
	}

	Describe("actions", func() {
		var johnSnow, marie, johnT moleculer.Payload
		bkr := broker.New(&moleculer.Config{
			DiscoverNodeID: func() string { return "node_find" },
			LogLevel:       logLevel,
		})
		adapter := &db.MongoAdapter{
			MongoURL:   mongoTestsHost,
			Timeout:    time.Second * 5,
			Database:   "tests",
			Collection: "user",
		}
		userService := moleculer.Service{
			Name:   "user",
			Mixins: []moleculer.Mixin{db.Mixin(adapter)},
		}

		BeforeEach(func() {
			bkr.AddService(userService)
			bkr.Start()
			johnSnow, marie, johnT = mocks.ConnectAndLoadUsers(adapter)
			Expect(johnSnow.IsError()).Should(BeFalse())
			Expect(marie.IsError()).Should(BeFalse())
			Expect(johnT.IsError()).Should(BeFalse())
		})

		AfterEach(func() {
			bkr.Stop()
		})

		It("find records and match with snapshot", func() {
			rs := <-bkr.Call("user.find", map[string]interface{}{})
			Expect(rs.IsError()).Should(BeFalse())
			Expect(snap.SnapshotMulti("find-result", cleanResult(rs))).Should(Succeed())
		})

		It("list records and match with snapshot", func() {
			rs := <-bkr.Call("user.list", map[string]interface{}{})
			Expect(rs.IsError()).Should(BeFalse())
			Expect(snap.SnapshotMulti("list-result", cleanResult(rs))).Should(Succeed())
		})

		It("create a record and match with snapshot", func() {
			rs := <-bkr.Call("user.create", map[string]interface{}{"name": "Ze", "lastname": "DoCaixao"})
			Expect(rs.IsError()).Should(BeFalse())
			Expect(snap.SnapshotMulti("created-record", cleanResult(rs))).Should(Succeed())
			fr := <-bkr.Call("user.get", map[string]interface{}{"id": rs.Get("id").String()})
			Expect(snap.SnapshotMulti("created-find", cleanResult(fr))).Should(Succeed())
		})

		It("update a record and match with snapshot", func() {
			rs := <-bkr.Call("user.update", map[string]interface{}{"id": johnSnow.Get("id").String(), "name": "Ze", "lastname": "DasCouves"})
			Expect(rs.IsError()).Should(BeFalse())
			Expect(snap.SnapshotMulti("updated-record", cleanResult(rs))).Should(Succeed())
			fr := <-bkr.Call("user.get", map[string]interface{}{"id": johnSnow.Get("id").String()})
			Expect(snap.SnapshotMulti("updated-find", cleanResult(fr))).Should(Succeed())
		})

		It("remove a record and match with snapshot", func() {
			rs := <-bkr.Call("user.remove", map[string]interface{}{"id": johnT.Get("id").String()})
			Expect(rs.IsError()).Should(BeFalse())
			Expect(snap.SnapshotMulti("removed-record", cleanResult(rs))).Should(Succeed())
			fr := <-bkr.Call("user.get", map[string]interface{}{"id": johnT.Get("id").String()})
			Expect(snap.SnapshotMulti("removed-find", cleanResult(fr))).Should(Succeed())
		})

	})
})
