package db

import (
	"fmt"
	"os"
	"time"

	snap "github.com/moleculer-go/cupaloy"
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer-db/mocks"
	"github.com/moleculer-go/moleculer/payload"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
)

var mongoTestsHost = "mongodb://" + os.Getenv("MONGO_TEST_HOST")

func mongoAdapter(database, collection string) *MongoAdapter {
	fmt.Println("mongoTestsHost: ", mongoTestsHost)
	adapter := &MongoAdapter{
		MongoURL:   mongoTestsHost,
		Timeout:    2 * time.Second,
		Database:   database,
		Collection: collection,
	}
	adapter.Init(log.WithField("test", "adapter"), M{})
	return adapter
}

type M map[string]interface{}

var _ = Describe("Mongo Adapter", func() {
	adapter := mongoAdapter("mongo_adapter_tests", "user")
	totalRecords := 6
	var johnSnow, marie, johnTravolta moleculer.Payload
	BeforeEach(func() {
		johnSnow, marie, johnTravolta = mocks.ConnectAndLoadUsers(adapter)
	})

	AfterEach(func() {
		adapter.Disconnect()
	})

	Describe("Count", func() {
		It("should count the number of records properly", func() {
			result := adapter.Count(payload.New(M{}))
			Expect(result.IsError()).Should(BeFalse())
			Expect(result.Int()).Should(Equal(totalRecords))
		})

		It("should count the number of records and apply filter", func() {
			result := adapter.Count(payload.New(M{"query": M{
				"name": "John",
			}}))
			Expect(result.IsError()).Should(BeFalse())
			Expect(result.Int()).Should(Equal(2))
		})

	})

	Describe("Find", func() {

		It("should find using an empty query and return all records", func() {
			result := adapter.Find(payload.New(M{}))
			Expect(result.IsError()).Should(BeFalse())
			Expect(result.Len()).Should(Equal(totalRecords))
		})

		//Sort apprently not working in this client
		XIt("should sort the results", func() {
			result := adapter.Find(payload.New(M{
				"query": M{},
				"sort":  "name",
			}))
			Expect(result.IsError()).Should(BeFalse())
			Expect(result.Len()).Should(Equal(totalRecords))
			Expect(snap.SnapshotMulti("sort-1", result.Remove("id"))).Should(Succeed())

			result2 := adapter.Find(payload.New(M{
				"query": M{},
				"sort":  "age",
			}))
			Expect(result2.IsError()).Should(BeFalse())
			Expect(result2.Len()).Should(Equal(totalRecords))
			result2 = result2.Remove("id")
			Expect(snap.SnapshotMulti("sort-2", result2)).Should(Succeed())

			//shuold not match sort-1
			Expect(snap.SnapshotMulti("sort-1", result2)).ShouldNot(Succeed())
		})

		It("should offset the results", func() {
			result := adapter.Find(payload.New(M{
				"query":  M{},
				"sort":   "name",
				"offset": 2,
			}))
			Expect(result.IsError()).Should(BeFalse())
			Expect(result.Len()).Should(Equal(totalRecords - 2))
			Expect(snap.SnapshotMulti("offset-2", result.Remove("id", "master"))).Should(Succeed())

			result = adapter.Find(payload.New(M{
				"query":  M{},
				"sort":   "name",
				"offset": 4,
			}))
			Expect(result.IsError()).Should(BeFalse())
			Expect(result.Len()).Should(Equal(totalRecords - 4))
			Expect(snap.SnapshotMulti("offset-4", result.Remove("id"))).Should(Succeed())
		})

		It("should find using an empty query and limit = 3 and return 3 records", func() {
			query := M{
				"query": M{},
				"limit": 3,
			}
			p := payload.New(query)
			r := adapter.Find(p)

			Expect(r.IsError()).Should(BeFalse())
			Expect(r.Len()).Should(Equal(3))
		})

		It("should search using search/searchFields params", func() {

			p := payload.New(map[string]interface{}{
				"search":       "John",
				"searchFields": []string{"name", "midlename"},
			})
			r := adapter.Find(p)

			Expect(r).ShouldNot(BeNil())
			Expect(r.Len()).Should(Equal(2))
		})

		It("should search using curtom query param", func() {
			query := M{
				"query": M{
					"age": M{
						"$gt": 60,
					},
				},
			}
			p := payload.New(query)
			r := adapter.Find(p)

			Expect(r.IsError()).Should(BeFalse())
			Expect(r.Len()).Should(Equal(2))

			query = M{
				"query": M{
					"$or": []M{
						M{"name": "John"},
						M{"lastname": "Claire"},
					},
				},
			}
			p = payload.New(query)
			r = adapter.Find(p)

			Expect(r.IsError()).Should(BeFalse())
			Expect(r.Len()).Should(Equal(3))
		})

	})

	Describe("FindById", func() {
		It("should find a records by its ID", func() {
			result := adapter.FindById(johnSnow.Get("id"))
			Expect(result.Exists()).Should(BeTrue())
			Expect(result.IsError()).Should(BeFalse())
			Expect(result.Get("name").String()).Should(Equal(johnSnow.Get("name").String()))
			Expect(result.Get("lastname").String()).Should(Equal(johnSnow.Get("lastname").String()))
			Expect(result.Get("age").Int()).Should(Equal(johnSnow.Get("age").Int()))

			result = adapter.FindById(marie.Get("id"))
			Expect(result.IsError()).Should(BeFalse())
			Expect(result.Get("name").String()).Should(Equal(marie.Get("name").String()))
			Expect(result.Get("lastname").String()).Should(Equal(marie.Get("lastname").String()))
			Expect(result.Get("age").Int()).Should(Equal(marie.Get("age").Int()))
		})
	})

	Describe("FindOne", func() {
		It("should find one a records at a time", func() {
			result := adapter.FindOne(payload.New(M{
				"query": M{
					"age": M{
						"$gt": 60,
					},
				},
				"sort": "name",
			}))
			Expect(result.Exists()).Should(BeTrue())
			Expect(result.IsError()).Should(BeFalse())
			Expect(result.Get("name").String()).Should(Equal("John"))

			result = adapter.FindOne(payload.New(M{
				"query": M{
					"age": M{
						"$lt": 20,
					},
				},
				"sort": "name",
			}))
			Expect(result.Exists()).Should(BeTrue())
			Expect(result.IsError()).Should(BeFalse())
			Expect(result.Get("name").String()).Should(Equal("Peter"))
		})
	})

	Describe("FindByIds", func() {
		It("should find multiple records", func() {
			result := adapter.FindByIds(payload.EmptyList().AddItem(johnSnow.Get("id")).AddItem(marie.Get("id")).AddItem(johnTravolta.Get("id")))
			Expect(result.Exists()).Should(BeTrue())
			Expect(result.IsError()).Should(BeFalse())
			Expect(result.Array()[0].Get("name").String()).Should(Equal("John"))
			Expect(result.Array()[1].Get("name").String()).Should(Equal("Marie"))
			Expect(result.Array()[2].Get("name").String()).Should(Equal("John"))

		})
	})

	Describe("Removes", func() {
		It("RemoveById should removed a record using a specific ID", func() {
			result := adapter.RemoveById(johnSnow.Get("id"))
			Expect(result.Exists()).Should(BeTrue())
			Expect(result.IsError()).Should(BeFalse())
			Expect(result.Get("deletedCount").Int()).Should(Equal(1))

			result = adapter.Find(payload.New(M{}))
			Expect(result.IsError()).Should(BeFalse())
			Expect(result.Len()).Should(Equal(totalRecords - 1))
		})

		It("RemoveAll should removed all records", func() {
			result := adapter.RemoveAll()
			Expect(result.Exists()).Should(BeTrue())
			Expect(result.IsError()).Should(BeFalse())
			Expect(result.Get("deletedCount").Int()).Should(Equal(6))

			result = adapter.Find(payload.New(M{}))
			Expect(result.IsError()).Should(BeFalse())
			Expect(result.Len()).Should(Equal(0))
		})
	})

	Describe("Updates", func() {

		It("Update should update record", func() {
			result := adapter.Update(payload.Empty().AddMany(M{
				"id":       johnSnow.Get("id").String(),
				"age":      175,
				"lastname": "Cruzader",
				"house":    "Spark",
			}))
			Expect(result.Exists()).Should(BeTrue())
			Expect(result.IsError()).Should(BeFalse())
			Expect(result.Get("modifiedCount").Int()).Should(Equal(1))

			result = adapter.FindById(johnSnow.Get("id"))
			Expect(result.Get("age").Int()).Should(Equal(175))
			Expect(result.Get("lastname").String()).Should(Equal("Cruzader"))
			Expect(result.Get("house").String()).Should(Equal("Spark"))
		})

		It("UpdateById should update record", func() {
			result := adapter.UpdateById(johnSnow.Get("id"), payload.New(M{
				"age": 120,
			}))
			Expect(result.Exists()).Should(BeTrue())
			Expect(result.IsError()).Should(BeFalse())
			Expect(result.Get("modifiedCount").Int()).Should(Equal(1))

			result = adapter.UpdateById(marie.Get("id"), payload.New(M{
				"lastname": "Vai com as outras",
				"age":      320,
				"newField": "newValue",
			}))
			Expect(result.Exists()).Should(BeTrue())
			Expect(result.IsError()).Should(BeFalse())

			result = adapter.FindById(marie.Get("id"))
			Expect(result.Get("age").Int()).Should(Equal(320))
			Expect(result.Get("lastname").String()).Should(Equal("Vai com as outras"))
			Expect(result.Get("newField").String()).Should(Equal("newValue"))

			result = adapter.FindById(johnSnow.Get("id"))
			Expect(result.Get("age").Int()).Should(Equal(120))

		})
	})

})
