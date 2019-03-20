package db

import (
	"time"

	"github.com/moleculer-go/moleculer/payload"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

//var MongoTestsHost = "mongodb://192.168.1.110"
var MongoTestsHost = "mongodb://localhost"

func mongoAdapter(database, collection string) *MongoAdapter {
	return &MongoAdapter{
		MongoURL:   MongoTestsHost,
		Timeout:    2 * time.Second,
		Database:   database,
		Collection: collection,
	}
}

type M map[string]interface{}

var _ = Describe("Mongo Adapter", func() {
	adapter := mongoAdapter("mongo_adapter_tests", "user")

	//var johnSnow, marie, johnTravolta moleculer.Payload
	BeforeEach(func() {
		connectAndLoadUsers(adapter)
	})

	AfterEach(func() {
		adapter.Disconnect()
	})

	Describe("Find", func() {
		It("should search using search/searchFields params", func() {

			p := payload.Create(map[string]interface{}{
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
			p := payload.Create(query)
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
			p = payload.Create(query)
			r = adapter.Find(p)

			Expect(r.IsError()).Should(BeFalse())
			Expect(r.Len()).Should(Equal(3))
		})

	})

})
