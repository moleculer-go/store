package db

import (
	"time"

	"github.com/moleculer-go/moleculer/payload"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var MongoTestsHost = "mongodb://192.168.1.110"

func mongoAdapter(database, collection string) *MongoAdapter {
	return &MongoAdapter{
		MongoURL:   MongoTestsHost,
		Timeout:    2 * time.Second,
		Database:   database,
		Collection: collection,
	}
}

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
				"searchFields": []string{"name"},
			})
			r := adapter.Find(p)

			Expect(r).ShouldNot(BeNil())
			Expect(r.Len()).Should(Equal(2))
		})

	})

})
