package db

import (
	"github.com/hashicorp/go-memdb"
	snap "github.com/moleculer-go/cupaloy"
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/payload"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("MemoryAdapter", func() {

	adapter := &MemoryAdapter{
		Table: "user",
		Schema: &memdb.DBSchema{
			Tables: map[string]*memdb.TableSchema{
				"user": &memdb.TableSchema{
					Name: "user",
					Indexes: map[string]*memdb.IndexSchema{
						"id": &memdb.IndexSchema{
							Name:    "id",
							Unique:  true,
							Indexer: &PayloadIndex{Field: "id"},
						},
						"name": &memdb.IndexSchema{
							Name:    "name",
							Unique:  false,
							Indexer: &PayloadIndex{Field: "name"},
						},
					},
				},
			},
		},
	}

	var johnSnow, marie, johnTravolta moleculer.Payload
	BeforeEach(func() {
		err := adapter.Connect()
		if err != nil {
			panic(err)
		}
		johnSnow = adapter.Insert(payload.Create(map[string]interface{}{
			"name":     "John",
			"lastname": "Snow",
			"age":      25,
		}))
		Expect(johnSnow.IsError()).Should(BeFalse())

		marie = adapter.Insert(payload.Create(map[string]interface{}{
			"name":     "Marie",
			"lastname": "Claire",
			"age":      75,
		}))
		Expect(marie.IsError()).Should(BeFalse())

		johnTravolta = adapter.Insert(payload.Create(map[string]interface{}{
			"name":     "John",
			"lastname": "Travolta",
			"age":      65,
		}))
		Expect(johnTravolta.IsError()).Should(BeFalse())
	})

	AfterEach(func() {
		johnSnow = nil
		johnTravolta = nil
		marie = nil
		adapter.Disconnect()
	})

	It("Find() should return matching records", func() {
		r := adapter.Find(payload.Create(map[string]interface{}{
			"searchFields": []string{"name"},
			"search":       "John",
		}))
		Expect(r.IsError()).Should(BeFalse())
		Expect(r.Len()).Should(Equal(2))
		Expect(snap.SnapshotMulti("Find()", r.Remove("id"))).Should(Succeed())
	})

	It("Update() should update existing record matching records", func() {
		r := adapter.Update(payload.Create(map[string]interface{}{
			"id":  johnTravolta.Get("id").String(),
			"age": 67,
		}))
		Expect(r.IsError()).Should(BeFalse())
		Expect(r.Get("name").String()).Should(Equal("John"))
		Expect(r.Get("lastname").String()).Should(Equal("Travolta"))
		Expect(r.Get("age").Int()).Should(Equal(67))
	})

	It("Insert() should insert new records", func() {
		r := adapter.Insert(payload.Create(map[string]interface{}{
			"name":     "Julio",
			"lastname": "Cesar",
		}))
		Expect(r.IsError()).Should(BeFalse())
		Expect(r.Get("name").String()).Should(Equal("Julio"))
		Expect(r.Get("lastname").String()).Should(Equal("Cesar"))

		r = adapter.Find(payload.Create(map[string]interface{}{
			"searchFields": []string{"name"},
			"search":       "Julio",
		}))
		Expect(r.IsError()).Should(BeFalse())
		Expect(r.Len()).Should(Equal(1))
		Expect(snap.SnapshotMulti("Insert()", r.Remove("id"))).Should(Succeed())
	})

})
