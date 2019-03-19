package db

import (
	"github.com/hashicorp/go-memdb"
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/payload"
	"github.com/moleculer-go/moleculer/test"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func userAdapter() *MemoryAdapter {
	return &MemoryAdapter{
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
}

func connectAndLoadUsers(adapter *MemoryAdapter) (moleculer.Payload, moleculer.Payload, moleculer.Payload) {
	err := adapter.Connect()
	if err != nil {
		panic(err)
	}
	johnSnow := adapter.Insert(payload.Create(map[string]interface{}{
		"name":     "John",
		"lastname": "Snow",
		"age":      25,
	}))
	Expect(johnSnow.IsError()).Should(BeFalse())

	marie := adapter.Insert(payload.Create(map[string]interface{}{
		"name":     "Marie",
		"lastname": "Claire",
		"age":      75,
	}))
	Expect(marie.IsError()).Should(BeFalse())

	johnTravolta := adapter.Insert(payload.Create(map[string]interface{}{
		"name":     "John",
		"lastname": "Travolta",
		"age":      65,
	}))
	Expect(johnTravolta.IsError()).Should(BeFalse())
	return johnSnow, marie, johnTravolta
}

var _ = Describe("Moleculer DB Mixin", func() {

	Describe("list action", func() {
		adapter := userAdapter()

		//var johnSnow, marie, johnTravolta moleculer.Payload
		BeforeEach(func() {
			connectAndLoadUsers(adapter)
		})

		ctx, _ := test.ContextAndDelegated("list-test", moleculer.BrokerConfig{})
		It("should return page, pageSize, rows, total and totalPages", func() {
			params := payload.Create(map[string]interface{}{
				"searchFields": []string{"name"},
				"search":       "John",
			})
			mx := Service(adapter)
			ls := mx.Actions[2]
			rs := ls.Handler(ctx.(moleculer.Context), params)
			pl := payload.Create(rs)
			pl = pl.Add(map[string]interface{}{
				"rows": pl.Get("rows").Remove("id"),
			})
			Expect(pl.Get("rows").Len()).Should(Equal(2))
			Expect(pl.Get("page").Int()).Should(Equal(1))
			Expect(pl.Get("pageSize").Int()).Should(Equal(10))
			Expect(pl.Get("total").Int()).Should(Equal(2))
			Expect(pl.Get("totalPages").Int()).Should(Equal(1))
		})

	})

})
