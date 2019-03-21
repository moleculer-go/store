package db

import (
	"github.com/hashicorp/go-memdb"
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/payload"
	"github.com/moleculer-go/moleculer/test"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func memoryAdapter(table string, dbSchema *memdb.DBSchema) *MemoryAdapter {
	return &MemoryAdapter{
		Table:  table,
		Schema: dbSchema,
	}
}

var userDbSchema = &memdb.DBSchema{
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
				"all": &memdb.IndexSchema{
					Name:    "all",
					Unique:  false,
					Indexer: &PayloadIndex{Field: "all"},
				},
			},
		},
	},
}

func connectAndLoadUsers(adapter Adapter) (moleculer.Payload, moleculer.Payload, moleculer.Payload) {
	err := adapter.Connect()
	if err != nil {
		panic(err)
	}
	adapter.RemoveAll()
	johnSnow := adapter.Insert(payload.New(map[string]interface{}{
		"name":     "John",
		"lastname": "Snow",
		"age":      25,
	}))
	Expect(johnSnow.IsError()).Should(BeFalse())

	marie := adapter.Insert(payload.New(map[string]interface{}{
		"name":     "Marie",
		"lastname": "Claire",
		"age":      75,
	}))
	Expect(marie.IsError()).Should(BeFalse())

	johnTravolta := adapter.Insert(payload.New(map[string]interface{}{
		"name":     "John",
		"lastname": "Travolta",
		"age":      65,
	}))

	adapter.Insert(payload.New(map[string]interface{}{
		"name":     "Julian",
		"lastname": "Assange",
		"age":      46,
	}))

	adapter.Insert(payload.New(map[string]interface{}{
		"name":     "Peter",
		"lastname": "Pan",
		"age":      13,
	}))

	adapter.Insert(payload.New(map[string]interface{}{
		"name":     "Stone",
		"lastname": "Man",
		"age":      13,
	}))

	Expect(johnTravolta.IsError()).Should(BeFalse())
	return johnSnow, marie, johnTravolta
}

var _ = Describe("Moleculer DB Mixin", func() {

	Describe("list action", func() {
		adapter := memoryAdapter("user", userDbSchema)

		//var johnSnow, marie, johnTravolta moleculer.Payload
		BeforeEach(func() {
			connectAndLoadUsers(adapter)
		})

		ctx, _ := test.ContextAndDelegated("list-test", moleculer.BrokerConfig{})
		It("should return page, pageSize, rows, total and totalPages", func() {
			params := payload.New(map[string]interface{}{
				"searchFields": []string{"name"},
				"search":       "John",
			})
			mx := Service(adapter)
			ls := mx.Actions[2]
			rs := ls.Handler(ctx.(moleculer.Context), params)
			pl := payload.New(rs)
			pl = pl.Add("rows", pl.Get("rows").Remove("id"))
			Expect(pl.Get("rows").Len()).Should(Equal(2))
			Expect(pl.Get("page").Int()).Should(Equal(1))
			Expect(pl.Get("pageSize").Int()).Should(Equal(10))
			Expect(pl.Get("total").Int()).Should(Equal(2))
			Expect(pl.Get("totalPages").Int()).Should(Equal(1))
		})

	})

})
