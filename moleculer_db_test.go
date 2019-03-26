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
		AfterEach(func() {
			adapter.Disconnect()
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

	Describe("find action", func() {
		adapter := memoryAdapter("user", userDbSchema)

		BeforeEach(func() {
			connectAndLoadUsers(adapter)
		})
		AfterEach(func() {
			adapter.Disconnect()
		})

		settings := map[string]interface{}{
			"fields":    []string{"**"},
			"populates": map[string]interface{}{},
		}

		ctx, delegates := test.ContextAndDelegated("find-test", moleculer.BrokerConfig{})
		delegates.MultActionDelegate = func(callMaps map[string]map[string]interface{}) chan map[string]moleculer.Payload {
			c := make(chan map[string]moleculer.Payload, 1)
			c <- map[string]moleculer.Payload{}
			return c
		}
		It("should constrain the fields returned based on the fields settings", func() {
			params := payload.New(map[string]interface{}{
				"searchFields": []string{"name"},
				"search":       "John",
				"fields":       []string{"name"},
			})
			find := findAction(adapter, settings)
			rs := find(ctx.(moleculer.Context), params).(moleculer.Payload)
			Expect(rs.Len()).Should(Equal(2))
			Expect(rs.Array()[0].Get("name").String()).Should(Equal("John"))
			Expect(rs.Array()[0].Get("_id").Exists()).Should(BeFalse())
			Expect(rs.Array()[0].Get("lastname").Exists()).Should(BeFalse())
			Expect(rs.Array()[0].Get("age").Exists()).Should(BeFalse())

		})

		It("should constrain the fields returned based on the fields param", func() {
			params := payload.New(map[string]interface{}{
				"searchFields": []string{"name"},
				"search":       "John",
				"fields":       []string{"name"},
			})
			find := findAction(adapter, settings)
			rs := find(ctx.(moleculer.Context), params).(moleculer.Payload)
			Expect(rs.Len()).Should(Equal(2))
			Expect(rs.Array()[0].Get("name").String()).Should(Equal("John"))
			Expect(rs.Array()[0].Get("_id").Exists()).Should(BeFalse())
			Expect(rs.Array()[0].Get("lastname").Exists()).Should(BeFalse())
			Expect(rs.Array()[0].Get("age").Exists()).Should(BeFalse())

			params = payload.New(map[string]interface{}{
				"searchFields": []string{"name"},
				"search":       "John",
				"fields":       []string{"name", "age"},
			})
			rs = find(ctx.(moleculer.Context), params).(moleculer.Payload)
			Expect(rs.Len()).Should(Equal(2))
			Expect(rs.Array()[0].Get("name").String()).Should(Equal("John"))
			Expect(rs.Array()[0].Get("_id").Exists()).Should(BeFalse())
			Expect(rs.Array()[0].Get("lastname").Exists()).Should(BeFalse())
			Expect(rs.Array()[0].Get("age").Exists()).Should(BeTrue())
		})

		It("should populate the fields", func() {
			params := payload.New(map[string]interface{}{
				"searchFields": []string{"name"},
				"search":       "John",
				"fields":       []string{"name"},
			})
			find := findAction(adapter, settings)
			rs := find(ctx.(moleculer.Context), params).(moleculer.Payload)
			Expect(rs.Len()).Should(Equal(2))
			Expect(rs.Array()[0].Get("name").String()).Should(Equal("John"))
			Expect(rs.Array()[0].Get("_id").Exists()).Should(BeFalse())
			Expect(rs.Array()[0].Get("lastname").Exists()).Should(BeFalse())
			Expect(rs.Array()[0].Get("age").Exists()).Should(BeFalse())

			// params = payload.New(map[string]interface{}{
			// 	"searchFields": []string{"name"},
			// 	"search":       "John",
			// 	"fields":       []string{"name", "age"},
			// })
			// rs = find(ctx.(moleculer.Context), params).(moleculer.Payload)
			// Expect(rs.Len()).Should(Equal(2))
			// Expect(rs.Array()[0].Get("name").String()).Should(Equal("John"))
			// Expect(rs.Array()[0].Get("_id").Exists()).Should(BeFalse())
			// Expect(rs.Array()[0].Get("lastname").Exists()).Should(BeFalse())
			// Expect(rs.Array()[0].Get("age").Exists()).Should(BeTrue())
		})
	})

	Describe("populates", func() {

		It("actionFromPopulate should return action name from a mapping", func() {
			config := "users.get"
			action := actionFromPopulate(config)
			Expect(action).Should(Equal(config))
		})

		It("actionFromPopulate should return action name from a complex mapping", func() {
			config := map[string]interface{}{"action": "users.get"}
			action := actionFromPopulate(config)
			Expect(action).Should(Equal(config["action"]))
		})

		It("actionParamsFromPopulate should return params from a complex mapping", func() {
			config := M{"params": M{"fields": []string{"name", "email"}}}
			params := actionParamsFromPopulate(config)
			Expect(params.Exists()).Should(BeTrue())
			Expect(params.Get("fields").Exists()).Should(BeTrue())
			Expect(params.Get("fields").StringArray()).Should(Equal([]string{"name", "email"}))
		})

		It("addFieldValues should extract params for the parent records that are required to filter the child records.", func() {
			params := payload.Empty()
			item := payload.New(M{"friends": []string{"123", "213", "321"}})
			field := "friends"
			r := addFieldValues(params, item, field)
			Expect(r.Exists()).Should(BeTrue())
			Expect(r.Get("id").Exists()).Should(BeTrue())
			Expect(r.Get("id").StringArray()).Should(Equal([]string{"123", "213", "321"}))

			item = payload.New(M{"author": "123"})
			field = "author"
			r = addFieldValues(params, item, field)
			Expect(r.Exists()).Should(BeTrue())
			Expect(r.Get("id").Exists()).Should(BeTrue())
			Expect(r.Get("id").String()).Should(Equal("123"))
		})

		It("createPopulateMCalls should deal with a single result, simple config from settings", func() {

			userID := "12345"
			result := payload.New(M{
				"id":     userID,
				"master": "222",
			})
			params := payload.New(M{"": ""})
			settingsPopulates := M{"master": "users.get"}
			mcalls := createPopulateMCalls(result, params, settingsPopulates)

			r := payload.New(mcalls)
			Expect(r.Get(userID + "_master_users.get").Exists()).Should(BeTrue())
			Expect(r.Get(userID + "_master_users.get").Get("action").Exists()).Should(BeTrue())
			Expect(r.Get(userID + "_master_users.get").Get("action").String()).Should(Equal("users.get"))
			Expect(r.Get(userID + "_master_users.get").Get("params").Exists()).Should(BeTrue())
			Expect(r.Get(userID + "_master_users.get").Get("params").Get("id").Exists()).Should(BeTrue())
			Expect(r.Get(userID + "_master_users.get").Get("params").Get("id").IsArray()).Should(BeFalse())
			Expect(r.Get(userID + "_master_users.get").Get("params").Get("id").String()).Should(Equal("222"))
		})

		It("createPopulateMCalls should deal with a single result, simple config from settings, multiple ids to fetch", func() {
			userID := "12345"
			result := payload.New(M{
				"id":      userID,
				"friends": []string{"222", "333"},
			})
			params := payload.New(M{"": ""})
			settingsPopulates := M{"friends": "users.get"}
			mcalls := createPopulateMCalls(result, params, settingsPopulates)

			r := payload.New(mcalls)
			Expect(r.Get(userID + "_friends_users.get").Exists()).Should(BeTrue())
			Expect(r.Get(userID + "_friends_users.get").Get("action").Exists()).Should(BeTrue())
			Expect(r.Get(userID + "_friends_users.get").Get("action").String()).Should(Equal("users.get"))
			Expect(r.Get(userID + "_friends_users.get").Get("params").Exists()).Should(BeTrue())
			Expect(r.Get(userID + "_friends_users.get").Get("params").Get("id").Exists()).Should(BeTrue())
			Expect(r.Get(userID + "_friends_users.get").Get("params").Get("id").IsArray()).Should(BeTrue())
			Expect(r.Get(userID + "_friends_users.get").Get("params").Get("id").StringArray()).Should(Equal([]string{"222", "333"}))
		})

		It("createPopulateMCalls should deal with a multiple results, simple config from settings, multiple ids to fetch", func() {
			userID1 := "666"
			user1 := payload.New(M{
				"id":      userID1,
				"friends": []string{"222", "333"},
			})

			userID2 := "222"
			user2 := payload.New(M{
				"id":      userID2,
				"friends": []string{"666", "888"},
			})

			params := payload.New(M{"": ""})
			settingsPopulates := M{"friends": "users.get"}

			users := payload.New([]moleculer.Payload{user1, user2})
			mcalls := createPopulateMCalls(users, params, settingsPopulates)

			r := payload.New(mcalls)
			Expect(r.Get(userID1 + "_friends_users.get").Exists()).Should(BeTrue())
			Expect(r.Get(userID1 + "_friends_users.get").Get("action").Exists()).Should(BeTrue())
			Expect(r.Get(userID1 + "_friends_users.get").Get("action").String()).Should(Equal("users.get"))
			Expect(r.Get(userID1 + "_friends_users.get").Get("params").Exists()).Should(BeTrue())
			Expect(r.Get(userID1 + "_friends_users.get").Get("params").Get("id").Exists()).Should(BeTrue())
			Expect(r.Get(userID1 + "_friends_users.get").Get("params").Get("id").IsArray()).Should(BeTrue())
			Expect(r.Get(userID1 + "_friends_users.get").Get("params").Get("id").StringArray()).Should(Equal([]string{"222", "333"}))

			Expect(r.Get(userID2 + "_friends_users.get").Exists()).Should(BeTrue())
			Expect(r.Get(userID2 + "_friends_users.get").Get("action").Exists()).Should(BeTrue())
			Expect(r.Get(userID2 + "_friends_users.get").Get("action").String()).Should(Equal("users.get"))
			Expect(r.Get(userID2 + "_friends_users.get").Get("params").Exists()).Should(BeTrue())
			Expect(r.Get(userID2 + "_friends_users.get").Get("params").Get("id").Exists()).Should(BeTrue())
			Expect(r.Get(userID2 + "_friends_users.get").Get("params").Get("id").IsArray()).Should(BeTrue())
			Expect(r.Get(userID2 + "_friends_users.get").Get("params").Get("id").StringArray()).Should(Equal([]string{"666", "888"}))
		})

		It("populateSingleRecordWithResults should populate the fields with results of MCall", func() {

			populates := M{"master": "users.get"}
			result := payload.New(M{
				"id":     "12345",
				"master": "444",
			})
			calls := map[string]moleculer.Payload{
				"12345_master_users.get": payload.New(M{
					"id":   "444",
					"name": "Yoda",
				}),
			}
			r := populateSingleRecordWithResults(populates, result, calls)
			Expect(r.Exists()).Should(BeTrue())
			Expect(r.IsError()).Should(BeFalse())
			Expect(r.Get("id").String()).Should(Equal("12345"))
			Expect(r.Get("master").IsMap()).Should(BeTrue())
			Expect(r.Get("master").Get("id").String()).Should(Equal("444"))
			Expect(r.Get("master").Get("name").String()).Should(Equal("Yoda"))
		})

		It("populateRecordsWithResults should populate multiple record with the results of the populate MCall", func() {

			populates := M{"master": "users.get"}
			result := payload.New([]M{M{
				"id":     "12345",
				"master": "444",
			},
				M{
					"id":     "6789",
					"master": "555",
				}})
			calls := map[string]moleculer.Payload{
				"12345_master_users.get": payload.New(M{
					"id":   "444",
					"name": "Yoda",
				}),
				"6789_master_users.get": payload.New(M{
					"id":   "555",
					"name": "Gandalf",
				}),
			}
			r := populateRecordsWithResults(populates, result, calls)
			Expect(r.Exists()).Should(BeTrue())
			Expect(r.IsArray()).Should(BeTrue())
			Expect(r.IsError()).Should(BeFalse())
			Expect(r.First().Get("id").String()).Should(Equal("12345"))
			Expect(r.First().Get("master").IsMap()).Should(BeTrue())
			Expect(r.First().Get("master").Get("id").String()).Should(Equal("444"))
			Expect(r.First().Get("master").Get("name").String()).Should(Equal("Yoda"))

			Expect(r.Array()[1].Get("id").String()).Should(Equal("6789"))
			Expect(r.Array()[1].Get("master").IsMap()).Should(BeTrue())
			Expect(r.Array()[1].Get("master").Get("id").String()).Should(Equal("555"))
			Expect(r.Array()[1].Get("master").Get("name").String()).Should(Equal("Gandalf"))
		})
	})

	FDescribe("get action", func() {
		adapter := memoryAdapter("user", userDbSchema)
		var johnSnow, maria moleculer.Payload
		BeforeEach(func() {
			johnSnow, maria, _ = connectAndLoadUsers(adapter)
		})
		AfterEach(func() {
			adapter.Disconnect()
		})

		settings := map[string]interface{}{
			"fields":    []string{"**"},
			"populates": map[string]interface{}{},
		}

		ctx, delegates := test.ContextAndDelegated("get-test", moleculer.BrokerConfig{})
		delegates.MultActionDelegate = func(callMaps map[string]map[string]interface{}) chan map[string]moleculer.Payload {
			c := make(chan map[string]moleculer.Payload, 1)
			c <- map[string]moleculer.Payload{}
			return c
		}
		It("should get a record by a single id", func() {
			params := payload.New(map[string]interface{}{
				"ids": []string{johnSnow.Get("id").String()},
			})
			get := getAction(adapter, settings)
			rs := get(ctx.(moleculer.Context), params).(moleculer.Payload)
			Expect(rs.IsError()).Should(BeFalse())
			Expect(rs.Get("name").String()).Should(Equal(johnSnow.Get("name").String()))
			Expect(rs.Get("lastname").String()).Should(Equal(johnSnow.Get("lastname").String()))
			Expect(rs.Get("age").String()).Should(Equal(johnSnow.Get("age").String()))
		})

		FIt("should get multiple records by id", func() {
			params := payload.New(map[string]interface{}{
				"ids": []string{johnSnow.Get("id").String(), maria.Get("id").String()},
			})
			get := getAction(adapter, settings)
			rs := get(ctx.(moleculer.Context), params).(moleculer.Payload)
			Expect(rs.IsError()).Should(BeFalse())
			Expect(rs.Len()).Should(Equal(2))
			Expect(rs.Array()[0].Get("name").String()).Should(Equal(johnSnow.Get("name").String()))
			Expect(rs.Array()[0].Get("lastname").String()).Should(Equal(johnSnow.Get("lastname").String()))
			Expect(rs.Array()[0].Get("age").String()).Should(Equal(johnSnow.Get("age").String()))

			Expect(rs.Array()[1].Get("name").String()).Should(Equal(maria.Get("name").String()))
			Expect(rs.Array()[1].Get("lastname").String()).Should(Equal(maria.Get("lastname").String()))
			Expect(rs.Array()[1].Get("age").String()).Should(Equal(maria.Get("age").String()))
		})
	})

})
