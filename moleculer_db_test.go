package db

import (
	"time"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer-db/mocks"
	"github.com/moleculer-go/moleculer/context"
	"github.com/moleculer-go/moleculer/payload"
	"github.com/moleculer-go/moleculer/test"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func contextAndDelegated(nodeID string, config moleculer.Config) (moleculer.BrokerContext, *moleculer.BrokerDelegates) {
	dl := test.DelegatesWithIdAndConfig(nodeID, config)
	ctx := context.BrokerContext(dl)
	return ctx, dl
}

var _ = Describe("Moleculer DB Mixin", func() {

	Describe("list action", func() {
		adapter := &MemoryAdapter{
			Table:        "user",
			SearchFields: []string{"name"},
		}

		BeforeEach(func() {
			mocks.ConnectAndLoadUsers(adapter)
		})
		AfterEach(func() {
			adapter.Disconnect()
		})
		svc := &moleculer.ServiceSchema{
			Settings: Mixin(adapter).Settings,
		}
		ctx, _ := contextAndDelegated("list-test", moleculer.Config{})
		It("should return page, pageSize, rows, total and totalPages", func() {
			params := payload.New(map[string]interface{}{
				"searchFields": []string{"name"},
				"search":       "John",
			})
			list := listAction(adapter, func() *moleculer.ServiceSchema { return svc })
			rs := list(ctx.(moleculer.Context), params)
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
		adapter := &MemoryAdapter{
			Table:        "user",
			SearchFields: []string{"name"},
		}

		BeforeEach(func() {
			mocks.ConnectAndLoadUsers(adapter)
		})
		AfterEach(func() {
			adapter.Disconnect()
		})

		svc := &moleculer.ServiceSchema{
			Settings: map[string]interface{}{
				"fields":    []string{"**"},
				"populates": map[string]interface{}{},
			},
		}

		ctx, delegates := contextAndDelegated("find-test", moleculer.Config{})
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
			find := findAction(adapter, func() *moleculer.ServiceSchema { return svc })
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
			find := findAction(adapter, func() *moleculer.ServiceSchema { return svc })
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
			find := findAction(adapter, func() *moleculer.ServiceSchema { return svc })
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

		It("addIds should extract params for the parent records that are required to filter the child records.", func() {
			params := payload.Empty()
			item := payload.New(M{"friends": []string{"123", "213", "321"}})
			field := "friends"
			r := addIds(params, item, field)
			Expect(r.Exists()).Should(BeTrue())
			Expect(r.Get("ids").Exists()).Should(BeTrue())
			Expect(r.Get("ids").StringArray()).Should(Equal([]string{"123", "213", "321"}))

			item = payload.New(M{"author": "123"})
			field = "author"
			r = addIds(params, item, field)
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
			mcalls := createPopulateMCalls(result, params, settingsPopulates, []string{"master"})

			r := payload.New(mcalls)
			Expect(r.Get(userID + "_master_users.get").Exists()).Should(BeTrue())
			Expect(r.Get(userID + "_master_users.get").Get("action").Exists()).Should(BeTrue())
			Expect(r.Get(userID + "_master_users.get").Get("action").String()).Should(Equal("users.get"))
			Expect(r.Get(userID + "_master_users.get").Get("params").Exists()).Should(BeTrue())
			Expect(r.Get(userID + "_master_users.get").Get("params").Get("id").Exists()).Should(BeTrue())
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
			mcalls := createPopulateMCalls(result, params, settingsPopulates, []string{"friends"})

			r := payload.New(mcalls)
			Expect(r.Get(userID + "_friends_users.get").Exists()).Should(BeTrue())
			Expect(r.Get(userID + "_friends_users.get").Get("action").Exists()).Should(BeTrue())
			Expect(r.Get(userID + "_friends_users.get").Get("action").String()).Should(Equal("users.get"))
			Expect(r.Get(userID + "_friends_users.get").Get("params").Exists()).Should(BeTrue())
			Expect(r.Get(userID + "_friends_users.get").Get("params").Get("ids").Exists()).Should(BeTrue())
			Expect(r.Get(userID + "_friends_users.get").Get("params").Get("ids").IsArray()).Should(BeTrue())
			Expect(r.Get(userID + "_friends_users.get").Get("params").Get("ids").StringArray()).Should(Equal([]string{"222", "333"}))
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
			mcalls := createPopulateMCalls(users, params, settingsPopulates, []string{"friends"})

			r := payload.New(mcalls)
			Expect(r.Get(userID1 + "_friends_users.get").Exists()).Should(BeTrue())
			Expect(r.Get(userID1 + "_friends_users.get").Get("action").Exists()).Should(BeTrue())
			Expect(r.Get(userID1 + "_friends_users.get").Get("action").String()).Should(Equal("users.get"))
			Expect(r.Get(userID1 + "_friends_users.get").Get("params").Exists()).Should(BeTrue())
			Expect(r.Get(userID1 + "_friends_users.get").Get("params").Get("ids").Exists()).Should(BeTrue())
			Expect(r.Get(userID1 + "_friends_users.get").Get("params").Get("ids").IsArray()).Should(BeTrue())
			Expect(r.Get(userID1 + "_friends_users.get").Get("params").Get("ids").StringArray()).Should(Equal([]string{"222", "333"}))

			Expect(r.Get(userID2 + "_friends_users.get").Exists()).Should(BeTrue())
			Expect(r.Get(userID2 + "_friends_users.get").Get("action").Exists()).Should(BeTrue())
			Expect(r.Get(userID2 + "_friends_users.get").Get("action").String()).Should(Equal("users.get"))
			Expect(r.Get(userID2 + "_friends_users.get").Get("params").Exists()).Should(BeTrue())
			Expect(r.Get(userID2 + "_friends_users.get").Get("params").Get("ids").Exists()).Should(BeTrue())
			Expect(r.Get(userID2 + "_friends_users.get").Get("params").Get("ids").IsArray()).Should(BeTrue())
			Expect(r.Get(userID2 + "_friends_users.get").Get("params").Get("ids").StringArray()).Should(Equal([]string{"666", "888"}))
		})

		It("populateSingleRecordWithResults should populate the array fields with results of MCall", func() {

			populates := M{"friends": "users.get"}
			result := payload.New(M{
				"id":      "12345",
				"friends": []string{"444", "555"},
			})
			calls := map[string]moleculer.Payload{
				"12345_friends_users.get": payload.New([]moleculer.Payload{
					payload.New(M{
						"id":   "444",
						"name": "Yoda",
					}),
					payload.New(M{
						"id":   "555",
						"name": "Musk",
					}),
				}),
			}
			r := populateSingleRecordWithResults(populates, result, calls, []string{"friends"})
			Expect(r.Exists()).Should(BeTrue())
			Expect(r.IsError()).Should(BeFalse())
			Expect(r.Get("id").String()).Should(Equal("12345"))
			Expect(r.Get("friends").IsArray()).Should(BeTrue())
			Expect(r.Get("friends").Len()).Should(Equal(2))
			Expect(r.Get("friends").Array()[0].Get("id").String()).Should(Equal("444"))
			Expect(r.Get("friends").Array()[0].Get("name").String()).Should(Equal("Yoda"))
			Expect(r.Get("friends").Array()[1].Get("id").String()).Should(Equal("555"))
			Expect(r.Get("friends").Array()[1].Get("name").String()).Should(Equal("Musk"))
		})

		It("populateSingleRecordWithResults should populate the single fields with results of MCall", func() {

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
			r := populateSingleRecordWithResults(populates, result, calls, []string{"master"})
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
			r := populateRecordsWithResults(populates, result, calls, []string{"master"})
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

	Describe("get action", func() {
		adapter := &MemoryAdapter{
			Table:        "user",
			SearchFields: []string{"name"},
		}
		var johnSnow, maria moleculer.Payload
		BeforeEach(func() {
			johnSnow, maria, _ = mocks.ConnectAndLoadUsers(adapter)
		})
		AfterEach(func() {
			adapter.Disconnect()
		})

		svc := &moleculer.ServiceSchema{
			Settings: map[string]interface{}{
				"fields":    []string{"**"},
				"populates": map[string]interface{}{},
			},
		}
		ctx, delegates := contextAndDelegated("get-test", moleculer.Config{})
		delegates.MultActionDelegate = func(callMaps map[string]map[string]interface{}) chan map[string]moleculer.Payload {
			c := make(chan map[string]moleculer.Payload, 1)
			c <- map[string]moleculer.Payload{}
			return c
		}
		It("should get a record by a single id", func() {
			params := payload.New(map[string]interface{}{
				"id": johnSnow.Get("id").String(),
			})
			get := getAction(adapter, func() *moleculer.ServiceSchema { return svc })
			rs := get(ctx.(moleculer.Context), params).(moleculer.Payload)
			Expect(rs.IsError()).Should(BeFalse())
			Expect(rs.Get("name").String()).Should(Equal(johnSnow.Get("name").String()))
			Expect(rs.Get("lastname").String()).Should(Equal(johnSnow.Get("lastname").String()))
			Expect(rs.Get("age").String()).Should(Equal(johnSnow.Get("age").String()))
		})

		It("should get multiple records by id", func() {
			params := payload.New(map[string]interface{}{
				"ids": []string{johnSnow.Get("id").String(), maria.Get("id").String()},
			})
			get := getAction(adapter, func() *moleculer.ServiceSchema { return svc })
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

	Describe("create action", func() {
		adapter := &MemoryAdapter{
			Table:        "user",
			SearchFields: []string{"name"},
		}
		ctx, delegates := contextAndDelegated("create-test", moleculer.Config{})
		var broadCastReceived moleculer.BrokerContext
		delegates.BroadcastEvent = func(context moleculer.BrokerContext) {
			broadCastReceived = context
		}
		BeforeEach(func() {
			adapter.Connect()
		})

		AfterEach(func() {
			adapter.Disconnect()
		})

		It("should faild with empty params", func() {
			create := createAction(adapter, func() *moleculer.ServiceSchema { return &moleculer.ServiceSchema{} })
			r := create(ctx.(moleculer.Context), payload.New(nil)).(moleculer.Payload)
			Expect(r.IsError()).Should(BeTrue())
			Expect(r.Error().Error()).Should(Equal("params cannot be empty!"))

			r = create(ctx.(moleculer.Context), nil).(moleculer.Payload)
			Expect(r.IsError()).Should(BeTrue())
			Expect(r.Error().Error()).Should(Equal("params cannot be empty!"))
		})

		It("should create a record and find by id", func() {
			params := payload.New(map[string]interface{}{
				"name":     "Michael",
				"lastname": "Jackson",
			})
			create := createAction(adapter, func() *moleculer.ServiceSchema { return &moleculer.ServiceSchema{} })
			r := create(ctx.(moleculer.Context), params).(moleculer.Payload)
			Expect(r.IsError()).Should(BeFalse())
			Expect(r.Get("id").Exists()).Should(BeTrue())
			Expect(r.Get("name").String()).Should(Equal("Michael"))
			Expect(r.Get("lastname").String()).Should(Equal("Jackson"))

			time.Sleep(time.Millisecond * 100)
			Expect(broadCastReceived).ShouldNot(BeNil())
			Expect(broadCastReceived.Payload().String()).Should(Equal(r.Get("id").String()))

			fr := adapter.FindById(r.Get("id"))
			Expect(fr.Get("name").String()).Should(Equal("Michael"))
			Expect(fr.Get("lastname").String()).Should(Equal("Jackson"))
		})

	})

	Describe("update action", func() {
		adapter := &MemoryAdapter{
			Table:        "user",
			SearchFields: []string{"name"},
		}
		ctx, delegates := contextAndDelegated("create-test", moleculer.Config{})
		var broadCastReceived moleculer.BrokerContext
		delegates.BroadcastEvent = func(context moleculer.BrokerContext) {
			broadCastReceived = context
		}
		var johnSnow moleculer.Payload
		BeforeEach(func() {
			johnSnow, _, _ = mocks.ConnectAndLoadUsers(adapter)
		})

		AfterEach(func() {
			adapter.Disconnect()
		})
		update := updateAction(adapter, func() *moleculer.ServiceSchema { return &moleculer.ServiceSchema{} })

		It("should fail when missing id param", func() {
			r := update(ctx.(moleculer.Context), payload.New(map[string]interface{}{"name": "Santa"})).(moleculer.Payload)
			Expect(r.IsError()).Should(BeTrue())
			Expect(r.Error().Error()).Should(Equal("id field required!"))
		})

		It("should fail when missing params", func() {
			r := update(ctx.(moleculer.Context), nil).(moleculer.Payload)
			Expect(r.IsError()).Should(BeTrue())
			Expect(r.Error().Error()).Should(Equal("params cannot be empty!"))

			r = update(ctx.(moleculer.Context), payload.New(nil)).(moleculer.Payload)
			Expect(r.IsError()).Should(BeTrue())
			Expect(r.Error().Error()).Should(Equal("params cannot be empty!"))
		})

		It("should update a record and find by id", func() {
			params := payload.New(map[string]interface{}{
				"id":       johnSnow.Get("id").String(),
				"lastname": "Stark",
			})
			r := update(ctx.(moleculer.Context), params).(moleculer.Payload)
			Expect(r.IsError()).Should(BeFalse())
			Expect(r.Get("id").Exists()).Should(BeTrue())
			Expect(r.Get("name").String()).Should(Equal("John"))
			Expect(r.Get("lastname").String()).Should(Equal("Stark"))

			time.Sleep(time.Millisecond * 100)
			Expect(broadCastReceived).ShouldNot(BeNil())
			Expect(broadCastReceived.Payload().String()).Should(Equal(r.Get("id").String()))

			fr := adapter.FindById(johnSnow.Get("id"))
			Expect(fr.Get("name").String()).Should(Equal("John"))
			Expect(fr.Get("lastname").String()).Should(Equal("Stark"))
		})

	})

	Describe("removed action", func() {
		adapter := &MemoryAdapter{
			Table:        "user",
			SearchFields: []string{"name"},
		}
		ctx, delegates := contextAndDelegated("create-test", moleculer.Config{})
		var broadCastReceived moleculer.BrokerContext
		delegates.BroadcastEvent = func(context moleculer.BrokerContext) {
			broadCastReceived = context
		}
		var johnSnow, marie, johnT moleculer.Payload
		BeforeEach(func() {
			johnSnow, marie, johnT = mocks.ConnectAndLoadUsers(adapter)
			Expect(johnSnow.IsError()).Should(BeFalse())
			Expect(marie.IsError()).Should(BeFalse())
			Expect(johnT.IsError()).Should(BeFalse())
		})

		AfterEach(func() {
			adapter.Disconnect()
		})
		remove := removeAction(adapter, func() *moleculer.ServiceSchema { return &moleculer.ServiceSchema{} })

		It("should fail when missing id param", func() {
			r := remove(ctx.(moleculer.Context), payload.New(map[string]interface{}{})).(moleculer.Payload)
			Expect(r.IsError()).Should(BeTrue())
			Expect(r.Error().Error()).Should(Equal("id field required!"))
		})

		It("should fail when missing params", func() {
			r := remove(ctx.(moleculer.Context), nil).(moleculer.Payload)
			Expect(r.IsError()).Should(BeTrue())
			Expect(r.Error().Error()).Should(Equal("params cannot be empty!"))

			r = remove(ctx.(moleculer.Context), payload.New(nil)).(moleculer.Payload)
			Expect(r.IsError()).Should(BeTrue())
			Expect(r.Error().Error()).Should(Equal("params cannot be empty!"))
		})

		It("should remove a record", func() {
			total := adapter.Count(payload.Empty()).Int()
			params := payload.New(map[string]interface{}{
				"id": johnSnow.Get("id").String(),
			})
			r := remove(ctx.(moleculer.Context), params).(moleculer.Payload)
			Expect(r.IsError()).Should(BeFalse())
			Expect(r.Get("id").Exists()).Should(BeTrue())
			Expect(r.Get("deletedCount").Int()).Should(Equal(1))

			time.Sleep(time.Millisecond * 100)
			Expect(broadCastReceived).ShouldNot(BeNil())
			Expect(broadCastReceived.Payload().String()).Should(Equal(r.Get("id").String()))

			ct := adapter.Count(payload.Empty()).Int()
			Expect(ct).Should(Equal(total - 1))

			fr := adapter.FindById(johnSnow.Get("id"))
			Expect(fr.Exists()).Should(BeFalse())

			//marie
			params = payload.New(map[string]interface{}{
				"id": marie.Get("id").String(),
			})
			r = remove(ctx.(moleculer.Context), params).(moleculer.Payload)
			Expect(r.IsError()).Should(BeFalse())
			Expect(r.Get("id").Exists()).Should(BeTrue())
			Expect(r.Get("deletedCount").Int()).Should(Equal(1))

			time.Sleep(time.Millisecond * 100)
			Expect(broadCastReceived).ShouldNot(BeNil())
			Expect(broadCastReceived.Payload().String()).Should(Equal(r.Get("id").String()))

			ct = adapter.Count(payload.Empty()).Int()
			Expect(ct).Should(Equal(total - 2))

			fr = adapter.FindById(marie.Get("id"))
			Expect(fr.Exists()).Should(BeFalse())

			//johnT
			params = payload.New(map[string]interface{}{
				"id": johnT.Get("id").String(),
			})
			r = remove(ctx.(moleculer.Context), params).(moleculer.Payload)
			Expect(r.IsError()).Should(BeFalse())
			Expect(r.Get("id").Exists()).Should(BeTrue())
			Expect(r.Get("deletedCount").Int()).Should(Equal(1))

			time.Sleep(time.Millisecond * 100)
			Expect(broadCastReceived).ShouldNot(BeNil())
			Expect(broadCastReceived.Payload().String()).Should(Equal(r.Get("id").String()))

			ct = adapter.Count(payload.Empty()).Int()
			Expect(ct).Should(Equal(total - 3))

			fr = adapter.FindById(johnT.Get("id"))
			Expect(fr.Exists()).Should(BeFalse())
		})

	})

})
