package sqlite

import (
	"fmt"

	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqlitex"

	"github.com/moleculer-go/moleculer"

	"github.com/moleculer-go/moleculer/payload"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
)

type M map[string]interface{}

func must(e error) {
	if e != nil {
		panic(e)
	}
}

func countTable(adapter *Adapter, table string) int {
	conn := adapter.getConn()
	defer adapter.returnConn(conn)

	count := 0
	err := sqlitex.Exec(conn, "SELECT COUNT(*) as count FROM "+table, func(stmt *sqlite.Stmt) error {
		count = int(stmt.GetInt64("count"))
		return nil
	})
	must(err)
	return count
}

var _ = Describe("Sqlite", func() {

	logLevel := log.ErrorLevel
	It("should create, init connect and disconnect adapter", func() {
		adapter := Adapter{
			URI:   "file:memory:?mode=memory",
			Table: "session",
			Columns: []Column{
				{
					Name: "code",
					Type: "string",
				},
			},
		}
		adapter.Init(log.WithField("", ""), M{})
		Expect(adapter.log).ShouldNot(BeNil())
		Expect(adapter.Connect()).Should(Succeed())
		Expect(adapter.Disconnect()).Should(Succeed())
	})

	It("should create an adapter with default idField = id", func() {
		adapter := Adapter{
			URI:      "file:memory:?mode=memory",
			Flags:    0,
			PoolSize: 1,
			Table:    "session",
			Columns: []Column{
				{
					Name: "code",
					Type: "string",
				},
			},
		}
		adapter.Init(log.WithField("", ""), M{})
		Expect(adapter.Connect()).Should(Succeed())

		rec := adapter.Insert(payload.New(M{
			"code": "asdasd",
		}))
		Expect(rec).ShouldNot(BeNil())
		Expect(rec.Get("id").Exists()).Should(BeTrue())
		Expect(rec.Get("id").Int()).Should(Equal(1))
		Expect(adapter.Disconnect()).Should(Succeed())
	})

	It("should create an adapter with custom idField", func() {
		log.SetLevel(logLevel)

		adapter := Adapter{
			URI:      "file:memory:?mode=memory",
			Flags:    0,
			PoolSize: 1,
			Table:    "session",
			Columns: []Column{
				{
					Name: "code",
					Type: "string",
				},
			},
		}
		adapter.Init(log.WithField("", ""), M{"idField": "customIdField"})

		Expect(adapter.Connect()).Should(Succeed())

		rec := adapter.Insert(payload.New(M{
			"code": "asdasd",
		}))
		Expect(rec).ShouldNot(BeNil())
		Expect(rec.Get("customIdField").Exists()).Should(BeTrue())
		Expect(rec.Get("customIdField").Int()).Should(Equal(1))
		Expect(adapter.Disconnect()).Should(Succeed())
	})

	Describe("Insert, find, delete", func() {

		var adapter Adapter
		table := "users"
		var marie moleculer.Payload
		BeforeEach(func() {
			adapter = Adapter{
				URI:      "file:memory:?mode=memory",
				Flags:    0,
				PoolSize: 1,
				Table:    table,
				Columns: []Column{
					{
						Name: "name",
						Type: "TEXT",
					},
					{
						Name: "email",
						Type: "TEXT",
					},
					{
						Name: "number",
						Type: "NUMERIC",
					},
					{
						Name: "integer",
						Type: "INTEGER",
					},
				},
			}
			log.SetLevel(logLevel)
			adapter.Init(log.WithField("", ""), M{})
			adapter.Connect()
			marie = adapter.Insert(payload.New(M{
				"name":    "Marie",
				"email":   "marie@jane.com",
				"number":  5.44444,
				"integer": 200,
			}))
		})
		AfterEach(func() {
			adapter.Disconnect()
		})

		It("should insert a record", func() {
			r := adapter.Insert(payload.New(M{
				"name":    "John",
				"email":   "john@snow.com",
				"number":  15.5,
				"integer": 10,
			}))
			Expect(r).ShouldNot(BeNil())
			count := countTable(&adapter, "users")
			Expect(count).Should(Equal(2))
		})

		It("should find a record using query", func() {
			r := adapter.Find(payload.New(M{
				"query": M{"name": "Marie"},
			}))
			Expect(r).ShouldNot(BeNil())
			Expect(r.Len()).Should(Equal(1))
			Expect(r.First().Get("id").Int()).Should(Equal(1))
			Expect(r.First().Get("name").String()).Should(Equal("Marie"))
			Expect(r.First().Get("email").String()).Should(Equal("marie@jane.com"))
			Expect(r.First().Get("number").Float()).Should(Equal(float64(5.44444)))
			Expect(r.First().Get("integer").Int()).Should(Equal(200))
		})

		It("should find one record", func() {
			r := adapter.FindOne(payload.New(M{
				"query": M{"name": "Marie"},
			}))
			Expect(r).ShouldNot(BeNil())
			Expect(r.Get("id").Int()).Should(Equal(1))
			Expect(r.Get("name").String()).Should(Equal("Marie"))
			Expect(r.Get("email").String()).Should(Equal("marie@jane.com"))
			Expect(r.Get("number").Float()).Should(Equal(float64(5.44444)))
			Expect(r.Get("integer").Int()).Should(Equal(200))
		})

		It("should find by id ", func() {
			r := adapter.FindById(payload.New(1))
			Expect(r).ShouldNot(BeNil())
			Expect(r.Get("id").Int()).Should(Equal(1))
			Expect(r.Get("name").String()).Should(Equal("Marie"))
			Expect(r.Get("email").String()).Should(Equal("marie@jane.com"))
			Expect(r.Get("number").Float()).Should(Equal(float64(5.44444)))
			Expect(r.Get("integer").Int()).Should(Equal(200))
		})

		It("should FindByIds", func() {

			r := adapter.Insert(payload.New(M{
				"name":  "Mountain",
				"email": "mountain@dew.com",
			}))

			ids := payload.EmptyList().AddItem(1).AddItem(r.Get("id"))

			list := adapter.FindByIds(ids)
			Expect(list.Len()).Should(Equal(2))
			Expect(list.First().Get("id").Int()).Should(Equal(1))
			Expect(list.First().Get("name").String()).Should(Equal("Marie"))
			Expect(list.First().Get("email").String()).Should(Equal("marie@jane.com"))
			Expect(list.First().Get("number").Float()).Should(Equal(float64(5.44444)))
			Expect(list.First().Get("integer").Int()).Should(Equal(200))

			Expect(list.Array()[1].Get("id").Int()).Should(Equal(2))
			Expect(list.Array()[1].Get("name").String()).Should(Equal("Mountain"))
			Expect(list.Array()[1].Get("email").String()).Should(Equal("mountain@dew.com"))
		})

		It("should update a record", func() {
			r := adapter.Update(payload.New(M{
				"id":    1,
				"email": "changed@mail.com",
			}))
			Expect(r).ShouldNot(BeNil())
			Expect(r.Get("email").String()).Should(Equal("changed@mail.com"))

		})

		It("should updateById a record", func() {
			r := adapter.UpdateById(payload.New(1), payload.New(M{
				"name":    "Vick",
				"email":   "changed@mail.com",
				"number":  456756.45676,
				"integer": 21321322,
			}))
			Expect(r).ShouldNot(BeNil())
			Expect(r.Get("name").String()).Should(Equal("Vick"))
			Expect(r.Get("email").String()).Should(Equal("changed@mail.com"))
			Expect(r.Get("number").Float()).Should(Equal(456756.45676))
			Expect(r.Get("integer").Int()).Should(Equal(21321322))
		})

		It("should delete a record", func() {
			count := countTable(&adapter, "users")
			Expect(count).Should(Equal(1))

			r := adapter.RemoveById(marie.Get("id"))
			count = countTable(&adapter, "users")
			Expect(count).Should(Equal(0))
			Expect(r).ShouldNot(BeNil())
			Expect(r.Get("deletedCount").Int()).Should(Equal(1))
		})

	})

	Describe("Find options", func() {

		var adapter Adapter
		BeforeEach(func() {
			adapter = Adapter{
				URI:      "file:memory:?mode=memory",
				Flags:    0,
				PoolSize: 1,
				Table:    "testFind",
				Columns: []Column{
					{
						Name: "name",
						Type: "TEXT",
					},
					{
						Name: "email",
						Type: "TEXT",
					},
				},
			}
			log.SetLevel(logLevel)
			adapter.Init(log.WithField("", ""), M{})
			adapter.Connect()
			adapter.Insert(payload.New(map[string]string{
				"name":  "Jackson",
				"email": "Jackson@five.com",
			}))

			adapter.Insert(payload.New(map[string]string{
				"name":  "Michael",
				"email": "michael@jackson.com",
			}))

			adapter.Insert(payload.New(map[string]string{
				"name":  "Mario",
				"email": "mario@silva.com",
			}))

			adapter.Insert(payload.New(map[string]string{
				"name":  "Anderson",
				"email": "Zabib",
			}))

			adapter.Insert(payload.New(map[string]string{
				"name":  "Connor",
				"email": "connor@mc.com",
			}))

			adapter.Insert(payload.New(map[string]string{
				"name":  "Zabib",
				"email": "zabib@nmgv.com",
			}))

		})

		AfterEach(func() {
			adapter.Disconnect()
		})

		It("should find all", func() {
			r := adapter.Find(payload.Empty())
			Expect(r).ShouldNot(BeNil())
			Expect(r.Len() > 0).Should(BeTrue())
		})

		It("should Find with limit", func() {
			r := adapter.Find(payload.New(map[string]interface{}{
				"limit": 2,
			}))
			Expect(r.Len()).Should(Equal(2))

			r = adapter.Find(payload.New(map[string]interface{}{
				"limit": 3,
			}))
			Expect(r.Len()).Should(Equal(3))

			r = adapter.Find(payload.New(map[string]interface{}{
				"limit": 4,
			}))
			Expect(r.Len()).Should(Equal(4))

			r = adapter.Find(payload.New(map[string]interface{}{
				"limit": 5,
			}))
			Expect(r.Len()).Should(Equal(5))
		})

		It("should Find with offset", func() {
			r := adapter.Find(payload.New(map[string]interface{}{
				"offset": 1,
				"limit":  2,
			}))
			Expect(r.Len()).Should(Equal(2))
			Expect(r.Array()[0].Get("id").Int()).Should(Equal(2))
			Expect(r.Array()[1].Get("id").Int()).Should(Equal(3))

			r = adapter.Find(payload.New(map[string]interface{}{
				"offset": 2,
				"limit":  3,
			}))
			Expect(r.Len()).Should(Equal(3))
			Expect(r.Array()[0].Get("id").Int()).Should(Equal(3))
			Expect(r.Array()[1].Get("id").Int()).Should(Equal(4))

			r = adapter.Find(payload.New(map[string]interface{}{
				"offset": 3,
				"limit":  2,
			}))
			Expect(r.Len()).Should(Equal(2))
			Expect(r.Array()[0].Get("id").Int()).Should(Equal(4))
			Expect(r.Array()[1].Get("id").Int()).Should(Equal(5))

			r = adapter.Find(payload.New(map[string]interface{}{
				"offset": 4,
				"limit":  2,
			}))
			Expect(r.Len()).Should(Equal(2))
			Expect(r.Array()[0].Get("id").Int()).Should(Equal(5))
			Expect(r.Array()[1].Get("id").Int()).Should(Equal(6))
		})

		It("should Find with sort", func() {
			r := adapter.Find(payload.New(map[string]interface{}{
				"sort": "name",
			}))
			Expect(r.Len()).Should(Equal(6))
			Expect(r.Array()[0].Get("name").String()).Should(Equal("Anderson"))
			Expect(r.Array()[1].Get("name").String()).Should(Equal("Connor"))

			r = adapter.Find(payload.New(map[string]interface{}{
				"sort": "-name",
			}))
			Expect(r.Len()).Should(Equal(6))
			Expect(r.Array()[0].Get("name").String()).Should(Equal("Zabib"))
			Expect(r.Array()[1].Get("name").String()).Should(Equal("Michael"))

			r = adapter.Find(payload.New(map[string]interface{}{
				"sort": "-id name",
			}))
			Expect(r.Len()).Should(Equal(6))
			Expect(r.Array()[0].Get("name").String()).Should(Equal("Zabib"))
			Expect(r.Array()[1].Get("name").String()).Should(Equal("Connor"))
		})

		It("should Find with searchFields", func() {
			r := adapter.Find(payload.New(map[string]interface{}{
				"search":       "Zabib",
				"searchFields": []string{"name", "email"},
			}))
			Expect(r.Len()).Should(Equal(2))
			Expect(r.Array()[0].Get("name").String()).Should(Equal("Anderson"))
			Expect(r.Array()[1].Get("name").String()).Should(Equal("Zabib"))
		})

		It("should Count the number of records", func() {
			r := adapter.Count(payload.Empty())
			Expect(r.Int()).Should(Equal(6))
		})

		It("should RemoveAll remove all records", func() {
			r := adapter.Count(payload.Empty())
			Expect(r.Int()).Should(Equal(6))

			r = adapter.RemoveAll()
			Expect(r.Get("deletedCount").Int()).Should(Equal(6))

			r = adapter.Count(payload.Empty())
			Expect(r.Int()).Should(Equal(0))
		})

	})

	Describe("Find advanced queries / filters", func() {
		var adapter Adapter
		BeforeEach(func() {
			adapter = Adapter{
				URI:      "file:memory:?mode=memory",
				Flags:    0,
				PoolSize: 1,
				Table:    "advancedFilters",
				Columns: []Column{
					{
						Name: "name",
						Type: "TEXT",
					},
					{
						Name: "email",
						Type: "TEXT",
					},
					{
						Name: "age",
						Type: "INTEGER",
					},
					{
						Name: "letter",
						Type: "TEXT",
					},
				},
			}
			log.SetLevel(logLevel)
			adapter.Init(log.WithField("", ""), M{})
			adapter.Connect()
			adapter.Insert(payload.New(M{
				"name":   "Jackson",
				"email":  "Jackson@five.com",
				"age":    5,
				"letter": "J",
			}))

			adapter.Insert(payload.New(M{
				"name":   "Michael",
				"email":  "michael@five.com",
				"age":    35,
				"letter": "M",
			}))

			adapter.Insert(payload.New(M{
				"name":   "Mario",
				"email":  "mario@silva.com",
				"age":    37,
				"letter": "M",
			}))

			adapter.Insert(payload.New(M{
				"name":   "Anderson",
				"email":  "Zabib@ufc.com",
				"age":    15,
				"letter": "A",
			}))

			adapter.Insert(payload.New(M{
				"name":   "Connor",
				"email":  "connor@ufc.com",
				"letter": "C",
			}))

			adapter.Insert(payload.New(M{
				"name":   "Zabib",
				"email":  "zabib@nmgv.com",
				"age":    28,
				"letter": "Z",
			}))

		})

		AfterEach(func() {
			adapter.Disconnect()
		})

		It("should find people with age bellow 20", func() {
			r := adapter.Find(payload.New(M{"query": M{"age": M{"<": 20}}}))
			Expect(r).ShouldNot(BeNil())
			Expect(r.Error()).Should(BeNil())
			Expect(r.Len()).Should(Equal(2))
		})

		It("should find people with age bellow or equal to 28", func() {
			r := adapter.Find(payload.New(M{"query": M{"age": M{"<=": 28}}}))
			Expect(r).ShouldNot(BeNil())
			Expect(r.Error()).Should(BeNil())
			Expect(r.Len()).Should(Equal(3))
		})

		It("should find people with age above 30", func() {
			r := adapter.Find(payload.New(M{"query": M{"age": M{">": 30}}}))
			Expect(r).ShouldNot(BeNil())
			Expect(r.Error()).Should(BeNil())
			Expect(r.Len()).Should(Equal(2))
		})

		It("should find people with age between 15 and 36", func() {
			r := adapter.Find(payload.New(M{"query": M{"age": M{"between": []int{15, 36}}}}))
			Expect(r).ShouldNot(BeNil())
			Expect(r.Error()).Should(BeNil())
			Expect(r.Len()).Should(Equal(3))
		})

		It("should find people with letter between A and Z", func() {
			r := adapter.Find(payload.New(M{"query": M{"letter": M{"between": []string{"A", "Z"}}}}))
			Expect(r).ShouldNot(BeNil())
			Expect(r.Error()).Should(BeNil())
			Expect(r.Len()).Should(Equal(6))
		})

		It("should find people with letter between B and M", func() {
			r := adapter.Find(payload.New(M{"query": M{"letter": M{"between": []string{"B", "M"}}}}))
			Expect(r).ShouldNot(BeNil())
			Expect(r.Error()).Should(BeNil())
			Expect(r.Len()).Should(Equal(4))
		})

		It("should find people with letter NOT between B and M", func() {
			r := adapter.Find(payload.New(M{"query": M{"letter": M{"not between": []string{"B", "M"}}}}))
			Expect(r).ShouldNot(BeNil())
			Expect(r.Error()).Should(BeNil())
			fmt.Println("not between rows: ", r)
			Expect(r.Len()).Should(Equal(2))
		})

		It("should find people with letter NOT between B and M OR @ufc email", func() {
			r := adapter.Find(payload.New(M{"query": M{"or": []M{
				M{"letter": M{"not between": []string{"B", "M"}}},
				M{"email": M{"like": "%@ufc%"}},
			}}}))
			Expect(r).ShouldNot(BeNil())
			Expect(r.Error()).Should(BeNil())
			Expect(r.Len()).Should(Equal(3))

			r = adapter.Find(payload.New(M{"query": M{"or": []M{
				M{"name": "Mario"},
				M{"name": "Anderson"},
			}}}))
			Expect(r).ShouldNot(BeNil())
			Expect(r.Error()).Should(BeNil())
			Expect(r.Len()).Should(Equal(2))
		})

		It("should find people with @ufc email", func() {
			r := adapter.Find(payload.New(M{"query": M{"email": M{"like": "%@ufc%"}}}))
			Expect(r).ShouldNot(BeNil())
			Expect(r.Error()).Should(BeNil())
			Expect(r.Len()).Should(Equal(2))
		})

		It("should find people with @five.com email", func() {
			r := adapter.Find(payload.New(M{"query": M{"email": M{"like": "%@five.com"}}}))
			Expect(r).ShouldNot(BeNil())
			Expect(r.Error()).Should(BeNil())
			Expect(r.Len()).Should(Equal(2))
		})

		It("should find people with age in the list of 5, 35, 37 and 200", func() {
			r := adapter.Find(payload.New(M{"query": M{"age": M{"in": []int{5, 35, 37, 200}}}}))
			Expect(r).ShouldNot(BeNil())
			Expect(r.Error()).Should(BeNil())
			Expect(r.Len()).Should(Equal(3))
		})

		It("should find people with age NOT in the list of 35, 37 and 200", func() {
			r := adapter.Find(payload.New(M{"query": M{"age": M{"not in": []int{5, 35, 37, 200}}}}))
			Expect(r).ShouldNot(BeNil())
			Expect(r.Error()).Should(BeNil())
			Expect(r.Len()).Should(Equal(2))
		})

		It("should find people with letter in the list of M, J and Y", func() {
			r := adapter.Find(payload.New(M{"query": M{"letter": M{"in": []string{"M", "J", "Y"}}}}))
			Expect(r).ShouldNot(BeNil())
			Expect(r.Error()).Should(BeNil())
			Expect(r.Len()).Should(Equal(3))
		})

		It("should find people with letter different than M", func() {
			r := adapter.Find(payload.New(M{"query": M{"letter": M{"<>": "M"}}}))
			Expect(r).ShouldNot(BeNil())
			Expect(r.Error()).Should(BeNil())
			Expect(r.Len()).Should(Equal(4))

			r = adapter.Find(payload.New(M{"query": M{"letter": M{"!=": "J"}}}))
			Expect(r).ShouldNot(BeNil())
			Expect(r.Error()).Should(BeNil())
			Expect(r.Len()).Should(Equal(5))
		})

		It("should find people where email is not null", func() {
			r := adapter.Find(payload.New(M{"query": M{"age": "is not null"}}))
			Expect(r).ShouldNot(BeNil())
			Expect(r.Error()).Should(BeNil())
			Expect(r.Len()).Should(Equal(5))

			r = adapter.Find(payload.New(M{"query": M{"age": "IS NULL"}}))
			Expect(r).ShouldNot(BeNil())
			Expect(r.Error()).Should(BeNil())
			Expect(r.Len()).Should(Equal(1))
		})

	})
})
