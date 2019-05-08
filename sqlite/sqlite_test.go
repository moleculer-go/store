package sqlite

import (
	"github.com/moleculer-go/sqlite"
	"github.com/moleculer-go/sqlite/sqlitex"

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

func countTable(adapter *SQLiteAdapter, table string) int {
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

	It("should create, init connect and disconnect adapter", func() {
		adapter := SQLiteAdapter{
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
		Expect(adapter.log).ShouldNot(BeNil())
		Expect(adapter.Connect()).Should(Succeed())
		Expect(adapter.Disconnect()).Should(Succeed())
	})

	It("should create an adapter with default idField = id", func() {
		adapter := SQLiteAdapter{
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
		log.SetLevel(log.DebugLevel)

		adapter := SQLiteAdapter{
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

		var adapter SQLiteAdapter
		table := "users"
		var marie moleculer.Payload
		BeforeEach(func() {
			adapter = SQLiteAdapter{
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
			log.SetLevel(log.DebugLevel)
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

		It("should Find with limit", func() {
			//TODO
		})

		It("should Find with offset", func() {
			//TODO
		})

		It("should Find with sort", func() {
			//TODO
		})

		It("should Find with searchFields", func() {
			//TODO
		})

		It("should Find with searchFields", func() {
			//TODO
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

})
