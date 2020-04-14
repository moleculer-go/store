package elastic

import (
	"github.com/moleculer-go/moleculer/payload"
	"github.com/moleculer-go/moleculer/util"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
)

var _ = Describe("Elastic", func() {

	log.SetLevel(log.InfoLevel)
	log.SetFormatter(&log.TextFormatter{
		ForceColors:      true,
		DisableTimestamp: true,
	})
	logger := log.WithFields(log.Fields{
		"Elastic": "Tests !",
	})

	It("should connect to local host", func() {
		adapter := Adapter{}
		adapter.Init(logger, nil)
		err := adapter.Connect()
		Expect(err).Should(BeNil())
		Expect(adapter.es).ShouldNot(BeNil())
	})

	It("should insert a document", func() {
		adapter := Adapter{}
		adapter.Init(logger, map[string]interface{}{
			"indexName": "insert_test_index",
		})
		err := adapter.Connect()
		Expect(err).Should(BeNil())
		adapter.RemoveAll()

		p := payload.Empty().Add("field", "content")
		r := adapter.Insert(p)
		Expect(r).ShouldNot(BeNil())
		Expect(r.Error()).Should(Succeed())

		Expect(adapter.es).ShouldNot(BeNil())
	})

	It("will insert 2 docs and should get one document that matches the search", func() {
		adapter := Adapter{}
		adapter.Init(logger, map[string]interface{}{
			"indexName": "get_test_index",
		})
		adapter.Connect()
		adapter.RemoveAll()

		content := util.RandomString(12)
		name := util.RandomString(12)

		adapter.Insert(payload.Empty().Add("field", content).Add("name", "jose"))

		adapter.Insert(payload.Empty().Add("field", "content").Add("name", name))

		r := adapter.Find(payload.New(map[string]interface{}{
			"search":       content,
			"searchFields": []string{"field"},
		}))

		Expect(r).ShouldNot(BeNil())
		Expect(r.Error()).Should(Succeed())
		Expect(r.Len()).Should(Equal(1))

		r = adapter.Find(payload.New(map[string]interface{}{
			"search":       name,
			"searchFields": []string{"name"},
		}))

		Expect(r).ShouldNot(BeNil())
		Expect(r.Error()).Should(Succeed())
		Expect(r.Len()).Should(Equal(1))
	})

	It("will insert 2 docs and should return just one using the limit parameter", func() {
		adapter := Adapter{}
		adapter.Init(logger, map[string]interface{}{
			"indexName": "limit_test_index",
		})
		adapter.Connect()
		adapter.RemoveAll()

		adapter.Insert(payload.Empty().Add("content", util.RandomString(12)).Add("name", "limited"))
		adapter.Insert(payload.Empty().Add("content", util.RandomString(12)).Add("name", "limited"))

		r := adapter.Find(payload.New(map[string]interface{}{
			"search":       "limited",
			"searchFields": []string{"name"},
			"limit":        1,
		}))

		Expect(r).ShouldNot(BeNil())
		Expect(r.Error()).Should(Succeed())
		Expect(r.Len()).Should(Equal(1))
	})

	It("Find should return just one record when limit = 1", func() {
		adapter := Adapter{}
		adapter.Init(logger, map[string]interface{}{
			"indexName": "limit_test_index",
		})
		adapter.Connect()
		adapter.RemoveAll()

		adapter.Insert(payload.Empty().Add("content", util.RandomString(12)).Add("name", "limited"))
		adapter.Insert(payload.Empty().Add("content", util.RandomString(12)).Add("name", "limited"))

		r := adapter.Find(payload.New(map[string]interface{}{
			"search":       "limited",
			"searchFields": []string{"name"},
			"limit":        1,
		}))

		Expect(r).ShouldNot(BeNil())
		Expect(r.Error()).Should(Succeed())
		Expect(r.Len()).Should(Equal(1))
	})

	It("parseQueryParams should parse sort parameter", func() {
		out := parseQueryParams(payload.Empty().Add("sort", "-id name"))
		Expect(out.Get("sort").Get("id").String()).Should(Equal("desc"))
		Expect(out.Get("sort").Get("name").String()).Should(Equal("asc"))
	})

	It("Find should respect offset and limit", func() {
		adapter := Adapter{}
		adapter.Init(logger, map[string]interface{}{
			"indexName": "find_offset_test_index",
		})
		adapter.Connect()

		adapter.Insert(payload.Empty().Add("id", 1).Add("name", "1"))
		adapter.Insert(payload.Empty().Add("id", 2).Add("name", "2"))
		adapter.Insert(payload.Empty().Add("id", 3).Add("name", "3"))
		adapter.Insert(payload.Empty().Add("id", 4).Add("name", "4"))
		adapter.Insert(payload.Empty().Add("id", 5).Add("name", "5"))
		adapter.Insert(payload.Empty().Add("id", 6).Add("name", "6"))

		r := adapter.Find(payload.New(map[string]interface{}{
			"offset": 1,
			"limit":  2,
		}))
		Expect(r.Len()).Should(Equal(2))
		Expect(r.Array()[0].Get("name").String()).Should(Equal("2"))
		Expect(r.Array()[1].Get("name").String()).Should(Equal("3"))

		r = adapter.Find(payload.New(map[string]interface{}{
			"offset": 2,
			"limit":  3,
		}))
		Expect(r.Len()).Should(Equal(3))
		Expect(r.Array()[0].Get("name").String()).Should(Equal("3"))
		Expect(r.Array()[1].Get("name").String()).Should(Equal("4"))

		r = adapter.Find(payload.New(map[string]interface{}{
			"offset": 3,
			"limit":  2,
		}))
		Expect(r.Len()).Should(Equal(2))
		Expect(r.Array()[0].Get("name").String()).Should(Equal("4"))
		Expect(r.Array()[1].Get("name").String()).Should(Equal("5"))

		r = adapter.Find(payload.New(map[string]interface{}{
			"offset": 4,
			"limit":  2,
		}))
		Expect(r.Len()).Should(Equal(2))
		Expect(r.Array()[0].Get("name").String()).Should(Equal("5"))
		Expect(r.Array()[1].Get("name").String()).Should(Equal("6"))
	})

	It("Find should respect sort params", func() {
		adapter := Adapter{}
		adapter.Init(logger, map[string]interface{}{
			"indexName": "find_sort_test_index",
		})
		adapter.Connect()
		adapter.RemoveAll()

		adapter.Insert(payload.Empty().Add("id", 1).Add("name", "a"))
		adapter.Insert(payload.Empty().Add("id", 2).Add("name", "b"))
		adapter.Insert(payload.Empty().Add("id", 2).Add("name", "c"))
		adapter.Insert(payload.Empty().Add("id", 4).Add("name", "d"))
		adapter.Insert(payload.Empty().Add("id", 5).Add("name", "e"))
		adapter.Insert(payload.Empty().Add("id", 6).Add("name", "f"))

		r := adapter.Find(payload.New(map[string]interface{}{
			"sort": "-id",
		}))
		Expect(r.Len()).Should(Equal(6))
		Expect(r.Array()[0].Get("name").String()).Should(Equal("f"))
		Expect(r.Array()[1].Get("name").String()).Should(Equal("e"))
		Expect(r.Array()[5].Get("name").String()).Should(Equal("a"))

		r = adapter.Find(payload.New(map[string]interface{}{
			"sort": "id",
		}))
		Expect(r.Len()).Should(Equal(6))
		Expect(r.Array()[0].Get("name").String()).Should(Equal("a"))
		Expect(r.Array()[1].Get("name").String()).Should(Equal("b"))
		Expect(r.Array()[5].Get("name").String()).Should(Equal("f"))

		//Waiting for feature to defined the indexed fields for a given entity
		// r = adapter.Find(payload.New(map[string]interface{}{
		// 	"sort": "-name",
		// }))
		// Expect(r.Len()).Should(Equal(6))
		// Expect(r.Array()[0].Get("name").String()).Should(Equal("f"))
		// Expect(r.Array()[1].Get("name").String()).Should(Equal("e"))
		// Expect(r.Array()[5].Get("name").String()).Should(Equal("a"))

	})
})
