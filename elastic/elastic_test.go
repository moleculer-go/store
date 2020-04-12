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

		p := payload.Empty().Add("field", "content")
		r := adapter.Insert(p)
		Expect(r).ShouldNot(BeNil())
		Expect(r.Error()).Should(Succeed())

		Expect(adapter.es).ShouldNot(BeNil())
	})

	It("should get a document", func() {
		adapter := Adapter{}
		adapter.Init(logger, map[string]interface{}{
			"indexName": "get_test_index",
		})
		adapter.Connect()

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
})
