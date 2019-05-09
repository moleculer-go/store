package store

import (
	"os"

	"github.com/moleculer-go/cupaloy/v2"
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/payload"
	"github.com/moleculer-go/store/mocks"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var snap = cupaloy.New(cupaloy.FailOnUpdate(os.Getenv("UPDATE_SNAPSHOTS") == ""))

var _ = Describe("MemoryAdapter", func() {

	adapter := &MemoryAdapter{
		Table:        "user",
		SearchFields: []string{"name"},
	}

	var johnSnow, johnTravolta moleculer.Payload
	BeforeEach(func() {
		johnSnow, _, johnTravolta = mocks.ConnectAndLoadUsers(adapter)
	})

	AfterEach(func() {
		johnTravolta = nil
		adapter.Disconnect()
	})

	It("Find() should return matching records", func() {
		r := adapter.Find(payload.New(map[string]interface{}{
			"searchFields": []string{"name"},
			"search":       "John",
		}))
		Expect(r.Error()).Should(BeNil())
		Expect(r.Len()).Should(Equal(2))
		Expect(snap.SnapshotMulti("Find()", r.Remove("id", "friends", "master").Sort("name"))).Should(Succeed())
	})

	It("FindById() should return one matching records by ID", func() {
		r := adapter.FindById(johnSnow.Get("id"))
		Expect(r.Error()).Should(BeNil())
		Expect(snap.SnapshotMulti("FindById()", r.Remove("id", "friends"))).Should(Succeed())
	})

	It("FindByIds() should return one matching records by ID", func() {
		r := adapter.FindByIds(payload.EmptyList().AddItem(johnSnow.Get("id")).AddItem(johnTravolta.Get("id")))
		Expect(r.Error()).Should(BeNil())
		Expect(r.Len()).Should(Equal(2))
		Expect(snap.SnapshotMulti("FindByIds()", r.Remove("id", "friends", "master"))).Should(Succeed())
	})

	It("Count() should return matching records", func() {
		r := adapter.Count(payload.New(map[string]interface{}{
			"searchFields": []string{"name"},
			"search":       "John",
		}))
		Expect(r.Error()).Should(BeNil())
		Expect(r.Int()).Should(Equal(2))
		Expect(snap.SnapshotMulti("Count()", r)).Should(Succeed())
	})

	It("Update() should update existing record matching records", func() {
		r := adapter.Update(payload.New(map[string]interface{}{
			"id":  johnTravolta.Get("id").String(),
			"age": 67,
		}))
		Expect(r.Error()).Should(BeNil())
		Expect(r.Get("name").String()).Should(Equal("John"))
		Expect(r.Get("lastname").String()).Should(Equal("Travolta"))
		Expect(r.Get("age").Int()).Should(Equal(67))
	})

	It("Insert() should insert new records", func() {
		r := adapter.Insert(payload.New(map[string]interface{}{
			"name":     "Julio",
			"lastname": "Cesar",
		}))
		Expect(r.Error()).Should(BeNil())
		Expect(r.Get("name").String()).Should(Equal("Julio"))
		Expect(r.Get("lastname").String()).Should(Equal("Cesar"))

		r = adapter.Find(payload.New(map[string]interface{}{
			"searchFields": []string{"name"},
			"search":       "Julio",
		}))
		Expect(r.Error()).Should(BeNil())
		Expect(r.Len()).Should(Equal(1))
		Expect(snap.SnapshotMulti("Insert()", r.Remove("id"))).Should(Succeed())
	})

	It("RemoveAll() should remove all records and return total of removed items", func() {
		total := adapter.Count(payload.Empty())
		Expect(total.Int()).Should(Equal(6))

		count := adapter.RemoveAll()
		Expect(count.Error()).Should(BeNil())
		Expect(count.Int()).Should(Equal(6))

		total = adapter.Count(payload.Empty())
		Expect(total.Int()).Should(Equal(0))
	})

})
