package stores

import (
	"errors"
	"fmt"
	"strings"

	"github.com/hashicorp/go-memdb"
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/payload"
	"github.com/moleculer-go/moleculer/util"
	log "github.com/sirupsen/logrus"
)

//MemoryAdapter stores data in memory!
type MemoryAdapter struct {
	//Schema *memdb.DBSchema
	SearchFields []string
	Table        string
	db           *memdb.MemDB
	logger       *log.Entry
}

func (adapter *MemoryAdapter) Init(logger *log.Entry, settings map[string]interface{}) {
	adapter.logger = logger
}

func (adapter *MemoryAdapter) generateSchema() *memdb.DBSchema {

	Indexes := map[string]*memdb.IndexSchema{
		"id": &memdb.IndexSchema{
			Name:    "id",
			Unique:  true,
			Indexer: &PayloadIndex{Field: "id"},
		},
		"all": &memdb.IndexSchema{
			Name:    "all",
			Unique:  false,
			Indexer: &PayloadIndex{Field: "all"},
		},
	}
	if adapter.SearchFields != nil {
		for _, field := range adapter.SearchFields {
			Indexes[field] = &memdb.IndexSchema{
				Name:    field,
				Unique:  false,
				Indexer: &PayloadIndex{Field: field},
			}
		}
	}
	return &memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			adapter.Table: &memdb.TableSchema{
				Name:    adapter.Table,
				Indexes: Indexes,
			},
		},
	}
}

func (adapter *MemoryAdapter) Connect() error {
	schema := adapter.generateSchema()
	db, err := memdb.NewMemDB(schema)
	if err != nil {
		return err
	}
	adapter.db = db
	return nil
}

func (adapter *MemoryAdapter) Disconnect() error {
	adapter.db = nil
	return nil
}

func (adapter *MemoryAdapter) Find(params moleculer.Payload) moleculer.Payload {
	searchFields := []string{"all"}
	search := "*"
	if params.Get("searchFields").Exists() {
		searchFields = params.Get("searchFields").StringArray()
	}
	if params.Get("search").Exists() {
		search = params.Get("search").String()
	}

	indexName := strings.Join(searchFields, "-")
	tx := adapter.db.Txn(false)
	defer tx.Abort()
	results, err := tx.Get(adapter.Table, indexName, search)
	if err != nil {
		return payload.Error("Failed trying to find. Error: ", err.Error())
	}
	items := []moleculer.Payload{}
	for {
		value := results.Next()
		if value == nil {
			break
		}
		items = append(items, payload.New(value))
	}
	return payload.New(items)
}

func (adapter *MemoryAdapter) FindOne(params moleculer.Payload) moleculer.Payload {
	indexName := strings.Join(params.Get("searchFields").StringArray(), "-")
	search := params.Get("search").String()
	tx := adapter.db.Txn(false)
	defer tx.Abort()
	result, err := tx.First(adapter.Table, indexName, search)
	if err != nil {
		return payload.Error("Failed trying to findOne. searchFields: ", indexName, " search: ", search, " Error: ", err.Error())
	}
	return payload.New(result)
}

func (adapter *MemoryAdapter) FindById(params moleculer.Payload) moleculer.Payload {
	findParams := payload.New(map[string]interface{}{
		"searchFields": []string{"id"},
		"search":       params.String(),
	})
	return adapter.FindOne(findParams)
}

func (adapter *MemoryAdapter) FindByIds(params moleculer.Payload) moleculer.Payload {
	ids := params.StringArray()
	list := []moleculer.Payload{}
	for _, id := range ids {
		list = append(list, adapter.FindById(payload.New(id)))
	}
	return payload.New(list)
}

func (adapter *MemoryAdapter) Count(params moleculer.Payload) moleculer.Payload {
	result := adapter.Find(params)
	return payload.New(result.Len())
}

func (adapter *MemoryAdapter) Insert(params moleculer.Payload) moleculer.Payload {
	params = params.AddMany(map[string]interface{}{
		"id":  util.RandomString(12),
		"all": "*",
	})
	tx := adapter.db.Txn(true)
	err := tx.Insert(adapter.Table, params)
	if err != nil {
		defer tx.Abort()
		return payload.Error("Failed trying to Insert. Error: ", err.Error())
	}
	defer tx.Commit()
	return params
}

func (adapter *MemoryAdapter) Update(params moleculer.Payload) moleculer.Payload {
	one := adapter.FindById(params.Get("id"))
	if !one.IsError() && one.Exists() {
		tx := adapter.db.Txn(true)
		err := tx.Delete(adapter.Table, one.Value())
		if err != nil {
			defer tx.Abort()
			return payload.Error("Failed trying to update record. source error: ", err.Error())
		}
		rec := one.AddMany(params.RawMap())
		err = tx.Insert(adapter.Table, rec)
		if err != nil {
			defer tx.Abort()
			return payload.Error("Failed trying to update record. source error: ", err.Error())
		}
		defer tx.Commit()
		return rec
	}
	return payload.Error("Failed trying to update record. Could not find record with id: ", params.Get("id").String())
}

func (adapter *MemoryAdapter) UpdateById(id, params moleculer.Payload) moleculer.Payload {
	return adapter.Update(params.Add("id", id))
}

func (adapter *MemoryAdapter) RemoveById(params moleculer.Payload) moleculer.Payload {
	one := adapter.FindById(params)
	if !one.IsError() && one.Exists() {
		tx := adapter.db.Txn(true)
		err := tx.Delete(adapter.Table, one.Value())
		if err != nil {
			defer tx.Abort()
			return payload.Error("Failed trying to removed record. source error: ", err.Error())
		}
		defer tx.Commit()
		return payload.Empty().Add("deletedCount", 1)
	}
	return nil
}

func (adapter *MemoryAdapter) RemoveAll() moleculer.Payload {
	items := adapter.Count(payload.New(nil))
	if items.IsError() {
		return items
	}
	adapter.Disconnect()
	adapter.Connect()
	return items
}

type PayloadIndex struct {
	Field     string
	Lowercase bool
}

func (s *PayloadIndex) FromArgs(args ...interface{}) ([]byte, error) {
	key := ""
	for _, item := range args {
		s, ok := item.(string)
		if !ok {
			return nil, errors.New("Indexer can only handler string arguments.")
		}
		if key != "" {
			key = key + "-"
		}
		key = key + s
	}
	if s.Lowercase {
		key = strings.ToLower(key)
	}
	key += "\x00"
	return []byte(key), nil
}

func (s *PayloadIndex) FromObject(obj interface{}) (bool, []byte, error) {
	p, isPayload := obj.(moleculer.Payload)
	m, isMap := obj.(map[string]interface{})
	if !isPayload && !isMap {
		return false, nil, errors.New("Invalid type. It must be moleculer.Payload!")
	}
	if isMap {
		p = payload.New(m)
	}
	if !p.Get(s.Field).Exists() {
		return false, nil, errors.New(fmt.Sprint("Field `", s.Field, "` not found!"))
	}
	svalue := p.Get(s.Field).String()
	if s.Lowercase {
		svalue = strings.ToLower(svalue)
	}
	svalue += "\x00"
	return true, []byte(svalue), nil
}
