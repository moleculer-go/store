package db

import (
	"math"
	"sync"

	"github.com/moleculer-go/moleculer"
	log "github.com/sirupsen/logrus"
)

var pageSize = 10
var maxPageSize = 100
var maxLimit = -1

var defaultSettings = map[string]interface{}{
	//idField : Name of ID field.
	"idField": "_id",

	//fields : Field filtering list. It must be an `Array`. If the value is `null` or `undefined` doesn't filter the fields of entities.
	"fields": []string{"**"},

	//populates : Schema for population. [Read more](#populating).
	"populates": []interface{}{},

	//pageSize : Default page size in `list` action.
	"pageSize": pageSize,

	//maxPageSize : Maximum page size in `list` action.
	"maxPageSize": maxPageSize,

	//*maxLimit : Maximum value of limit in `find` action. Default: `-1` (no limit)
	"maxLimit": maxLimit,

	//entityValidator : Validator schema or a function to validate the incoming entity in `create` & 'insert' actions.
	"entityValidator": nil,

	//db-adapter : database specific adaptor. Example mongodb-adaptor.
	"db-adapter": NotDefinedAdapter{},
}

type Adapter interface {
	Connect() error
	Disconnect() error
	Find(params moleculer.Payload) moleculer.Payload
	FindOne(params moleculer.Payload) moleculer.Payload
	FindById(params moleculer.Payload) moleculer.Payload
	FindByIds(params moleculer.Payload) moleculer.Payload
	Count(params moleculer.Payload) moleculer.Payload
	Insert(params moleculer.Payload) moleculer.Payload
	Update(params moleculer.Payload) moleculer.Payload
	UpdateById(params moleculer.Payload) moleculer.Payload
	RemoveById(params moleculer.Payload) moleculer.Payload
}

//Service create the Mixin schema for the Moleculer DB Service.
func Service(adapter Adapter) moleculer.Mixin {
	serviceSettings := defaultSettings
	return moleculer.Mixin{
		Name:     "db-mixin",
		Settings: defaultSettings,
		Created: func(svc moleculer.Service, logger *log.Entry) {

		},
		Started: func(context moleculer.BrokerContext, svc moleculer.Service) {
			serviceSettings = svc.Settings
			if adapter != nil {
				context.Logger().Debug("db-mixin started. adapter was provided on higher function!")
				return
			}
			settingsAdapter, exists := serviceSettings["db-adapter"]
			if !exists {
				return
			}
			context.Logger().Debug("db-mixin started. adapter from settings!")
			adapter = settingsAdapter.(Adapter)

		},
		Stopped: func(context moleculer.BrokerContext, svc moleculer.Service) {

		},
		Actions: []moleculer.Action{
			//find action
			{
				Name: "find",
				Settings: map[string]interface{}{
					"cache": map[string]interface{}{
						"keys": []string{"populate", "fields", "limit", "offset", "sort", "search", "searchFields", "query"},
					},
				},
				Schema: moleculer.ObjectSchema{
					struct {
						populate     []string               `optional:"true"`
						fields       []string               `optional:"true"`
						limit        int                    `optional:"true" min:"0"`
						offset       int                    `optional:"true" min:"0"`
						sort         string                 `optional:"true"`
						search       string                 `optional:"true"`
						searchFields []string               `optional:"true"`
						query        map[string]interface{} `optional:"true"`
					}{},
				},
				Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
					return constrainFields(
						adapter.Find(params), params, serviceSettings["fields"].([]string),
					)
				},
			},

			//count action
			{
				Name: "count",
				Settings: map[string]interface{}{
					"cache": map[string]interface{}{
						"keys": []string{"search", "searchFields", "query"},
					},
				},
				Schema: moleculer.ObjectSchema{
					struct {
						search       string                 `optional:"true"`
						searchFields []string               `optional:"true"`
						query        map[string]interface{} `optional:"true"`
					}{},
				},
				Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
					return adapter.Count(params)
				},
			},

			//list action
			{
				Name: "list",
				Settings: map[string]interface{}{
					"cache": map[string]interface{}{
						"keys": []string{"populate", "fields", "page", "pageSize", "sort", "search", "searchFields", "query"},
					},
				},
				Schema: moleculer.ObjectSchema{
					struct {
						populate     []string               `optional:"true"`
						fields       []string               `optional:"true"`
						page         int                    `optional:"true" min:"0"`
						pageSize     int                    `optional:"true" min:"0"`
						sort         string                 `optional:"true"`
						search       string                 `optional:"true"`
						searchFields []string               `optional:"true"`
						query        map[string]interface{} `optional:"true"`
					}{},
				},
				Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
					var rows moleculer.Payload
					wg := sync.WaitGroup{}
					wg.Add(1)
					go func() {
						rows = adapter.Find(params)
						wg.Done()
					}()
					total := adapter.Count(params)
					wg.Wait()

					pageSize := serviceSettings["pageSize"].(int)
					if params.Get("pageSize").Exists() {
						pageSize = params.Get("pageSize").Int()
					}
					page := 1
					if params.Get("page").Exists() {
						page = params.Get("page").Int()
					}

					totalPages := math.Floor(
						(total.Float() + float64(pageSize) - 1.0) / float64(pageSize))

					return map[string]interface{}{
						"rows":       rows,
						"total":      total,
						"page":       page,
						"pageSize":   pageSize,
						"totalPages": totalPages,
					}
				},
			},

			//create action
			{
				Name: "create",
				Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {

					return nil
				},
			},
		},
	}
}

// constrainFields limits the fields in the paylod to the ondes specified in the fields settings.
// first checks on the action, otherwise use the default.
func constrainFields(result, params moleculer.Payload, defaultFields []string) moleculer.Payload {
	//TODO
	return result
}
