package db

import (
	"fmt"
	"math"
	"sync"

	"github.com/moleculer-go/moleculer/payload"

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
	"populates": map[string]interface{}{},

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
	UpdateById(id, update moleculer.Payload) moleculer.Payload
	RemoveById(id moleculer.Payload) moleculer.Payload
	RemoveAll() moleculer.Payload
}

// settingsDefaults extract defauylt settings values for fields and populates
func settingsDefaults(settings map[string]interface{}) (fields []string, populates map[string]interface{}) {
	fields, hasFields := settings["fields"].([]string)
	if !hasFields {
		fields = []string{"**"}
	}
	populates, hasPopulates := settings["populates"].(map[string]interface{})
	if !hasPopulates {
		populates = map[string]interface{}{}
	}
	return fields, populates
}

// findAction
func findAction(adapter Adapter, settings map[string]interface{}) moleculer.ActionHandler {
	fields, populates := settingsDefaults(settings)
	return func(ctx moleculer.Context, params moleculer.Payload) interface{} {
		return populateFields(ctx, constrainFields(
			adapter.Find(params), params, fields,
		), params, populates)
	}
}

func listAction(adapter Adapter, serviceSettings map[string]interface{}) moleculer.ActionHandler {
	return func(ctx moleculer.Context, params moleculer.Payload) interface{} {
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
	}
}

// getAction
func getAction(adapter Adapter, settings map[string]interface{}) moleculer.ActionHandler {
	fields, populates := settingsDefaults(settings)
	return func(ctx moleculer.Context, params moleculer.Payload) interface{} {
		var result moleculer.Payload
		if params.Get("ids").Len() == 1 {
			result = adapter.FindById(params.Get("ids").First())
		} else if params.Get("ids").Len() > 1 {
			result = adapter.FindByIds(params.Get("ids"))
		}
		if result.IsError() {
			return result
		}
		return populateFields(ctx, constrainFields(
			result, params, fields,
		), params, populates)
	}
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
				Handler: findAction(adapter, serviceSettings),
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
				Handler: listAction(adapter, serviceSettings),
			},

			//get action
			{
				Name: "get",
				Settings: map[string]interface{}{
					"cache": map[string]interface{}{
						"keys": []string{"populate", "fields", "id", "mapping"},
					},
				},
				Schema: moleculer.ObjectSchema{
					struct {
						populate []string `optional:"true"`
						fields   []string `optional:"true"`
						ids      []string
						mapping  bool `optional:"true"`
					}{},
				},
				Handler: getAction(adapter, serviceSettings),
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

func contains(search []string, item string) bool {
	for _, sitem := range search {
		if item == sitem || sitem == "**" {
			return true
		}
	}
	return false
}

func constrainFields(result, params moleculer.Payload, fields []string) moleculer.Payload {
	if params.Get("fields").Exists() && params.Get("fields").IsArray() {
		fields = params.Get("fields").StringArray()
	}
	if result.IsArray() {
		list := []moleculer.Payload{}
		result.ForEach(func(index interface{}, item moleculer.Payload) bool {
			list = append(list, constrainFieldsSingleRecords(item, fields))
			return true
		})
		return payload.New(list)
	} else {
		return constrainFieldsSingleRecords(result, fields)
	}
}

// constrainFields limits the fields in the paylod to the ones specified in the fields settings.
// first checks on the action param fields, otherwise use the default from the settings.
func constrainFieldsSingleRecords(item moleculer.Payload, fields []string) moleculer.Payload {
	filtered := map[string]interface{}{}
	fmt.Println("item ", item)
	item.ForEach(func(field interface{}, value moleculer.Payload) bool {
		fmt.Println("value ", value)
		if contains(fields, field.(string)) {
			filtered[field.(string)] = value.Value()
		}
		return true
	})
	return payload.New(filtered)
}

// actionParamsFromPopulate extracts the action params from the populates config
func actionParamsFromPopulate(config interface{}) moleculer.Payload {
	pconfig := payload.New(config)
	if pconfig.IsMap() && pconfig.Get("params").Exists() {
		return pconfig.Get("params")
	}
	return payload.Empty()
}

// actionFromPopulate extracts the action name from the populates config
func actionFromPopulate(config interface{}) string {
	pconfig := payload.New(config)
	if pconfig.IsMap() && pconfig.Get("action").Exists() {
		return pconfig.Get("action").String()
	}
	return pconfig.String()
}

// addFieldValues add params to from the parent record, to filter the child records.
func addFieldValues(params, item moleculer.Payload, field string) moleculer.Payload {
	fvalue := item.Get(field)
	if fvalue.IsArray() {
		return params.Add("id", fvalue.StringArray())
	}
	return params.Add("id", fvalue.String())
}

// createPopulateCall add populates call to mcalls params for a given item.
func createPopulateCall(calls map[string]map[string]interface{}, item moleculer.Payload, populates map[string]interface{}) {
	id := item.Get("id").String()
	for field, config := range populates {
		action := actionFromPopulate(config)
		if action == "" {
			continue
		}
		mcallName := id + "_" + field + "_" + action
		actionParams := actionParamsFromPopulate(config)
		actionParams = addFieldValues(actionParams, item, field)
		calls[mcallName] = map[string]interface{}{
			"action": action,
			"params": actionParams,
		}
	}
}

//scenarios
//list of ids
// user.friends = ["id_1", "id_2", "id_3"]
//by the id of the owner
// user.id is the filter
// user.comments -> loaded from the comments service. comments.byUserId
// if is a list of users.. then collect all ids and make a single call.
func createPopulateMCalls(result, params moleculer.Payload, populates map[string]interface{}) map[string]map[string]interface{} {
	if params.Get("populates").Exists() && params.Get("populates").IsMap() {
		populates = params.Get("populates").RawMap()
	}
	calls := map[string]map[string]interface{}{}
	if result.IsArray() {
		result.ForEach(func(_ interface{}, item moleculer.Payload) bool {
			createPopulateCall(calls, item, populates)
			return true
		})
	} else {
		createPopulateCall(calls, result, populates)
	}
	return calls
}

// populateSingleRecordWithResults populate a single record with the populate values from the Mcall result.
func populateSingleRecordWithResults(populates map[string]interface{}, item moleculer.Payload, mcalls map[string]moleculer.Payload) moleculer.Payload {
	id := item.Get("id").String()
	for field, config := range populates {
		action := actionFromPopulate(config)
		if action == "" {
			continue
		}
		mcallName := id + "_" + field + "_" + action
		populateResult := mcalls[mcallName]
		item = item.Add(field, populateResult)
	}
	return item
}

// populateRecordsWithResults populate one record or multiple with the populatye values from the Mcall result.
func populateRecordsWithResults(populates map[string]interface{}, result moleculer.Payload, mcalls map[string]moleculer.Payload) moleculer.Payload {
	if result.IsArray() {
		list := []moleculer.Payload{}
		result.ForEach(func(index interface{}, item moleculer.Payload) bool {
			list = append(list, populateSingleRecordWithResults(populates, item, mcalls))
			return true
		})
		return payload.New(list)
	} else {
		return populateSingleRecordWithResults(populates, result, mcalls)
	}
}

// populateFields populate fields on the results.
func populateFields(ctx moleculer.Context, result, params moleculer.Payload, populates map[string]interface{}) moleculer.Payload {
	mcalls := <-ctx.MCall(createPopulateMCalls(result, params, populates))
	populateRecordsWithResults(populates, result, mcalls)
	return result
}
