package store

import (
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
	"idField": "id",

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
	Init(*log.Entry, map[string]interface{})
	Connect() error
	Disconnect() error
	Find(params moleculer.Payload) moleculer.Payload
	FindAndUpdate(params moleculer.Payload) moleculer.Payload
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

func transformResult(ctx moleculer.Context, params, result moleculer.Payload, getInstance func() *moleculer.ServiceSchema) moleculer.Payload {
	instance := getInstance()
	fields, populates := settingsDefaults(instance.Settings)
	return populateFields(ctx, constrainFields(
		result, params, fields,
	), params, populates)
}

// findAction
func findAction(adapter Adapter, getInstance func() *moleculer.ServiceSchema) moleculer.ActionHandler {
	return func(ctx moleculer.Context, params moleculer.Payload) interface{} {
		return transformResult(ctx, params, adapter.Find(params), getInstance)
	}
}

// findAndUpdateAction
func findAndUpdateAction(adapter Adapter, getInstance func() *moleculer.ServiceSchema) moleculer.ActionHandler {
	return func(ctx moleculer.Context, params moleculer.Payload) interface{} {
		return transformResult(ctx, params, adapter.FindAndUpdate(params), getInstance)
	}
}

//createAction
func createAction(adapter Adapter, getInstance func() *moleculer.ServiceSchema) moleculer.ActionHandler {
	return func(ctx moleculer.Context, params moleculer.Payload) interface{} {
		if params == nil || !params.Exists() {
			return payload.Error("params cannot be empty!")
		}
		r := adapter.Insert(params)
		if !r.IsError() {
			event := getInstance().Name + ".created"
			ctx.Broadcast(event, r.Get("id").String())
		}
		return r
	}
}

//updateAction
func updateAction(adapter Adapter, getInstance func() *moleculer.ServiceSchema) moleculer.ActionHandler {
	return func(ctx moleculer.Context, params moleculer.Payload) interface{} {
		if params == nil || !params.Exists() {
			return payload.Error("params cannot be empty!")
		}
		if !params.Get("id").Exists() {
			return payload.Error("id field required!") //TODO remove this after validator is added
		}
		r := adapter.UpdateById(params.Get("id"), params.Remove("id"))
		if !r.IsError() {
			event := getInstance().Name + ".updated"
			ctx.Broadcast(event, r.Get("id").String())
		}
		return r
	}
}

//removeAction
func removeAction(adapter Adapter, getInstance func() *moleculer.ServiceSchema) moleculer.ActionHandler {
	return func(ctx moleculer.Context, params moleculer.Payload) interface{} {
		if params == nil || !params.Exists() {
			return payload.Error("params cannot be empty!")
		}
		if !params.Get("id").Exists() {
			return payload.Error("id field required!") //TODO remove this after validator is added
		}
		r := adapter.RemoveById(params.Get("id"))
		if r.IsError() {
			return payload.Error("Could not remove record. Error: ", r.Error().Error())
		}
		event := getInstance().Name + ".removed"
		ctx.Broadcast(event, params.Get("id").String())
		return params.Add("deletedCount", r.Get("deletedCount"))
	}
}

// listAction
func listAction(adapter Adapter, getInstance func() *moleculer.ServiceSchema) moleculer.ActionHandler {
	return func(ctx moleculer.Context, params moleculer.Payload) interface{} {
		var rows moleculer.Payload
		pageSize := getInstance().Settings["pageSize"].(int)
		if params.Get("pageSize").Exists() {
			pageSize = params.Get("pageSize").Int()
		}
		page := 1
		if params.Get("page").Exists() {
			page = params.Get("page").Int()
		}

		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			limit := page * pageSize
			offset := (page - 1) * pageSize
			rows = adapter.Find(params.AddMany(map[string]interface{}{
				"limit":  limit,
				"offset": offset,
			}))
			wg.Done()
		}()
		total := adapter.Count(params)
		wg.Wait()
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
func getAction(adapter Adapter, getInstance func() *moleculer.ServiceSchema) moleculer.ActionHandler {
	return func(ctx moleculer.Context, params moleculer.Payload) interface{} {
		var result moleculer.Payload
		if params.Get("id").Exists() {
			result = adapter.FindById(params.Get("id"))
		} else if params.Get("ids").Exists() && params.Get("ids").IsArray() {
			result = adapter.FindByIds(params.Get("ids"))
		} else if params.Exists() && params.String() != "" {
			result = adapter.FindById(params)
		} else {
			return payload.Error("Invalid parameter. Action get requires the parameter id or ids!")
		}
		if result.IsError() {
			return payload.Error("Could not get record. Error: ", result.Error().Error())
		}
		return transformResult(ctx, params, result, getInstance)
	}
}

//Mixin return the Mixin schema for the Moleculer DB Service.
func Mixin(adapter Adapter) moleculer.Mixin {
	var instance *moleculer.ServiceSchema
	getInstance := func() *moleculer.ServiceSchema {
		return instance
	}
	return moleculer.Mixin{
		Name:     "db-mixin",
		Settings: defaultSettings,
		Created: func(svc moleculer.ServiceSchema, logger *log.Entry) {

		},
		Started: func(context moleculer.BrokerContext, svc moleculer.ServiceSchema) {
			instance = &svc
			if adapter == nil {
				settingsAdapter, exists := instance.Settings["db-adapter"]
				if exists {
					context.Logger().Info("db-mixin started - service: ", svc.Name, " -> adapter from settings!")
					adapter = settingsAdapter.(Adapter)
				}
			}
			if adapter != nil {
				context.Logger().Info("db-mixin started - service: ", svc.Name, " -> connecting")
				adapter.Init(context.Logger().WithField("store", "adapter"), svc.Settings)
				adapter.Connect()
				context.Logger().Info("db-mixin started - service: ", svc.Name, " -> connected!")
			}
		},
		Stopped: func(context moleculer.BrokerContext, svc moleculer.ServiceSchema) {
			if adapter != nil {
				context.Logger().Info("db-mixin stopped - service: ", svc.Name, " -> adapter.Disconnect()")
				adapter.Disconnect()
			}
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
				Handler: findAction(adapter, getInstance),
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
				Handler: listAction(adapter, getInstance),
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
				Handler: getAction(adapter, getInstance),
			},
			//create action
			{
				Name:    "create",
				Handler: createAction(adapter, getInstance),
			},
			//update action
			{
				Name: "update",
				Schema: moleculer.ObjectSchema{
					struct {
						id string
					}{},
				},
				Handler: updateAction(adapter, getInstance),
			},
			//remove action
			{
				Name: "remove",
				Schema: moleculer.ObjectSchema{
					struct {
						id string
					}{},
				},
				Handler: removeAction(adapter, getInstance),
			},
			//findAndUpdate Action
			{
				Name: "findAndUpdate",
				Settings: map[string]interface{}{
					"cache": false,
				},
				Schema: moleculer.ObjectSchema{
					struct {
						populate []string               `optional:"true"`
						fields   []string               `optional:"true"`
						limit    int                    `optional:"true" min:"0"`
						offset   int                    `optional:"true" min:"0"`
						sort     string                 `optional:"true"`
						update   map[string]interface{} `optional:"false"`
						query    map[string]interface{} `optional:"false"`
					}{},
				},
				Handler: findAndUpdateAction(adapter, getInstance),
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
	if item.IsError() {
		return item
	}
	filtered := map[string]interface{}{}
	item.ForEach(func(field interface{}, value moleculer.Payload) bool {
		if field != nil && contains(fields, field.(string)) {
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

// addIds add params to from the parent record, to filter the child records.
func addIds(params, item moleculer.Payload, field string) moleculer.Payload {
	fvalue := item.Get(field)
	if fvalue.IsArray() {
		return params.Add("ids", fvalue.StringArray())
	}
	return params.Add("id", fvalue.String())
}

// createPopulateCall add populates call to mcalls params for a given item.
func createPopulateCall(calls map[string]map[string]interface{}, item moleculer.Payload, populates map[string]interface{}, fields []string) {
	id := item.Get("id").String()
	for _, field := range fields {
		if !item.Get(field).Exists() {
			continue
		}
		config, hasConfig := populates[field]
		if !hasConfig {
			continue
		}
		action := actionFromPopulate(config)
		if action == "" {
			continue
		}
		mcallName := id + "_" + field + "_" + action
		actionParams := actionParamsFromPopulate(config)
		actionParams = addIds(actionParams, item, field)
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
func createPopulateMCalls(result, params moleculer.Payload, populates map[string]interface{}, fields []string) map[string]map[string]interface{} {
	calls := map[string]map[string]interface{}{}
	if result.IsArray() {
		result.ForEach(func(_ interface{}, item moleculer.Payload) bool {
			createPopulateCall(calls, item, populates, fields)
			return true
		})
	} else {
		createPopulateCall(calls, result, populates, fields)
	}
	return calls
}

// populateSingleRecordWithResults populate a single record with the populate values from the Mcall result.
func populateSingleRecordWithResults(populates map[string]interface{}, item moleculer.Payload, mcalls map[string]moleculer.Payload, fields []string) moleculer.Payload {
	id := item.Get("id").String()
	for _, field := range fields {
		config, hasConfig := populates[field]
		if !hasConfig {
			continue
		}
		action := actionFromPopulate(config)
		if action == "" {
			continue
		}
		mcallName := id + "_" + field + "_" + action
		populateResult := mcalls[mcallName]
		if item.Get(field).Exists() {
			item = item.Remove(field)
		}
		item = item.Add(field, populateResult.Value())
	}
	return item
}

// populateRecordsWithResults populate one record or multiple with the populatye values from the Mcall result.
func populateRecordsWithResults(populates map[string]interface{}, result moleculer.Payload, mcalls map[string]moleculer.Payload, fields []string) moleculer.Payload {
	if result.IsArray() {
		list := []moleculer.Payload{}
		result.ForEach(func(index interface{}, item moleculer.Payload) bool {
			list = append(list, populateSingleRecordWithResults(populates, item, mcalls, fields))
			return true
		})
		return payload.New(list)
	} else {
		return populateSingleRecordWithResults(populates, result, mcalls, fields)
	}
}

// populateFields populate fields on the results.
func populateFields(ctx moleculer.Context, result, params moleculer.Payload, populates map[string]interface{}) moleculer.Payload {
	if !params.Get("populate").Exists() {
		return result
	}
	var fields []string
	if params.Get("populate").IsArray() {
		fields = params.Get("populate").StringArray()
	} else {
		fields = []string{params.Get("populate").String()}
	}
	mparams := createPopulateMCalls(result, params, populates, fields)
	if len(mparams) > 0 {
		mcalls := <-ctx.MCall(mparams)
		result = populateRecordsWithResults(populates, result, mcalls, fields)
	}
	return result
}
