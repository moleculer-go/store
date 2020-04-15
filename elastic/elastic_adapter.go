package elastic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	elastic "github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/payload"
	"github.com/moleculer-go/moleculer/serializer"
	"github.com/moleculer-go/moleculer/util"
	log "github.com/sirupsen/logrus"
)

type Adapter struct {
	URIs []string
	es   *elastic.Client

	indexName string

	connected  bool
	log        *log.Entry
	settings   map[string]interface{}
	mappings   map[string]interface{}
	serializer serializer.Serializer
}

func (a *Adapter) Init(log *log.Entry, settings map[string]interface{}) {
	a.log = log
	a.settings = settings
	a.loadSettings(a.settings)
	a.serializer = serializer.CreateJSONSerializer(a.log)
}

func (a *Adapter) loadSettings(settings map[string]interface{}) {
	if uri, ok := settings["uris"].(string); ok {
		a.URIs = strings.Split(uri, ",")
	}
	if indexName, ok := settings["indexName"].(string); ok {
		a.indexName = indexName
	}
	if mappings, ok := settings["mappings"].(map[string]interface{}); ok {
		a.mappings = mappings
	}
}

func (a *Adapter) printClusterInfo() {
	// 1. Get cluster info
	res, err := a.es.Info()
	if err != nil {
		a.log.Errorln("Could not get cluster Info - source: " + err.Error())
		return
	}
	defer res.Body.Close()
	// Check response status
	if res.IsError() {
		a.log.Errorln("Response error: " + res.String())
	}
	// Deserialize the response into a map.
	var r map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		a.log.Errorln("Error parsing the response body: " + res.String())
	}

	// Print client and server version numbers.
	a.log.Infoln(strings.Repeat("~", 37))
	a.log.Infoln("Elastic Search Connected !")
	a.log.Infof("Client: %s", elastic.Version)
	a.log.Infof("Server: %s", r["version"].(map[string]interface{})["number"])
}

func (a *Adapter) Connect() error {
	es, err := elastic.NewDefaultClient()
	if err != nil {
		return errors.New("Could not create client - error: " + err.Error())
	}
	a.es = es
	a.printClusterInfo()
	err = a.setupIndex()
	return err
}

//createIndex
func (a *Adapter) createIndex() error {
	if len(a.mappings) == 0 {
		return nil
	}
	params := payload.Empty().Add("mappings", a.mappings)
	req := esapi.IndicesCreateRequest{
		Index: a.indexName,
		Body:  strings.NewReader(a.serializer.PayloadToString(params)),
	}
	res, err := req.Do(context.Background(), a.es)
	r := a.handleResponse(res, err, "Error creating index: "+a.indexName)
	if r.IsError() {
		return r.Error()
	}
	return nil
}

//updateMappings update the field mapping in the index
func (a *Adapter) updateMappings() error {
	if len(a.mappings) == 0 {
		return nil
	}
	properties := a.mappings["properties"].(map[string]interface{})
	if len(properties) == 0 {
		return nil
	}

	//for field, params := range properties.Map() {
	req := esapi.IndicesPutMappingRequest{
		Index: []string{a.indexName},
		Body:  strings.NewReader(a.serializer.PayloadToString(payload.Empty().Add("properties", properties))),
	}
	res, err := req.Do(context.Background(), a.es)
	r := a.handleResponse(res, err, "Error updating mappings for index: "+a.indexName)
	if r.IsError() {
		a.log.Error(r.Error())
		return r.Error()
	}
	return nil
}

//setupIndex tried to create the index if defined in the service settings
func (a *Adapter) setupIndex() error {
	err := a.createIndex()
	if err != nil {
		a.log.Error("Error trying to create Index - error: " + err.Error())
		err = a.updateMappings()
	}
	return err
}

func (a *Adapter) Disconnect() error {
	a.es = nil
	return nil
}

// WIP allow for defining field types
// func (a *Adapter) mappings() {
// 	req := esapi.IndicesPutMappingRequest{
// 		Index: []string{},

// 	}
// }

func (a *Adapter) indexRequest(req esapi.IndexRequest) moleculer.Payload {
	res, err := req.Do(context.Background(), a.es)
	if err != nil {
		return payload.New(err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return payload.New(errors.New("[" + res.Status() + "] Error indexing document ID=" + req.DocumentID))
	}

	result := a.serializer.ReaderToPayload(res.Body)

	a.log.Debugf("indexRequest () Status: %s - Result: %s = Version: %d", res.Status(), result.Get("result").String(), result.Get("_version").Int())

	result = result.Add("documentID", req.DocumentID)
	return result
}

//handleResponse parse the elastic response
func (a *Adapter) handleResponse(res *esapi.Response, err error, errorMsg string) moleculer.Payload {
	if err != nil {
		return payload.New(err)
	}
	defer res.Body.Close()
	r := a.serializer.ReaderToPayload(res.Body)
	if res.IsError() {
		errorMsg = errorMsg + " - root cause: " + r.Get("error").Get("root_cause").First().Get("reason").String()
		a.log.Error(errorMsg)
		a.log.Trace("Error payload: ", r)
		return payload.PayloadError(errorMsg, r)
	}
	return r
}

func (a *Adapter) Insert(params moleculer.Payload) moleculer.Payload {
	result := a.indexRequest(esapi.IndexRequest{
		Index:      a.indexName,
		DocumentID: util.RandomString(12),
		Body:       strings.NewReader(a.serializer.PayloadToString(params)),
		Refresh:    "true",
	})
	return result
}

func (a *Adapter) RemoveAll() moleculer.Payload {
	req := esapi.DeleteByQueryRequest{
		Index: []string{a.indexName},
		Body: strings.NewReader(`
		{
			"query": {
			  "match_all": {}
			}
		}`),
	}
	res, err := req.Do(context.Background(), a.es)
	return a.handleResponse(res, err, "Error deleting docs by query")
}

func (a *Adapter) RemoveById(id moleculer.Payload) moleculer.Payload {
	req := esapi.DeleteRequest{
		Index:      a.indexName,
		DocumentID: id.String(),
	}
	res, err := req.Do(context.Background(), a.es)
	return a.handleResponse(res, err, "Error deleting docs by id: "+id.String())
}

func parseSearchFields(params, query moleculer.Payload) moleculer.Payload {
	searchFields := params.Get("searchFields")
	search := params.Get("search")
	mm := payload.Empty()
	if search.Exists() {
		mm.Add("query", search.String())
	}
	if searchFields.Exists() {
		fields := searchFields.StringArray()
		mm.Add("fields", fields)
	}
	if mm.Len() > 0 {
		query = query.Add("multi_match", mm)
	} else {
		query = query.Add("match_all", payload.Empty())
	}
	return query
}

func parseQueryParams(params moleculer.Payload) moleculer.Payload {
	r := payload.Empty()
	if params.Get("limit").Exists() {
		r = r.Add("size", params.Get("limit").Int())
	}
	if params.Get("offset").Exists() {
		r = r.Add("from", params.Get("offset").Int())
	}
	if params.Get("sort").Exists() {
		sort := map[string]string{}
		if params.Get("sort").IsArray() {
			sort = sortsFromStringArray(params.Get("sort"))
		} else {
			sort = sortsFromString(params.Get("sort"))
		}
		if len(sort) > 0 {
			r = r.Add("sort", sort)
		}
	}
	return r
}

// sort sample -> "sort" : { "published" : "desc", "_doc" : "asc" }`

//sortEntry create a sort entry
func sortEntry(entry string) (field string, direction string) {
	if strings.Index(entry, "-") == 0 {
		field = strings.Replace(entry, "-", "", 1)
		direction = "desc"
	} else {
		field = entry
		direction = "asc"
	}
	return field, direction
}

//sortsFromString
func sortsFromString(sort moleculer.Payload) map[string]string {
	parts := strings.Split(strings.Trim(sort.String(), " "), " ")
	if len(parts) > 1 {
		sorts := map[string]string{}
		for _, value := range parts {
			field, direction := sortEntry(value)
			sorts[field] = direction
		}
		return sorts
	} else if len(parts) == 1 && parts[0] != "" {
		field, direction := sortEntry(parts[0])
		return map[string]string{field: direction}
	}
	fmt.Println("**** invalid Sort Entry **** ")
	return map[string]string{}
}

func sortsFromStringArray(sort moleculer.Payload) map[string]string {
	sorts := map[string]string{}
	sort.ForEach(func(index interface{}, value moleculer.Payload) bool {
		field, direction := sortEntry(value.String())
		sorts[field] = direction
		return true
	})
	return sorts
}

func parseFilter(params moleculer.Payload) moleculer.Payload {
	query := payload.Empty()
	if params.Get("query").Exists() {
		query = params.Get("query")
	}
	query = parseSearchFields(params, query)
	queryParams := parseQueryParams(params)
	return queryParams.Add("query", query)
}

func getHits(params, search moleculer.Payload) moleculer.Payload {
	return search.Get("hits").Get("hits")
}

func (a *Adapter) Find(params moleculer.Payload) moleculer.Payload {

	query := a.serializer.PayloadToString(parseFilter(params))
	a.log.Traceln("Find() params: ", params, "query: ", query)

	res, err := a.es.Search(
		a.es.Search.WithContext(context.Background()),
		a.es.Search.WithIndex(a.indexName),
		a.es.Search.WithBody(strings.NewReader(query)),
		a.es.Search.WithTrackTotalHits(true),
		a.es.Search.WithPretty(),
	)
	if err != nil {
		return payload.New(err)
	}
	defer res.Body.Close()
	p := a.serializer.ReaderToPayload(res.Body)
	if res.IsError() {
		//a.log.Error("error executing search - ", a.serializer.PayloadToString(p))
		msg := "error executing search. root cause: " + p.Get("error").Get("root_cause").First().Get("reason").String()
		a.log.Error(msg)
		return payload.Error(msg)
	}

	a.log.Traceln("search result:")
	a.log.Traceln(p)
	list := getHits(params, p)
	result := list.MapOver(func(in moleculer.Payload) moleculer.Payload {
		return in.Get("_source")
	})
	a.log.Traceln("find result transformed: ")
	a.log.Traceln(result)
	return result
}

func (a *Adapter) FindOne(params moleculer.Payload) moleculer.Payload {
	return a.Find(params.Add("limit", 1)).First()
}
