package elastic

import (
	"context"
	"encoding/json"
	"errors"
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
}

func (a *Adapter) printClusterInfo() {
	// 1. Get cluster info
	res, err := a.es.Info()
	if err != nil {
		a.log.Errorln("Could not get cluser Info - source: " + err.Error())
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
	a.log.Printf("Client: %s", elastic.Version)
	a.log.Printf("Server: %s", r["version"].(map[string]interface{})["number"])
	a.log.Println(strings.Repeat("~", 37))
	a.log.Println("Elastic Search Connected !")
}

func (a *Adapter) Connect() error {
	es, err := elastic.NewDefaultClient()
	if err != nil {
		return errors.New("Could not client - source: " + err.Error())
	}
	a.es = es
	a.printClusterInfo()
	return nil
}

func (a *Adapter) Disconnect() error {
	return nil
}

func (a *Adapter) esRequest(req esapi.IndexRequest) moleculer.Payload {
	res, err := req.Do(context.Background(), a.es)
	if err != nil {
		return payload.New(err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return payload.New(errors.New("[" + res.Status() + "] Error indexing document ID=" + req.DocumentID))
	}

	result := a.serializer.ReaderToPayload(res.Body)

	a.log.Debugf("esRequest () Status: %s - Result: %s = Version: %d", res.Status(), result.Get("result").String(), result.Get("_version").Int())

	result = result.Add("documentID", req.DocumentID)
	return result
}

func (a *Adapter) Insert(params moleculer.Payload) moleculer.Payload {
	result := a.esRequest(esapi.IndexRequest{
		Index:      a.indexName,
		DocumentID: util.RandomString(12),
		Body:       strings.NewReader(a.serializer.PayloadToString(params)),
		Refresh:    "true",
	})
	return result
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

func parseQueryparams(params moleculer.Payload) moleculer.Payload {
	r := payload.Empty()
	if params.Get("limit").Exists() {
		r = r.Add("size", params.Get("limit").Int())
	}
	if params.Get("offset").Exists() {
		r = r.Add("from", params.Get("offset").Int())
	}
	// if params.Get("sort").Exists() {
	// 	if param.Get("sort").IsArray() {
	// 		sort = sortsFromStringArray(param.Get("sort"))
	// 	} else {
	// 		sort = sortsFromString(param.Get("sort"))
	// 	}
	// }
	return r
}

func parseFilter(params moleculer.Payload) moleculer.Payload {
	query := payload.Empty()
	if params.Get("query").Exists() {
		query = params.Get("query")
	}
	query = parseSearchFields(params, query)
	queryParams := parseQueryparams(params)
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
		a.log.Error("error executing search - ", p.RawMap())
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
