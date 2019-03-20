package db

import (
	"context"
	"fmt"
	"time"

	"github.com/moleculer-go/moleculer/payload"

	"github.com/moleculer-go/moleculer"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

//MongoAdapter Mongo DB Adapter :)
type MongoAdapter struct {
	MongoURL   string
	Timeout    time.Duration
	Database   string
	Collection string
	client     *mongo.Client
	coll       *mongo.Collection
}

// Connect connect to mongo, stores the client and the collection.
func (adapter *MongoAdapter) Connect() error {
	ctx, _ := context.WithTimeout(context.Background(), adapter.Timeout)
	var err error
	adapter.client, err = mongo.Connect(ctx, options.Client().ApplyURI(adapter.MongoURL))
	if err != nil {
		return err
	}
	err = adapter.client.Ping(ctx, readpref.Primary())
	if err != nil {
		return err
	}
	adapter.coll = adapter.client.Database(adapter.Database).Collection(adapter.Collection)
	return nil
}

// Disconnect disconnects from mongo.
func (adapter *MongoAdapter) Disconnect() error {
	ctx, _ := context.WithTimeout(context.Background(), adapter.Timeout)
	return adapter.client.Disconnect(ctx)
}

func parseSearchFields(params, query moleculer.Payload) moleculer.Payload {
	searchFields := params.Get("searchFields")
	search := params.Get("search")
	searchValue := ""
	if search.Exists() {
		searchValue = search.String()
	}
	if searchFields.Exists() {
		fields := searchFields.StringArray()
		if len(fields) == 1 {
			query = query.Add(fields[0], searchValue)
		} else if len(fields) > 1 {
			or := payload.EmptyList()
			for _, field := range fields {
				or = or.AddItem(payload.Empty().Add(field, searchValue))
			}
			query = query.Add("$or", or)
		}
	}
	return query
}

// Find search the data store with the params provided.
func (adapter *MongoAdapter) Find(params moleculer.Payload) moleculer.Payload {
	ctx, _ := context.WithTimeout(context.Background(), adapter.Timeout)

	query := payload.Empty()
	if params.Get("query").Exists() {
		query = params.Get("query")
	}
	query = parseSearchFields(params, query)

	bs := query.Bson()
	fmt.Println("Find() bs -> ", bs)
	cursor, err := adapter.coll.Find(ctx, bs)
	if err != nil {
		return payload.Create(err)
	}
	defer cursor.Close(ctx)
	list := []moleculer.Payload{}
	for cursor.Next(ctx) {
		var result bson.M
		err := cursor.Decode(&result)
		if err != nil {
			return payload.Create(err)
		}
		list = append(list, payload.Create(result))
	}
	if err := cursor.Err(); err != nil {
		return payload.Create(err)
	}
	return payload.Create(list)
}

func (adapter *MongoAdapter) FindOne(params moleculer.Payload) moleculer.Payload {
	params = params.AddMany(map[string]interface{}{
		"limit": 1,
	})
	list := adapter.Find(params).Array()
	if len(list) == 0 {
		return nil
	}
	return list[0]
}
func (adapter *MongoAdapter) FindById(params moleculer.Payload) moleculer.Payload {
	return nil
}
func (adapter *MongoAdapter) FindByIds(params moleculer.Payload) moleculer.Payload {
	return nil
}

func (adapter *MongoAdapter) Count(params moleculer.Payload) moleculer.Payload {
	return nil
}

func (adapter *MongoAdapter) Insert(params moleculer.Payload) moleculer.Payload {
	ctx, _ := context.WithTimeout(context.Background(), adapter.Timeout)
	res, err := adapter.coll.InsertOne(ctx, params.Bson())
	if err != nil {
		return payload.Error("Error while trying to insert record. Error: ", err.Error())
	}
	return params.Add("id", res.InsertedID)
}

func (adapter *MongoAdapter) Update(params moleculer.Payload) moleculer.Payload {
	return nil
}

func (adapter *MongoAdapter) UpdateById(params moleculer.Payload) moleculer.Payload {
	return nil
}
func (adapter *MongoAdapter) RemoveById(params moleculer.Payload) moleculer.Payload {
	return nil
}

func (adapter *MongoAdapter) RemoveAll() moleculer.Payload {
	ctx, _ := context.WithTimeout(context.Background(), adapter.Timeout)
	res, err := adapter.coll.DeleteMany(ctx, bson.M{})
	if err != nil {
		return payload.Error("Error while trying to remove all records. Error: ", err.Error())
	}
	return payload.Create(res.DeletedCount)
}
