package db

import (
	"context"
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

func (adapter *MongoAdapter) Find(params moleculer.Payload) moleculer.Payload {
	ctx, _ := context.WithTimeout(context.Background(), adapter.Timeout)
	cursor, err := adapter.coll.Find(ctx, params.Bson())
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
	params = payload.Add(params, map[string]interface{}{
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
	return params.Add(map[string]interface{}{
		"id": res.InsertedID,
	})
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
