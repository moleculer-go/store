package db

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/moleculer-go/moleculer/payload"

	"github.com/moleculer-go/moleculer"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
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
				bm := bson.M{}
				bm[field] = searchValue
				or = or.AddItem(bm)
			}
			query = query.Add("$or", or)
		}
	}
	return query
}

func parseFindOptions(params moleculer.Payload) *options.FindOptions {
	opts := options.FindOptions{}
	limit := params.Get("limit")
	offset := params.Get("offset")
	sort := params.Get("sort")
	if limit.Exists() {
		v := limit.Int64()
		opts.Limit = &v
	}
	if offset.Exists() {
		v := offset.Int64()
		opts.Skip = &v
	}
	if sort.Exists() {
		if sort.IsArray() {
			opts.Sort = sortsFromStringArray(sort)
		} else {
			opts.Sort = sortsFromString(sort)
		}

	}
	return &opts
}

func sortEntry(entry string) primitive.E {
	item := primitive.E{entry, 1}
	if strings.Index(entry, "-") == 0 {
		entry = strings.Replace(entry, "-", "", 1)
		item = primitive.E{entry, -1}
	}
	return item
}

func sortsFromString(sort moleculer.Payload) primitive.D {
	parts := strings.Split(strings.Trim(sort.String(), " "), " ")
	if len(parts) > 1 {
		sorts := primitive.D{}
		for _, value := range parts {
			item := sortEntry(value)
			sorts = append(sorts, item)
		}
		return sorts
	} else if len(parts) == 1 && parts[0] != "" {
		return bson.D{sortEntry(parts[0])}
	}
	fmt.Println("**** invalid Sort Entry **** ")
	return nil

}

func sortsFromStringArray(sort moleculer.Payload) bson.D {
	sorts := bson.D{}
	sort.ForEach(func(index interface{}, value moleculer.Payload) bool {
		item := sortEntry(value.String())
		sorts = append(sorts, item)
		return true
	})
	return sorts
}

func parseFilter(params moleculer.Payload) bson.M {
	query := payload.Empty()
	if params.Get("query").Exists() {
		query = params.Get("query")
	}
	query = parseSearchFields(params, query)
	return query.Bson()
}

func (adapter *MongoAdapter) openCursor(params moleculer.Payload) (*mongo.Cursor, context.Context, error) {
	ctx, _ := context.WithTimeout(context.Background(), adapter.Timeout)
	filter := parseFilter(params)
	opts := parseFindOptions(params)
	cursor, err := adapter.coll.Find(ctx, filter, opts)
	if err != nil {
		return nil, nil, err
	}
	return cursor, ctx, nil
}

// applyTransforms apply a list of transformations on the value param.
func applyTransforms(value bson.M, transforms ...func(bson.M) bson.M) bson.M {
	for _, transform := range transforms {
		value = transform(value)
	}
	return value
}

// cursorToPayload iterates throught a cursor and populate a list of payloads.
func cursorToPayload(ctx context.Context, cursor *mongo.Cursor, transform ...func(bson.M) bson.M) moleculer.Payload {
	list := []moleculer.Payload{}
	for cursor.Next(ctx) {
		var item bson.M
		err := cursor.Decode(&item)
		if err != nil {
			return payload.New(err)
		}
		transformed := applyTransforms(item, transform...)
		list = append(list, payload.New(transformed))
	}
	if err := cursor.Err(); err != nil {
		return payload.New(err)
	}
	return payload.New(list)
}

// idTransform transform id from primitive.ObjectID to string
func idTransform(bm bson.M) bson.M {
	_, hasId := bm["id"]
	_id, has_Id := bm["_id"]
	if has_Id && !hasId {
		bm["id"] = _id.(primitive.ObjectID).Hex()
		delete(bm, "_id")
	}
	return bm
}

// Find search the data store with the params provided.
func (adapter *MongoAdapter) Find(params moleculer.Payload) moleculer.Payload {
	cursor, ctx, err := adapter.openCursor(params)
	if err != nil {
		return payload.New(err)
	}
	defer cursor.Close(ctx)
	return cursorToPayload(ctx, cursor, idTransform)
}

func (adapter *MongoAdapter) FindOne(params moleculer.Payload) moleculer.Payload {
	params = params.Add("limit", 1)
	return adapter.Find(params).First()
}

func (adapter *MongoAdapter) FindById(params moleculer.Payload) moleculer.Payload {
	objId, err := primitive.ObjectIDFromHex(params.String())
	if err != nil {
		return payload.Error("Invalid id error: ", err)
	}
	filter := payload.New(bson.M{
		"query": bson.M{"_id": objId},
		"limit": 1,
	})
	return adapter.FindOne(filter)
}

func (adapter *MongoAdapter) FindByIds(params moleculer.Payload) moleculer.Payload {
	if !params.IsArray() {
		return payload.Error("FindByIds() only support lists!  --> !params.IsArray()")
	}
	r := payload.EmptyList()
	params.ForEach(func(idx interface{}, id moleculer.Payload) bool {
		r = r.AddItem(adapter.FindById(id))
		return true
	})
	return r
}

// Count count the number of records for the given filter.
func (adapter *MongoAdapter) Count(params moleculer.Payload) moleculer.Payload {
	ctx, _ := context.WithTimeout(context.Background(), adapter.Timeout)
	filter := parseFilter(params)
	count, err := adapter.coll.CountDocuments(ctx, filter)
	if err != nil {
		return payload.New(err)
	}
	return payload.New(count)
}

func (adapter *MongoAdapter) Insert(params moleculer.Payload) moleculer.Payload {
	ctx, _ := context.WithTimeout(context.Background(), adapter.Timeout)
	values := params.Bson()
	res, err := adapter.coll.InsertOne(ctx, values)
	if err != nil {
		return payload.Error("Error while trying to insert record. Error: ", err.Error())
	}
	return params.Add("id", res.InsertedID.(primitive.ObjectID).Hex())
}

func (adapter *MongoAdapter) Update(params moleculer.Payload) moleculer.Payload {
	id := params.Get("id")
	if !id.Exists() {
		return payload.Error("Cannot update record without id")
	}
	return adapter.UpdateById(id, params.Remove("id"))
}

func (adapter *MongoAdapter) UpdateById(id, update moleculer.Payload) moleculer.Payload {
	objId, err := primitive.ObjectIDFromHex(id.String())
	if err != nil {
		return payload.Error("Cannot update record without id - error: ", err)
	}
	ctx, _ := context.WithTimeout(context.Background(), adapter.Timeout)
	values := payload.Empty().Add("$set", update).Bson()
	ur, uerr := adapter.coll.UpdateOne(ctx, bson.M{"_id": objId}, values)
	if uerr != nil {
		return payload.Error("Cannot update record - error: ", uerr)
	}
	return payload.Empty().Add("modifiedCount", ur.ModifiedCount).Add("matchedCount", ur.MatchedCount)
}

func (adapter *MongoAdapter) RemoveById(id moleculer.Payload) moleculer.Payload {
	objId, err := primitive.ObjectIDFromHex(id.String())
	if err != nil {
		return payload.Error("Cannot update record without id - error: ", err)
	}
	ctx, _ := context.WithTimeout(context.Background(), adapter.Timeout)
	dr, uerr := adapter.coll.DeleteOne(ctx, bson.M{"_id": objId})
	if uerr != nil {
		return payload.Error("Cannot update record - error: ", uerr)
	}
	return payload.Empty().Add("deletedCount", dr.DeletedCount)
}

func (adapter *MongoAdapter) RemoveAll() moleculer.Payload {
	ctx, _ := context.WithTimeout(context.Background(), adapter.Timeout)
	res, err := adapter.coll.DeleteMany(ctx, bson.M{})
	if err != nil {
		return payload.Error("Error while trying to remove all records. Error: ", err.Error())
	}
	return payload.Empty().Add("deletedCount", res.DeletedCount)
}
