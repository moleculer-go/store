[![Build Status](https://cloud.drone.io/api/badges/moleculer-go/moleculer-db/status.svg)](https://cloud.drone.io/moleculer-go/moleculer-db)
[![Coverage Status](https://coveralls.io/repos/github/moleculer-go/moleculer-db/badge.svg?branch=feat%2Fmongo-adapter)](https://coveralls.io/github/moleculer-go/moleculer-db?branch=feat%2Fmongo-adapter)

# Official DB addons for Moleculer-Go framework

Moleculer framework has an official set of [DB adapters](https://github.com/moleculer-go/stores). Use them to persist your data in a database.

{% note info Database per service%}
Moleculer follows the _one database per service_ pattern. To learn more about this design pattern and its implications check this [article](https://microservices.io/patterns/data/database-per-service.html).
{% endnote %}

## Features

- default CRUD actions (create, find, count, list, get, update, remove)
- [cached](caching.html) actions
- pagination support
- pluggable adapter - There is the default memory adapter for testing & prototyping)
- official adapters for MongoDB.
- fields filtering
- populating
- encode/decode IDs
- entity lifecycle events for notifications

## Memory Adapter

Moleculer's memory adapter uses [hashicorp/go-memdb](https://github.com/hashicorp/go-memdb). Use it to quickly set up and test you prototype and for writing test cases.

{% note warn%}
Only use this adapter for prototyping and testing. When you are ready to go into production simply swap to [Mongo](moleculer-db.html#Mongo-Adapter) ... adapters as they all implement common [Settings](moleculer-db.html#Settings), [Actions](moleculer-db.html#Actions).
{% endnote %}

### Install

```bash
$ go get -u github.com/moleculer-go/stores
```

### Usage

```go
package main

import (
 "fmt"
 "time"

 db "github.com/moleculer-go/stores"

 "github.com/moleculer-go/moleculer"
 "github.com/moleculer-go/moleculer/broker"
)

func main() {
 var bkr = broker.New(&moleculer.Config{LogLevel: "info"})
 bkr.Publish(moleculer.Service{
  Name: "users",
  Settings: map[string]interface{}{
   "fields":    []string{"_id", "username", "name"},
   "populates": map[string]interface{}{"friends": "users.get"},
  },
  Mixins: []moleculer.Mixin{db.Mixin(&db.MemoryAdapter{
   Table:        "users",
   SearchFields: []string{"name", "username"},
  })},
 })
 bkr.Start()
 time.Sleep(time.Millisecond * 300)
 user := <-bkr.Call("users.create", map[string]interface{}{
  "username": "john",
  "name":     "John Doe",
  "status":   1,
 })

 id := user.Get("id").String()
 // Get all users
 fmt.Printf("all users: ", <-bkr.Call("users.find", map[string]interface{}{}))

 // List users with pagination
 fmt.Printf("list users: ", <-bkr.Call("users.list", map[string]interface{}{
  "page":     2,
  "pageSize": 10,
 }))

 idParam := map[string]interface{}{"id": id}

 // Get a user
 fmt.Printf("get user: ", <-bkr.Call("users.get", idParam))

 // Update a user
 fmt.Printf("update user: ", <-bkr.Call("users.update", map[string]interface{}{
  "id":   id,
  "name": "Jane Doe",
 }))

 // Print user after update
 fmt.Printf("get user: ", <-bkr.Call("users.get", idParam))

 // Delete a user
 fmt.Printf("remove user: ", <-bkr.Call("users.remove", idParam))

 bkr.Stop()
}
```

#### run the example above with:

```bash
$ go run github.com/moleculer-go/stores/examples/users
```

> More examples can be found on [GitHub](https://github.com/moleculer-go/stores/examples)

## Settings

All DB adapters share a common set of settings:

| Property          | Type                     | Default      | Description                                                                                                                           |
| ----------------- | ------------------------ | ------------ | ------------------------------------------------------------------------------------------------------------------------------------- |
| `idField`         | `string`                 | **required** | Name of ID field.                                                                                                                     |
| `fields`          | `[]string`               | ["**"]       | Field filtering list. It must be an `Array`. If the value is nil it will assume ["**"] and it will not filter the fields of entities. |
| `populates`       | `map[string]interface{}` |              | Schema for population. [Read more](#Populating).                                                                                      |
| `pageSize`        | `Number`                 | **required** | Default page size in `list` action.                                                                                                   |
| `maxPageSize`     | `Number`                 | **required** | Maximum page size in `list` action.                                                                                                   |
| `maxLimit`        | `Number`                 | **required** | Maximum value of limit in `find` action. Default: `-1` (no limit)                                                                     |
| `entityValidator` | `Object`, `function`     | `null`       | Validator schema or a function to validate the incoming entity in `create` action.                                                    |

## Actions

DB adapters also implement CRUD operations. These actions are public methods and can be called by other services.

### [`find`](https://github.com/moleculer-go/stores/blob/master/moleculer_db.go#L81) ![Cached action](https://img.shields.io/badge/cache-true-blue.svg)

Find entities by query.

#### Parameters

| Property       | Type                     | Default      | Description                      |
| -------------- | ------------------------ | ------------ | -------------------------------- |
| `populate`     | `[]string`               | -            | Populated fields.                |
| `fields`       | `[]string`               | -            | Fields filter.                   |
| `limit`        | `Number`                 | **required** | Max count of rows.               |
| `offset`       | `Number`                 | **required** | Count of skipped rows.           |
| `sort`         | `string`                 | **required** | Sorted fields.                   |
| `search`       | `string`                 | **required** | Search text.                     |
| `searchFields` | `string`                 | **required** | Fields for searching.            |
| `query`        | `map[string]interface{}` | **required** | Query object. Passes to adapter. |

#### Results

**Type:** `moluculer.Paylod` - List of found entities.

### [`count`](https://github.com/moleculer-go/stores/blob/master/moleculer_db.go#L261) ![Cached action](https://img.shields.io/badge/cache-true-blue.svg)

Get count of entities by query.

#### Parameters

| Property       | Type     | Default      | Description                      |
| -------------- | -------- | ------------ | -------------------------------- |
| `search`       | `string` | **required** | Search text.                     |
| `searchFields` | `string` | **required** | Fields list for searching.       |
| `query`        | `Object` | **required** | Query object. Passes to adapter. |

#### Results

**Type:** `Number` - Count of found entities.

### [`list`](https://github.com/moleculer-go/stores/blob/master/moleculer_db.go#L140) ![Cached action](https://img.shields.io/badge/cache-true-blue.svg)

List entities by filters and pagination results.

#### Parameters

| Property       | Type                     | Default      | Description                      |
| -------------- | ------------------------ | ------------ | -------------------------------- |
| `populate`     | `[]string`               | -            | Populated fields.                |
| `fields`       | `[]string`               | -            | Fields filter.                   |
| `page`         | `Number`                 | **required** | Page number.                     |
| `pageSize`     | `Number`                 | **required** | Size of a page.                  |
| `sort`         | `string`                 | **required** | Sorted fields.                   |
| `search`       | `string`                 | **required** | Search text.                     |
| `searchFields` | `string`                 | **required** | Fields for searching.            |
| `query`        | `map[string]interface{}` | **required** | Query object. Passes to adapter. |

#### Results

**Type:** `moleculer.Payload` - List of found entities and count.

### [`create`](https://github.com/moleculer-go/stores/blob/master/moleculer_db.go#L88)

Create a new entity.

#### Parameters

Payload with fields to be saved in the new entity record.

#### Results

**Type:** `moleculer.Payload` - Saved entity.

### [`get`](https://github.com/moleculer-go/stores/blob/master/moleculer_db.go#L174) ![Cached action](https://img.shields.io/badge/cache-true-blue.svg)

Get entity by ID.

##### Parameters

| Property   | Type       | Default      | Description                                                               |
| ---------- | ---------- | ------------ | ------------------------------------------------------------------------- |
| `id`       | `string`   | **required** | ID of entity.                                                             |
| `ids`      | `[]string` | **required** | ID(s) of entities.                                                        |
| `populate` | `[]string` | -            | Field list for populate.                                                  |
| `fields`   | `[]string` | -            | Fields filter.                                                            |
| `mapping`  | `Bool`     | -            | Convert the returned `Array` to `Map` where the key is the value of `id`. |

#### Results

**Type:** `moleculer.Payload` - Found entity(ies).

### [`update`](https://github.com/moleculer-go/stores/blob/master/moleculer_db.go#L103)

Update an entity by ID.

> After update, clear the cache & call lifecycle events.

#### Parameters

| Property | Type     | Default | Description                      |
| -------- | -------- | ------- | -------------------------------- |
| `id`     | `string` | -       | Id of the records being updated. |

#### Results

**Type:** `moleculer.Payload` - Updated entity.

### [`remove`](https://github.com/moleculer-go/stores/blob/master/moleculer_db.go#L121)

Remove an entity by ID.

#### Parameters

| Property | Type     | Default      | Description   |
| -------- | -------- | ------------ | ------------- |
| `id`     | `string` | **required** | ID of entity. |

#### Results

**Type:** `Number` - Count of removed entities.

## Populating

The service allows you to easily populate fields from other services. For exapmle: If you have an `author` field in `post` entity, you can populate it with `users` service by ID of author. If the field is an `Array` of IDs, it will populate all entities via only one request

**Example of populate schema**

```go
package main

import (
 "fmt"
 "time"

 db "github.com/moleculer-go/stores"

 "github.com/moleculer-go/moleculer"
 "github.com/moleculer-go/moleculer/broker"
)

func main() {
 var bkr = broker.New(&moleculer.Config{LogLevel: "info"})
 bkr.Publish(moleculer.Service{
  Name: "users",
  Settings: map[string]interface{}{
   "fields":    []string{"id", "username", "name"},
   "populates": map[string]interface{}{"friends": "users.get"},
  },
  Mixins: []moleculer.Mixin{db.Mixin(&db.MemoryAdapter{
   Table:        "users",
   SearchFields: []string{"name", "username"},
  })},
 })
 bkr.Publish(moleculer.Service{
  Name: "posts",
  Settings: map[string]interface{}{
   "populates": map[string]interface{}{
    //Shorthand populate rule. Resolve the 'voters' values with the users.get action.
    "voters": "users.get",
    // Define the params of action call.
    //It will receive only with username of author.
    "author": map[string]interface{}{
     "action": "users.get",
     "params": map[string]interface{}{
      "fields": []string{"username"},
     },
    },
   },
  },
  Mixins: []moleculer.Mixin{db.Mixin(&db.MemoryAdapter{
   Table: "posts",
  })},
 })
 bkr.Start()
 time.Sleep(time.Millisecond * 300)

 johnSnow := <-bkr.Call("users.create", map[string]interface{}{
  "name":     "John",
  "lastname": "Snow",
  "username": "jsnow",
  "fullname": "John Snow",
 })
 marie := <-bkr.Call("users.create", map[string]interface{}{
  "name":     "Marie",
  "lastname": "Claire",
  "username": "mclaire",
  "fullname": "Marie Claire",
 })

 post := <-bkr.Call("posts.create", map[string]interface{}{
  "content": "Lorem ipsum dolor sit amet, consectetur ...",
  "voters":  []string{marie.Get("id").String()},
  "author":  johnSnow.Get("id").String(),
  "status":  1,
 })

 // List posts with populated author
 fmt.Printf("posts with author: ", <-bkr.Call("posts.find", map[string]interface{}{
  "populate": []string{"author"},
 }))

 // List posts with populated voters
 fmt.Printf("posts with voters: ", <-bkr.Call("posts.find", map[string]interface{}{
  "populate": []string{"voters"},
 }))

 // remove post
 <-bkr.Call("posts.remove", map[string]interface{}{
  "id": post.Get("id").String(),
 })

 //remove users
 <-bkr.Call("users.remove", map[string]interface{}{
  "id": johnSnow.Get("id").String(),
 })
 <-bkr.Call("users.remove", map[string]interface{}{
  "id": marie.Get("id").String(),
 })

 bkr.Stop()
}

```

```bash
run the example above with:
$ go run github.com/moleculer-go/stores/examples/populates
```

> The `populate` parameter is available in `find`, `list` and `get` actions.

## Extend with custom actions

Naturally you can extend this service with your custom actions.

```go
package main

import (
 "fmt"
 "time"

 db "github.com/moleculer-go/stores"

 "github.com/moleculer-go/moleculer"
 "github.com/moleculer-go/moleculer/broker"
)

func main() {
 var bkr = broker.New(&moleculer.Config{LogLevel: "info"})
 bkr.Publish(moleculer.Service{
  Name: "users",
  Settings: map[string]interface{}{
   "fields":    []string{"id", "username", "name"},
   "populates": map[string]interface{}{"friends": "users.get"},
  },
  Mixins: []moleculer.Mixin{db.Mixin(&db.MemoryAdapter{
   Table:        "users",
   SearchFields: []string{"name", "username"},
  })},
 })
 adapter := &db.MemoryAdapter{
  Table: "posts",
 }
 bkr.Publish(moleculer.Service{
  Name: "posts",
  Settings: map[string]interface{}{
   "populates": map[string]interface{}{
    //Shorthand populate rule. Resolve the 'voters' values with the users.get action.
    "voters": "users.get",
    // Define the params of action call. It will receive only with username & full name of author.
    "author": map[string]interface{}{
     "action": "users.get",
     "params": map[string]interface{}{
      "fields": []string{"username", "fullname"},
     },
    },
   },
  },
  Mixins: []moleculer.Mixin{db.Mixin(adapter)},
  Actions: []moleculer.Action{
   {
    Name: "byAuthors",
    Handler: func(ctx moleculer.Context, params moleculer.Payload) interface{} {
     return <-ctx.Call("posts.find", map[string]interface{}{
      "query": map[string]interface{}{
       "author": params.Get("authorId").String(),
      },
      "limit": 10,
      "sort":  "-createdAt",
     })
    },
   },
  },
 })
 bkr.Start()
 time.Sleep(time.Millisecond * 300)

 johnSnow := <-bkr.Call("users.create", map[string]interface{}{
  "name":     "John",
  "lastname": "Snow",
  "username": "jsnow",
  "fullname": "John Snow",
 })
 marie := <-bkr.Call("users.create", map[string]interface{}{
  "name":     "Marie",
  "lastname": "Claire",
  "username": "mclaire",
  "fullname": "Marie Claire",
 })

 post := <-bkr.Call("posts.create", map[string]interface{}{
  "content": "Lorem ipsum dolor sit amet, consectetur ...",
  "voters":  []string{marie.Get("id").String()},
  "author":  johnSnow.Get("id").String(),
  "status":  1,
 })

 // List posts with populated authors
 fmt.Printf("posts by authors: ", <-bkr.Call("posts.byAuthors", map[string]interface{}{
  "authorId": johnSnow.Get("id").String(),
 }))

 // remove post
 <-bkr.Call("posts.remove", map[string]interface{}{
  "id": post.Get("id").String(),
 })

 //remove users
 <-bkr.Call("users.remove", map[string]interface{}{
  "id": johnSnow.Get("id").String(),
 })
 <-bkr.Call("users.remove", map[string]interface{}{
  "id": marie.Get("id").String(),
 })

 bkr.Stop()
}
```

```bash
run the example above with:
$ go run github.com/moleculer-go/stores/examples/customActions
```

## Cache

Not Implemented yet!
![Under Construction](https://img.shields.io/badge/under-construction-red.svg)

## Mongo Adapter

This adapter is based on [MongoDB](https://go.mongodb.org/mongo-driver/).

### Install

```bash
$ go get -u github.com/moleculer-go/stores
```

{% note info Dependencies%}
To use this adapter you need to install [MongoDB](https://www.mongodb.com/) on you system.
{% endnote %}

### Usage

```go
package main

import (
 "fmt"
 "time"

 db "github.com/moleculer-go/stores"

 "github.com/moleculer-go/moleculer"
 "github.com/moleculer-go/moleculer/broker"
)

func main() {
 var bkr = broker.New(&moleculer.Config{LogLevel: "info"})
 bkr.Publish(moleculer.Service{
  Name: "users",
  Settings: map[string]interface{}{
   "fields":    []string{"_id", "username", "name"},
   "populates": map[string]interface{}{"friends": "users.get"},
  },
  Mixins: []moleculer.Mixin{db.Mixin(&db.MongoAdapter{
   MongoURL:   "mongodb://localhost:27017",
   Collection: "users",
   Database:   "test",
   Timeout:    time.Second * 5,
  })},
 })
 bkr.Start()
 time.Sleep(time.Millisecond * 300)
 user := <-bkr.Call("users.create", map[string]interface{}{
  "username": "john",
  "name":     "John Doe",
  "status":   1,
 })

 id := user.Get("id").String()
 // Get all users
 fmt.Printf("all users: ", <-bkr.Call("users.find", map[string]interface{}{}))
 //...
 bkr.Stop()
}

```

> More MongoDB examples can be found on [GitHub](https://github.com/moleculer-go/stores/tree/master/examples)
