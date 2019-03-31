package main

import (
	"fmt"
	"time"

	db "github.com/moleculer-go/moleculer-db"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
)

func main() {
	var bkr = broker.New(&moleculer.Config{LogLevel: "debug"})
	bkr.AddService(moleculer.Service{
		Name: "users",
		Settings: map[string]interface{}{
			"fields":    []string{"_id", "username", "name"},
			"populates": map[string]interface{}{"friends": "users.get"},
		},
		Mixins: []moleculer.Mixin{db.Mixin(&db.MongoAdapter{
			MongoURL:   "mongodb://localhost:27017",
			Collection: "user",
			Database:   "services",
			Timeout:    time.Second * 10,
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

	// Get a user
	fmt.Printf("get user: ", <-bkr.Call("users.get", map[string]interface{}{
		"id": id,
	}))

	// Update a user
	fmt.Printf("update user: ", <-bkr.Call("users.update", map[string]interface{}{
		"id":   id,
		"name": "Jane Doe",
	}))

	// Print user after update
	fmt.Printf("get user: ", <-bkr.Call("users.get", map[string]interface{}{
		"id": id,
	}))

	// Delete a user
	fmt.Printf("remove user: ", <-bkr.Call("users.remove", map[string]interface{}{
		"id": id,
	}))

	bkr.Stop()
}
