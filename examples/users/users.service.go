package main

import (
	"fmt"
	"time"

	"github.com/moleculer-go/store"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
)

func main() {
	var bkr = broker.New(&moleculer.Config{LogLevel: "info"})
	bkr.Publish(moleculer.ServiceSchema{
		Name: "users",
		Settings: map[string]interface{}{
			"fields":    []string{"_id", "username", "name"},
			"populates": map[string]interface{}{"friends": "users.get"},
		},
		Mixins: []moleculer.Mixin{store.Mixin(&store.MemoryAdapter{
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
