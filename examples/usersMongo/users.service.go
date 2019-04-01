package main

import (
	"fmt"
	"time"

	db "github.com/moleculer-go/moleculer-db"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
	"github.com/moleculer-go/moleculer/cli"
)

func main() {
	cli.Start(
		&moleculer.Config{LogLevel: "info"},
		func(broker *broker.ServiceBroker) {

			broker.AddService(moleculer.Service{
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
			broker.Start()
			time.Sleep(time.Millisecond * 300)
			user := <-broker.Call("users.create", map[string]interface{}{
				"username": "john",
				"name":     "John Doe",
				"status":   1,
			})

			id := user.Get("id").String()
			// Get all users
			fmt.Printf("all users: ", <-broker.Call("users.find", map[string]interface{}{}))

			// List users with pagination
			fmt.Printf("list users: ", <-broker.Call("users.list", map[string]interface{}{
				"page":     2,
				"pageSize": 10,
			}))

			idParam := map[string]interface{}{"id": id}

			// Get a user
			fmt.Printf("get user: ", <-broker.Call("users.get", idParam))

			// Update a user
			fmt.Printf("update user: ", <-broker.Call("users.update", map[string]interface{}{
				"id":   id,
				"name": "Jane Doe",
			}))

			// Print user after update
			fmt.Printf("get user: ", <-broker.Call("users.get", idParam))

			// Delete a user
			fmt.Printf("remove user: ", <-broker.Call("users.remove", idParam))

			broker.Stop()

		})
}
