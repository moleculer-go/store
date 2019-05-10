package main

import (
	"fmt"
	"time"

	"github.com/moleculer-go/store"
	"github.com/moleculer-go/store/sqlite"
	"github.com/spf13/cobra"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
	"github.com/moleculer-go/moleculer/cli"
)

func main() {
	cli.Start(
		&moleculer.Config{LogLevel: "debug"},
		func(broker *broker.ServiceBroker, cmd *cobra.Command) {
			broker.Publish(moleculer.ServiceSchema{
				Name: "users",
				Settings: map[string]interface{}{
					"fields":    []string{"id", "username", "name"},
					"populates": map[string]interface{}{"friends": "users.get"},
				},
				Mixins: []moleculer.Mixin{store.Mixin(&sqlite.Adapter{
					URI:   "file:memory:?mode=memory",
					Table: "users",
					Columns: []sqlite.Column{
						{
							Name: "username",
							Type: "string",
						},
						{
							Name: "name",
							Type: "string",
						},
						{
							Name: "status",
							Type: "integer",
						},
					},
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
