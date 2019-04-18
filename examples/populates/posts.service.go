package main

import (
	"fmt"
	"time"

	db "github.com/moleculer-go/moleculer-db"

	"github.com/moleculer-go/moleculer"
	"github.com/moleculer-go/moleculer/broker"
)

func main() {
	var bkr = broker.New(&moleculer.Config{LogLevel: "info"})
	bkr.Publish(moleculer.ServiceSchema{
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
	bkr.Publish(moleculer.ServiceSchema{
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
