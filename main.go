package main

import (
	"log"
	"rocket-service/api"
	inventory "rocket-service/rockets-inventory"
	queries "rocket-service/rockets-queries"
)

func main() {
	db, err := api.Init("./rockets.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	inventory := inventory.NewInventory(db)
	queries := queries.NewQueries(db)
	api := api.NewAPI(inventory, queries)
	startError := api.Start()
	if startError != nil {
		log.Fatal(startError)
	}
}
