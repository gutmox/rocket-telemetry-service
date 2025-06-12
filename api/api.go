package api

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"

	inventory "rocket-service/rockets-inventory"
	queries "rocket-service/rockets-queries"

	"github.com/gorilla/mux"
	_ "github.com/mattn/go-sqlite3"
)

type API struct {
	inventory *inventory.Inventory
	queries   *queries.Queries
}

func NewAPI(inventory *inventory.Inventory, queries *queries.Queries) *API {
	return &API{inventory, queries}
}

// Init initializes the database, modules, and HTTP router.
// If dbPath is empty, uses in-memory SQLite.
func Init(dbPath string) (*sql.DB, error) {
	if dbPath == "" {
		dbPath = ":memory:?_busy_timeout=5000"
	} else {
		dbPath += "?_busy_timeout=5000"
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS rockets (
            channel TEXT PRIMARY KEY,
            type TEXT,
            speed INTEGER,
            mission TEXT,
            status TEXT,
            last_message_number INTEGER DEFAULT 0
        );
    `)
	if err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

func (a *API) Start() error {

	r := a.InitHandlers()

	log.Println("Server starting on :8088")
	return http.ListenAndServe(":8088", r)
}

func (a *API) InitHandlers() *mux.Router {
	r := mux.NewRouter()

	r.HandleFunc("/messages", a.handleMessage).Methods("POST")
	r.HandleFunc("/rockets/{channel}", a.handleRockets).Methods("GET")
	r.HandleFunc("/rockets", a.handleListRockets).Methods("GET")

	return r
}

func (a *API) handleMessage(w http.ResponseWriter, r *http.Request) {
	var msg inventory.RocketMessage
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		log.Printf("Error processing message %s", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := a.inventory.UpdateRocketState(msg); err != nil {
		log.Printf("Error updating rocket inventory %s", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "message processed"})
}

func (a *API) handleRockets(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	channel := vars["channel"]

	rocket, err := a.queries.GetRocket(channel)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(rocket)
}

func (a *API) handleListRockets(w http.ResponseWriter, r *http.Request) {
	sortBy := r.URL.Query().Get("sort_by")
	rockets, err := a.queries.ListRockets(sortBy)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(rockets)
}
