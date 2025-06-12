package queries

import (
	"database/sql"
	"reflect"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func setupDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	_, err = db.Exec(`
        CREATE TABLE rockets (
            channel TEXT PRIMARY KEY,
            type TEXT,
            speed INTEGER,
            mission TEXT,
            status TEXT,
            last_message_number INTEGER DEFAULT 0
        )
    `)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	return db
}

func TestGetRocket(t *testing.T) {
	db := setupDB(t)
	defer db.Close()

	// Insert test data
	db.Exec("INSERT INTO rockets (channel, type, speed, mission, status) VALUES (?, ?, ?, ?, ?)",
		"test-channel", "Falcon-9", 500, "ARTEMIS", "launched")

	queries := NewQueries(db)
	rocket, err := queries.GetRocket("test-channel")
	if err != nil {
		t.Fatalf("GetRocket failed: %v", err)
	}

	expected := &RocketState{
		Channel: "test-channel",
		Type:    stringPtr("Falcon-9"),
		Speed:   intPtr(500),
		Mission: stringPtr("ARTEMIS"),
		Status:  stringPtr("launched"),
	}
	if !reflect.DeepEqual(rocket, expected) {
		t.Errorf("Expected %+v, got %+v", expected, rocket)
	}
}

func TestGetRocket_NotFound(t *testing.T) {
	db := setupDB(t)
	defer db.Close()

	queries := NewQueries(db)
	_, err := queries.GetRocket("non-existent")
	if err == nil || err.Error() != "rocket not found" {
		t.Errorf("Expected 'rocket not found' error, got %v", err)
	}
}

func TestListRockets_SortBySpeed(t *testing.T) {
	db := setupDB(t)
	defer db.Close()

	// Insert test data
	db.Exec("INSERT INTO rockets (channel, speed) VALUES (?, ?)", "chan1", 1000)
	db.Exec("INSERT INTO rockets (channel, speed) VALUES (?, ?)", "chan2", 500)
	db.Exec("INSERT INTO rockets (channel, speed) VALUES (?, ?)", "chan3", 750)

	queries := NewQueries(db)
	rockets, err := queries.ListRockets("speed")
	if err != nil {
		t.Fatalf("ListRockets failed: %v", err)
	}

	if len(rockets) != 3 {
		t.Fatalf("Expected 3 rockets, got %d", len(rockets))
	}

	if rockets[0].Channel != "chan2" || rockets[1].Channel != "chan3" || rockets[2].Channel != "chan1" {
		t.Errorf("Expected channels in order [chan2, chan3, chan1], got %+v", rockets)
	}
}

func stringPtr(s string) *string { return &s }
func intPtr(i int) *int          { return &i }
