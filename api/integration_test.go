package api

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"reflect"
	inventory "rocket-service/rockets-inventory"
	queries "rocket-service/rockets-queries"
	"sync"
	"testing"
)

func setupTestServer(t *testing.T) (*httptest.Server, func()) {
	db, err := Init("") // Use in-memory SQLite
	if err != nil {
		t.Fatalf("Failed to initialize server: %v", err)
	}
	inventory := inventory.NewInventory(db)
	queries := queries.NewQueries(db)
	api := NewAPI(inventory, queries)
	handlers := api.InitHandlers()
	server := httptest.NewServer(handlers)
	return server, func() {
		server.Close()
		db.Close()
	}
}

func loadTestMessage(t *testing.T, filePath string) []byte {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read test file %s: %v", filePath, err)
	}
	return data
}

func TestIntegration_PostAndGetRocket(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	body := loadTestMessage(t, "testdata/rocket_launched.json")
	resp, err := http.Post(server.URL+"/messages", "application/json", bytes.NewBuffer(body))
	if err != nil {
		t.Fatalf("Failed to post message: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, err = http.Get(server.URL + "/rockets/test-channel")
	if err != nil {
		t.Fatalf("Failed to get rocket: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	var rocket queries.RocketState
	json.NewDecoder(resp.Body).Decode(&rocket)
	resp.Body.Close()

	expected := queries.RocketState{
		Channel: "test-channel",
		Type:    stringPtr("Falcon-9"),
		Speed:   intPtr(500),
		Mission: stringPtr("ARTEMIS"),
		Status:  stringPtr("launched"),
	}
	if !reflect.DeepEqual(rocket, expected) {
		t.Errorf("Expected rocket %+v, got %+v", expected, rocket)
	}
}

func TestIntegration_OutOfOrderMessages(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Post messages in order: 3, 1, 2, 3 (duplicate)
	messages := []struct {
		file          string
		messageNumber int
	}{
		{"testdata/speed_increased_3.json", 3}, // Out-of-order
		{"testdata/rocket_launched.json", 1},   // First in sequence
		{"testdata/speed_increased.json", 2},   // Next in sequence
		{"testdata/speed_increased_3.json", 3}, // Duplicate
	}

	for _, m := range messages {
		body := loadTestMessage(t, m.file)
		resp, err := http.Post(server.URL+"/messages", "application/json", bytes.NewBuffer(body))
		if err != nil {
			t.Fatalf("Failed to post message %s: %v", m.file, err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status 200 for %s, got %d", m.file, resp.StatusCode)
		}
		resp.Body.Close()
	}

	// Verify all messages were applied in sequence (1, 2, 3), duplicate ignored
	resp, err := http.Get(server.URL + "/rockets/test-channel")
	if err != nil {
		t.Fatalf("Failed to get rocket: %v", err)
	}
	var rocket queries.RocketState
	json.NewDecoder(resp.Body).Decode(&rocket)
	resp.Body.Close()

	if *rocket.Speed != 1000 { // 500 (msg 1) + 300 (msg 2) + 200 (msg 3)
		t.Errorf("Expected speed 1000, got %d", *rocket.Speed)
	}
	if *rocket.Mission != "ARTEMIS" {
		t.Errorf("Expected mission ARTEMIS, got %s", *rocket.Mission)
	}
}

func TestIntegration_DuplicateMessages(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Post same message twice
	body := loadTestMessage(t, "testdata/rocket_launched.json")
	for i := 0; i < 2; i++ {
		resp, err := http.Post(server.URL+"/messages", "application/json", bytes.NewBuffer(body))
		if err != nil {
			t.Fatalf("Failed to post message: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status 200, got %d", resp.StatusCode)
		}
		resp.Body.Close()
	}

	resp, err := http.Get(server.URL + "/rockets/test-channel")
	if err != nil {
		t.Fatalf("Failed to get rocket: %v", err)
	}
	var rocket queries.RocketState
	json.NewDecoder(resp.Body).Decode(&rocket)
	resp.Body.Close()

	if *rocket.Speed != 500 {
		t.Errorf("Expected speed 500, got %d", *rocket.Speed)
	}
}

func TestIntegration_ConcurrentMessages(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Initialize rocket
	body := loadTestMessage(t, "testdata/rocket_launched.json")
	resp, err := http.Post(server.URL+"/messages", "application/json", bytes.NewBuffer(body))
	if err != nil {
		t.Fatalf("Failed to post initial message: %v", err)
	}
	resp.Body.Close()

	var wg sync.WaitGroup
	numRequests := 10
	wg.Add(numRequests)

	for i := 1; i <= numRequests; i++ {
		go func(msgNum int) {
			defer wg.Done()
			// Load and modify messageNumber
			var msg map[string]interface{}
			body := loadTestMessage(t, "testdata/speed_increased.json")
			json.Unmarshal(body, &msg)
			msg["metadata"].(map[string]interface{})["messageNumber"] = float64(msgNum + 1)
			body, _ = json.Marshal(msg)

			resp, err := http.Post(server.URL+"/messages", "application/json", bytes.NewBuffer(body))
			if err != nil {
				t.Errorf("Failed to post message: %v", err)
			}
			if resp.StatusCode != http.StatusOK {
				t.Errorf("Expected status 200, got %d", resp.StatusCode)
			}
			resp.Body.Close()
		}(i)
	}

	wg.Wait()

	resp, err = http.Get(server.URL + "/rockets/test-channel")
	if err != nil {
		t.Fatalf("Failed to get rocket: %v", err)
	}
	var rocket queries.RocketState
	json.NewDecoder(resp.Body).Decode(&rocket)
	resp.Body.Close()

	if *rocket.Speed != 3500 { // 500 + (10 * 300)
		t.Errorf("Expected speed 3500, got %d", *rocket.Speed)
	}
}

func TestIntegration_ListRockets_SortByMission(t *testing.T) {
	server, cleanup := setupTestServer(t)

	defer cleanup()

	// Post two rockets
	messages := []string{
		"testdata/rocket_launched_chan1.json",
		"testdata/rocket_launched_chan2.json",
	}

	for _, file := range messages {
		body := loadTestMessage(t, file)
		resp, err := http.Post(server.URL+"/messages", "application/json", bytes.NewBuffer(body))
		if err != nil {
			t.Fatalf("Failed to post message %s: %v", file, err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status 200 for %s, got %d", file, resp.StatusCode)
		}
		resp.Body.Close()
	}

	resp, err := http.Get(server.URL + "/rockets?sort_by=mission")
	if err != nil {
		t.Fatalf("Failed to list rockets: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	var rockets []queries.RocketState
	json.NewDecoder(resp.Body).Decode(&rockets)
	resp.Body.Close()

	if len(rockets) != 2 {
		t.Fatalf("Expected 2 rockets, got %d", len(rockets))
	}
	if rockets[0].Channel != "chan2" || rockets[1].Channel != "chan1" {
		t.Errorf("Expected channels [chan2, chan1], got %+v", rockets)
	}
}

func TestIntegration_RocketNotFound(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	resp, err := http.Get(server.URL + "/rockets/non-existent")
	if err != nil {
		t.Fatalf("Failed to get rocket: %v", err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("Expected status 404, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func stringPtr(s string) *string { return &s }
func intPtr(i int) *int          { return &i }
