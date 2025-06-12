package inventory

import (
	"database/sql"
	"encoding/json"
	"sync"
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

func TestRocketLaunchedHandler(t *testing.T) {
	db := setupDB(t)
	defer db.Close()

	// Start transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to start transaction: %v", err)
	}
	defer tx.Rollback()

	handler := &RocketLaunchedHandler{}
	msg := RocketLaunchedMessage{
		Type:        "Falcon-9",
		LaunchSpeed: 500,
		Mission:     "ARTEMIS",
	}
	msgBytes, _ := json.Marshal(msg)

	err = handler.Process(tx, "test-channel", 1, msgBytes)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	// Commit transaction to persist changes
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Query state
	var r struct {
		Type          string
		Speed         int
		Mission       string
		Status        string
		MessageNumber int
	}
	err = db.QueryRow("SELECT type, speed, mission, status, last_message_number FROM rockets WHERE channel = ?", "test-channel").
		Scan(&r.Type, &r.Speed, &r.Mission, &r.Status, &r.MessageNumber)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if r.Type != "Falcon-9" || r.Speed != 500 || r.Mission != "ARTEMIS" || r.Status != "launched" || r.MessageNumber != 1 {
		t.Errorf("Unexpected state: %+v", r)
	}
}

func TestRocketSpeedIncreasedHandler(t *testing.T) {
	db := setupDB(t)
	defer db.Close()

	// Initialize rocket
	_, err := db.Exec("INSERT INTO rockets (channel, speed, last_message_number) VALUES (?, ?, ?)", "test-channel", 1000, 0)
	if err != nil {
		t.Fatalf("Failed to insert rocket: %v", err)
	}

	// Start transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to start transaction: %v", err)
	}
	defer tx.Rollback()

	handler := &RocketSpeedIncreasedHandler{}
	msg := RocketSpeedChangedMessage{By: 500}
	msgBytes, _ := json.Marshal(msg)

	err = handler.Process(tx, "test-channel", 1, msgBytes)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	// Commit transaction
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	var speed, messageNumber int
	err = db.QueryRow("SELECT speed, last_message_number FROM rockets WHERE channel = ?", "test-channel").
		Scan(&speed, &messageNumber)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if speed != 1500 || messageNumber != 1 {
		t.Errorf("Unexpected state: speed=%d, messageNumber=%d", speed, messageNumber)
	}
}

func TestRocketSpeedDecreasedHandler(t *testing.T) {
	db := setupDB(t)
	defer db.Close()

	// Initialize rocket
	_, err := db.Exec("INSERT INTO rockets (channel, speed, last_message_number) VALUES (?, ?, ?)", "test-channel", 500, 0)
	if err != nil {
		t.Fatalf("Failed to insert rocket: %v", err)
	}

	// Start transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to start transaction: %v", err)
	}
	defer tx.Rollback()

	handler := &RocketSpeedDecreasedHandler{}
	msg := RocketSpeedChangedMessage{By: 600}
	msgBytes, _ := json.Marshal(msg)

	err = handler.Process(tx, "test-channel", 1, msgBytes)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	// Commit transaction
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	var speed, messageNumber int
	err = db.QueryRow("SELECT speed, last_message_number FROM rockets WHERE channel = ?", "test-channel").
		Scan(&speed, &messageNumber)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if speed != 0 || messageNumber != 1 {
		t.Errorf("Unexpected state: speed=%d, messageNumber=%d", speed, messageNumber)
	}
}

func TestUpdateRocketState_DuplicateMessage(t *testing.T) {
	db := setupDB(t)
	defer db.Close()

	inventory := NewInventory(db)

	// Process initial message
	msg := RocketMessage{
		Metadata: Metadata{
			Channel:       "test-channel",
			MessageNumber: 1,
			MessageType:   "RocketLaunched",
		},
		Message: json.RawMessage(`{"type":"Falcon-9","launchSpeed":500,"mission":"ARTEMIS"}`),
	}
	err := inventory.UpdateRocketState(msg)
	if err != nil {
		t.Fatalf("Initial process failed: %v", err)
	}

	// Process duplicate message
	err = inventory.UpdateRocketState(msg)
	if err != nil {
		t.Fatalf("Duplicate process failed: %v", err)
	}

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM rockets WHERE channel = ?", "test-channel").Scan(&count)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected one rocket, got count=%d", count)
	}

	var speed int
	err = db.QueryRow("SELECT speed FROM rockets WHERE channel = ?", "test-channel").Scan(&speed)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if speed != 500 {
		t.Errorf("Expected speed=500, got %d", speed)
	}
}

func TestUpdateRocketState_OutOfOrder(t *testing.T) {
	db := setupDB(t)
	defer db.Close()

	inventory := NewInventory(db)
	channel := "test-channel"

	// Send messages in order: 3, 1, 2, 3 (duplicate)
	messages := []RocketMessage{
		{
			Metadata: Metadata{
				Channel:       channel,
				MessageNumber: 3,
				MessageType:   "RocketSpeedIncreased",
			},
			Message: json.RawMessage(`{"by":200}`),
		},
		{
			Metadata: Metadata{
				Channel:       channel,
				MessageNumber: 1,
				MessageType:   "RocketLaunched",
			},
			Message: json.RawMessage(`{"type":"Falcon-9","launchSpeed":500,"mission":"ARTEMIS"}`),
		},
		{
			Metadata: Metadata{
				Channel:       channel,
				MessageNumber: 2,
				MessageType:   "RocketSpeedIncreased",
			},
			Message: json.RawMessage(`{"by":300}`),
		},
		{
			Metadata: Metadata{
				Channel:       channel,
				MessageNumber: 3,
				MessageType:   "RocketSpeedIncreased",
			},
			Message: json.RawMessage(`{"by":200}`),
		},
	}

	for _, msg := range messages {
		err := inventory.UpdateRocketState(msg)
		if err != nil {
			t.Fatalf("Failed to process message %d: %v", msg.Metadata.MessageNumber, err)
		}
	}

	// Verify final state
	var speed, lastMessageNumber int
	row := db.QueryRow("SELECT speed, last_message_number FROM rockets WHERE channel = ?", channel)
	err := row.Scan(&speed, &lastMessageNumber)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if speed != 1000 { // 500 (msg 1) + 300 (msg 2) + 200 (msg 3)
		t.Errorf("Expected speed=1000, got %d", speed)
	}
	if lastMessageNumber != 3 {
		t.Errorf("Expected last_message_number=3, got %d", lastMessageNumber)
	}
}

func TestUpdateRocketState_OutOfOrderWithGaps(t *testing.T) {
	db := setupDB(t)
	defer db.Close()

	inventory := NewInventory(db)
	channel := "test-channel"

	// Send messages in order: 4, 1, 2, 3 (gap at 4 until 3 is processed)
	messages := []RocketMessage{
		{
			Metadata: Metadata{
				Channel:       channel,
				MessageNumber: 4,
				MessageType:   "RocketSpeedIncreased",
			},
			Message: json.RawMessage(`{"by":400}`),
		},
		{
			Metadata: Metadata{
				Channel:       channel,
				MessageNumber: 1,
				MessageType:   "RocketLaunched",
			},
			Message: json.RawMessage(`{"type":"Falcon-9","launchSpeed":500,"mission":"ARTEMIS"}`),
		},
		{
			Metadata: Metadata{
				Channel:       channel,
				MessageNumber: 2,
				MessageType:   "RocketSpeedIncreased",
			},
			Message: json.RawMessage(`{"by":300}`),
		},
		{
			Metadata: Metadata{
				Channel:       channel,
				MessageNumber: 3,
				MessageType:   "RocketSpeedIncreased",
			},
			Message: json.RawMessage(`{"by":200}`),
		},
	}

	for _, msg := range messages {
		err := inventory.UpdateRocketState(msg)
		if err != nil {
			t.Fatalf("Failed to process message %d: %v", msg.Metadata.MessageNumber, err)
		}
	}

	// Verify final state
	var speed, lastMessageNumber int
	row := db.QueryRow("SELECT speed, last_message_number FROM rockets WHERE channel = ?", channel)
	err := row.Scan(&speed, &lastMessageNumber)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if speed != 1400 { // 500 (msg 1) + 300 (msg 2) + 200 (msg 3) + 400 (msg 4)
		t.Errorf("Expected speed=1400, got %d", speed)
	}
	if lastMessageNumber != 4 {
		t.Errorf("Expected last_message_number=4, got %d", lastMessageNumber)
	}
}

func TestUpdateRocketState_Concurrent(t *testing.T) {
	db := setupDB(t)
	defer db.Close()

	inventory := NewInventory(db)
	channel := "test-channel"

	// Initialize rocket
	_, err := db.Exec("INSERT INTO rockets (channel, speed, last_message_number) VALUES (?, ?, ?)", channel, 1000, 0)
	if err != nil {
		t.Fatalf("Failed to insert rocket: %v", err)
	}

	var wg sync.WaitGroup
	numGoroutines := 10
	wg.Add(numGoroutines)

	// Simulate concurrent messages with different messageNumbers
	for i := 1; i <= numGoroutines; i++ {
		go func(msgNum int) {
			defer wg.Done()
			msg := RocketMessage{
				Metadata: Metadata{
					Channel:       channel,
					MessageNumber: msgNum,
					MessageType:   "RocketSpeedIncreased",
				},
				Message: json.RawMessage(`{"by":100}`),
			}
			err := inventory.UpdateRocketState(msg)
			if err != nil {
				t.Errorf("Concurrent process failed: %v", err)
			}
		}(i)
	}

	wg.Wait()

	var speed, messageNumber int
	err = db.QueryRow("SELECT speed, last_message_number FROM rockets WHERE channel = ?", channel).
		Scan(&speed, &messageNumber)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if speed != 2000 { // 1000 + (10 * 100)
		t.Errorf("Expected speed=2000, got %d", speed)
	}
	if messageNumber != numGoroutines {
		t.Errorf("Expected messageNumber=%d, got %d", numGoroutines, messageNumber)
	}
}

func TestUpdateRocketState_InvalidMessageType(t *testing.T) {
	db := setupDB(t)
	defer db.Close()

	inventory := NewInventory(db)
	msg := RocketMessage{
		Metadata: Metadata{
			Channel:       "test-channel",
			MessageNumber: 1,
			MessageType:   "InvalidType",
		},
		Message: json.RawMessage(`{}`),
	}

	err := inventory.UpdateRocketState(msg)
	if err == nil || err.Error() != "invalid message type: InvalidType" {
		t.Errorf("Expected error 'invalid message type: InvalidType', got %v", err)
	}
}
