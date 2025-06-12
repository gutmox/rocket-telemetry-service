package inventory

import (
	"database/sql"
	"encoding/json"
)

type Metadata struct {
	Channel       string `json:"channel"`
	MessageNumber int    `json:"messageNumber"`
	MessageTime   string `json:"messageTime"`
	MessageType   string `json:"messageType"`
}

type RocketMessage struct {
	Metadata Metadata        `json:"metadata"`
	Message  json.RawMessage `json:"message"`
}

type MessageHandler interface {
	Process(tx *sql.Tx, channel string, messageNumber int, message json.RawMessage) error
}

var MessageHandlers = map[string]MessageHandler{
	"RocketLaunched":       &RocketLaunchedHandler{},
	"RocketSpeedIncreased": &RocketSpeedIncreasedHandler{},
	"RocketSpeedDecreased": &RocketSpeedDecreasedHandler{},
	"RocketExploded":       &RocketExplodedHandler{},
	"RocketMissionChanged": &RocketMissionChangedHandler{},
}

type RocketLaunchedHandler struct{}

type RocketLaunchedMessage struct {
	Type        string `json:"type"`
	LaunchSpeed int    `json:"launchSpeed"`
	Mission     string `json:"mission"`
}

func (h *RocketLaunchedHandler) Process(tx *sql.Tx, channel string, messageNumber int, message json.RawMessage) error {
	var m RocketLaunchedMessage
	if err := json.Unmarshal(message, &m); err != nil {
		return err
	}
	_, err := tx.Exec(`
        INSERT INTO rockets (channel, type, speed, mission, status, last_message_number)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(channel) DO UPDATE
        SET type = ?, speed = ?, mission = ?, status = ?, last_message_number = ?`,
		channel, m.Type, m.LaunchSpeed, m.Mission, "launched", messageNumber,
		m.Type, m.LaunchSpeed, m.Mission, "launched", messageNumber)
	return err
}

type RocketSpeedIncreasedHandler struct{}

type RocketSpeedChangedMessage struct {
	By int `json:"by"`
}

func (h *RocketSpeedIncreasedHandler) Process(tx *sql.Tx, channel string, messageNumber int, message json.RawMessage) error {
	var m RocketSpeedChangedMessage
	if err := json.Unmarshal(message, &m); err != nil {
		return err
	}
	_, err := tx.Exec(`
        UPDATE rockets SET speed = speed + ?, last_message_number = ?
        WHERE channel = ?`,
		m.By, messageNumber, channel)
	return err
}

type RocketSpeedDecreasedHandler struct{}

func (h *RocketSpeedDecreasedHandler) Process(tx *sql.Tx, channel string, messageNumber int, message json.RawMessage) error {
	var m RocketSpeedChangedMessage
	if err := json.Unmarshal(message, &m); err != nil {
		return err
	}
	_, err := tx.Exec(`
        UPDATE rockets 
        SET speed = CASE WHEN speed - ? < 0 THEN 0 ELSE speed - ? END, 
            last_message_number = ?
        WHERE channel = ?`,
		m.By, m.By, messageNumber, channel)
	return err
}

type RocketExplodedHandler struct{}

type RocketExplodedMessage struct {
	Reason string `json:"reason"`
}

func (h *RocketExplodedHandler) Process(tx *sql.Tx, channel string, messageNumber int, message json.RawMessage) error {
	var m RocketExplodedMessage
	if err := json.Unmarshal(message, &m); err != nil {
		return err
	}
	_, err := tx.Exec(`
        UPDATE rockets SET status = ?, last_message_number = ?
        WHERE channel = ?`,
		"exploded", messageNumber, channel)
	return err
}

type RocketMissionChangedHandler struct{}

type RocketMissionChangedMessage struct {
	NewMission string `json:"newMission"`
}

func (h *RocketMissionChangedHandler) Process(tx *sql.Tx, channel string, messageNumber int, message json.RawMessage) error {
	var m RocketMissionChangedMessage
	if err := json.Unmarshal(message, &m); err != nil {
		return err
	}
	_, err := tx.Exec(`
        UPDATE rockets SET mission = ?, last_message_number = ?
        WHERE channel = ?`,
		m.NewMission, messageNumber, channel)
	return err
}
