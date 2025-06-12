package queries

import (
	"database/sql"
	"fmt"
)

type RocketState struct {
	Channel string  `json:"channel"`
	Type    *string `json:"type,omitempty"`
	Speed   *int    `json:"speed,omitempty"`
	Mission *string `json:"mission,omitempty"`
	Status  *string `json:"status,omitempty"`
}

type Queries struct {
	db *sql.DB
}

func NewQueries(db *sql.DB) *Queries {
	return &Queries{db}
}

func (q *Queries) GetRocket(channel string) (*RocketState, error) {
	var r RocketState
	var speed sql.NullInt64
	var typ, mission, status sql.NullString

	err := q.db.QueryRow(`
        SELECT channel, type, speed, mission, status
        FROM rockets WHERE channel = ?`, channel).
		Scan(&r.Channel, &typ, &speed, &mission, &status)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("rocket not found")
	}
	if err != nil {
		return nil, err
	}

	if typ.Valid {
		r.Type = &typ.String
	}
	if speed.Valid {
		s := int(speed.Int64)
		r.Speed = &s
	}
	if mission.Valid {
		r.Mission = &mission.String
	}
	if status.Valid {
		r.Status = &status.String
	}

	return &r, nil
}

func (q *Queries) ListRockets(sortBy string) ([]RocketState, error) {
	var orderBy string
	switch sortBy {
	case "speed":
		orderBy = "speed ASC"
	case "mission":
		orderBy = "mission ASC"
	case "status":
		orderBy = "status ASC"
	default:
		orderBy = "channel ASC"
	}

	rows, err := q.db.Query("SELECT channel, type, speed, mission, status FROM rockets ORDER BY " + orderBy)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var rockets []RocketState
	for rows.Next() {
		var r RocketState
		var speed sql.NullInt64
		var typ, mission, status sql.NullString
		if err := rows.Scan(&r.Channel, &typ, &speed, &mission, &status); err != nil {
			return nil, err
		}
		if typ.Valid {
			r.Type = &typ.String
		}
		if speed.Valid {
			s := int(speed.Int64)
			r.Speed = &s
		}
		if mission.Valid {
			r.Mission = &mission.String
		}
		if status.Valid {
			r.Status = &status.String
		}
		rockets = append(rockets, r)
	}

	return rockets, nil
}
