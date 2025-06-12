package inventory

import (
	"database/sql"
	"fmt"
	"sort"
	"sync"
)

// Inventory manages rocket state updates
type Inventory struct {
	db             *sql.DB
	locks          map[string]*sync.Mutex
	global         sync.Mutex
	messageBuffers map[string][]RocketMessage
}

func NewInventory(db *sql.DB) *Inventory {
	return &Inventory{
		db:             db,
		locks:          make(map[string]*sync.Mutex),
		messageBuffers: make(map[string][]RocketMessage),
	}
}

func (i *Inventory) getLock(channel string) *sync.Mutex {
	i.global.Lock()
	defer i.global.Unlock()

	if lock, exists := i.locks[channel]; exists {
		return lock
	}
	lock := &sync.Mutex{}
	i.locks[channel] = lock
	return lock
}

func (i *Inventory) UpdateRocketState(msg RocketMessage) error {
	metadata := msg.Metadata
	channel := metadata.Channel

	lock := i.getLock(channel)
	lock.Lock()
	defer lock.Unlock()

	tx, err := i.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var lastMessageNumber int
	err = tx.QueryRow("SELECT last_message_number FROM rockets WHERE channel = ?", channel).Scan(&lastMessageNumber)
	if err != nil && err != sql.ErrNoRows {
		return err
	}

	// Ignore duplicates or already processed messages
	if metadata.MessageNumber <= lastMessageNumber {
		return tx.Commit()
	}

	// If message is out of order, add to buffer
	if metadata.MessageNumber > lastMessageNumber+1 {
		i.global.Lock()
		// Check if message is already in buffer to avoid duplicates
		alreadyBuffered := false
		for _, bufferedMsg := range i.messageBuffers[channel] {
			if bufferedMsg.Metadata.MessageNumber == metadata.MessageNumber {
				alreadyBuffered = true
				break
			}
		}
		if !alreadyBuffered {
			i.messageBuffers[channel] = append(i.messageBuffers[channel], msg)
			// Sort buffer by messageNumber
			sort.Slice(i.messageBuffers[channel], func(a, b int) bool {
				return i.messageBuffers[channel][a].Metadata.MessageNumber < i.messageBuffers[channel][b].Metadata.MessageNumber
			})
		}
		i.global.Unlock()
		return tx.Commit()
	}

	err = i.processMessage(tx, msg)
	if err != nil {
		return err
	}

	_, err = tx.Exec("UPDATE rockets SET last_message_number = ? WHERE channel = ?", metadata.MessageNumber, channel)
	if err != nil {
		return err
	}
	lastMessageNumber = metadata.MessageNumber

	for {
		i.global.Lock()
		nextMsg := i.getNextMessage(channel, lastMessageNumber+1)
		if nextMsg == nil {
			i.global.Unlock()
			break
		}

		i.removeMessage(channel, nextMsg.Metadata.MessageNumber)
		i.global.Unlock()

		if nextMsg.Metadata.MessageNumber <= lastMessageNumber {
			continue
		}

		err = i.processMessage(tx, *nextMsg)
		if err != nil {
			return err
		}

		lastMessageNumber = nextMsg.Metadata.MessageNumber
		_, err = tx.Exec("UPDATE rockets SET last_message_number = ? WHERE channel = ?", lastMessageNumber, channel)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (i *Inventory) getNextMessage(channel string, messageNumber int) *RocketMessage {
	for _, msg := range i.messageBuffers[channel] {
		if msg.Metadata.MessageNumber == messageNumber {
			return &msg
		}
	}
	return nil
}

func (i *Inventory) removeMessage(channel string, messageNumber int) {
	var updated []RocketMessage
	for _, msg := range i.messageBuffers[channel] {
		if msg.Metadata.MessageNumber != messageNumber {
			updated = append(updated, msg)
		}
	}
	i.messageBuffers[channel] = updated
}

func (i *Inventory) processMessage(tx *sql.Tx, msg RocketMessage) error {
	metadata := msg.Metadata

	handler, exists := MessageHandlers[metadata.MessageType]
	if !exists {
		return fmt.Errorf("invalid message type: %s", metadata.MessageType)
	}

	if err := handler.Process(tx, metadata.Channel, metadata.MessageNumber, msg.Message); err != nil {
		return err
	}

	return nil
}
