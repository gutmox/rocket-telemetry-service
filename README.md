# Rocket Telemetry Service

The Rocket Telemetry Service is a Go-based HTTP API for processing and querying rocket telemetry data. It handles messages about rocket events (e.g., launches, speed changes, mission updates) sent out of order with an at-least-once delivery guarantee, storing state in a SQLite database and exposing endpoints to retrieve rocket information.

## Features

- **Message Processing**: Handles `RocketLaunched`, `RocketSpeedIncreased`, `RocketSpeedDecreased`, `RocketExploded`, and `RocketMissionChanged` messages.
- **Out-of-Order Handling**: Processes messages in sequence with a in-memory buffer using a sorted slice for out-of-order messages.
- **At-Least-Once Guarantee**: Ignores duplicate messages based on `messageNumber`.
- **Concurrency**: Uses per-rocket mutexes for thread-safe message processing.
- **Query Endpoints**: Retrieve individual rocket states or list rockets with sorting options (by channel, speed, mission, or status).
- **Testing**: Comprehensive unit and integration tests with JSON-based scenarios.


## Prerequisites
- **Go**: Version 1.22 or later (`go version` to check).
- **Git**: Optional, for version control.
- **curl**: For manual API testing.

## Running the Application

Start the server, which listens on port 8088 and creates a rockets.db SQLite database:

```bash
go run main.go
```  


## API Endpoints

### POST /messages

Processes a telemetry message.

- Body: JSON with metadata (channel, messageNumber, messageType, messageTime) and message (type-specific data).

Example: ```bash
curl -X POST http://localhost:8088/messages -H "Content-Type: application/json" -d @integration/testdata/rocket_launched.json
```

- Response:

```bash 
{"status":"message processed"}
```

### GET /rockets/{channel}

Retrieves a rocket's state by channel.

Example:

```bash
curl http://localhost:8088/rockets/test-channel
```

Response:

```json
{
    "channel": "test-channel",
    "type": "Falcon-9",
    "speed": 500,
    "mission": "ARTEMIS",
    "status": "launched"
}
```

### GET /rockets

Lists all rockets, optionally sorted by sort_by query parameter (speed, mission, status, or channel).

Example: 

```bash
curl http://localhost:8088/rockets?sort_by=speed
```

Response:

```json
[
    {"channel": "chan2", "speed": 500},
    {"channel": "chan1", "speed": 1000}
]
```

## Testing


The project includes unit tests for each module (rockets-inventory, rockets-queries, api) and integration tests using JSON scenarios in integration/testdata/.

### Run all tests

```bash
go test ./... -v
```

### Test Coverage

```bash
go test ./... -cover
```

### Example: Testing Out-of-Order Messages

```bash
curl -X POST http://localhost:8088/messages -H "Content-Type: application/json" -d @integration/testdata/speed_increased_3.json
curl -X POST http://localhost:8088/messages -H "Content-Type: application/json" -d @integration/testdata/rocket_launched.json
curl -X POST http://localhost:8088/messages -H "Content-Type: application/json" -d @integration/testdata/speed_increased.json
curl http://localhost:8088/rockets/test-channel
```

- Expected response:

```json
{
    "channel": "test-channel",
    "type": "Falcon-9",
    "speed": 1000,
    "mission": "ARTEMIS",
    "status": "launched"
}
```





