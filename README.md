# Kafka Streams Temperature Processor

A lightweight Kafka Streams application that reads temperature events from Kafka, transforms them, and writes a normalized output event using Confluent JSON Schema SerDe.

## What this project does

- Consumes string-key/string-value records from topic `temp`
- Expects input values to be JSON containing a top-level `payload` object
- Extracts metadata and measurement fields from that payload
- Converts temperature from Fahrenheit to Celsius
- Adds a UTC processing timestamp
- Publishes typed `ProcessedTemp` records to `processed_KafkaStreams_temp`
- Registers and uses JSON Schema via Schema Registry

## Tech stack

- Java 17
- Apache Kafka Streams `3.6.0`
- Jackson Databind `2.15.2`
- Confluent JSON Schema SerDe `7.9.0`
- Maven

## Project structure

```text
src/main/java/org/example/
├── KafkaStreamsProcessors.java   # Stream topology + app bootstrap
└── ProcessedTemp.java            # Output event model for JSON Schema SerDe
```

## Processing flow

1. Read from input topic `temp` as `<String, String>`
2. Parse JSON message body
3. Read fields from `payload`
4. Build `ProcessedTemp`
5. Compute `UPDATED_TEMPERATURE_CELSIUS = (F - 32) * 5 / 9`
6. Filter out invalid messages (parse errors or malformed data)
7. Write to `processed_KafkaStreams_temp` as `<String, ProcessedTemp>`

If a record is invalid, it is skipped and logged to stderr.

## Runtime configuration (current defaults)

These values are hardcoded in `KafkaStreamsProcessors`:

- Kafka bootstrap server: `kafka:9092`
- Schema Registry URL: `http://schema-registry:8081`
- Streams application id: `temperature_processor`
- Input topic: `temp`
- Output topic: `processed_KafkaStreams_temp`

This setup is typically intended for a Docker network where service names are `kafka` and `schema-registry`.

## Prerequisites

Before running the app, make sure you have:

- Java 17 installed
- Maven installed
- A running Kafka broker reachable at `kafka:9092` (or update code)
- A running Schema Registry reachable at `http://schema-registry:8081` (or update code)
- Input and output topics created

## Build

```bash
mvn clean package
```

## Run

Use Maven Exec Plugin directly:

```bash
mvn exec:java -Dexec.mainClass=org.example.KafkaStreamsProcessors
```

You should see logs similar to:

- `Listening to topic: temp`
- `Kafka Streams processor started.`

## Input message contract

The stream logic expects input values with this shape:

```json
{
  "payload": {
    "message_id": "abc-123",
    "timestamp": "2026-03-16T10:00:00.000000000",
    "kafka_timestamp": "2026-03-16T10:00:00.000000000",
    "temperature": 77.0,
    "payload": "optional raw payload text"
  }
}
```

### Field mapping

- Kafka record key -> `PRODUCER`
- `payload.message_id` -> `MESSAGE_ID`
- `payload.timestamp` -> `PRODUCED_TIMESTAMP`
- `payload.kafka_timestamp` -> `KAFKA_TIMESTAMP`
- `payload.temperature` -> `TEMPERATURE_FAHRENHEIT`
- Derived -> `UPDATED_TEMPERATURE_CELSIUS`
- Processing time (UTC now) -> `PROCESSED_TIMESTAMP`
- `payload.payload` -> `PAYLOAD`

## Output event

Output records are written with JSON Schema SerDe as `ProcessedTemp`:

- `PRODUCER` (String)
- `MESSAGE_ID` (String)
- `TEMPERATURE_FAHRENHEIT` (Double)
- `UPDATED_TEMPERATURE_CELSIUS` (Double)
- `PRODUCED_TIMESTAMP` (String)
- `KAFKA_TIMESTAMP` (String)
- `PROCESSED_TIMESTAMP` (String)
- `PAYLOAD` (String)

## Quick local verification

1. Start Kafka + Schema Registry
2. Start this app
3. Produce a test event into topic `temp`
4. Consume from `processed_KafkaStreams_temp` and verify transformed fields

If your broker tools are available, your producer input can use:

```json
{"payload":{"message_id":"m-1","timestamp":"2026-03-16T10:00:00.000000000","kafka_timestamp":"2026-03-16T10:00:00.000000000","temperature":86.0,"payload":"sensor-a"}}
```

Expected Celsius value:

- `86.0F` -> `30.0C`

## Error handling behavior

- Invalid JSON or missing/incorrect structure leads to record skip
- Skipped records are logged with key and error message
- Uncaught stream exceptions trigger application shutdown
- Graceful shutdown hook closes streams with timeout

## Troubleshooting

- **Cannot connect to Kafka**: verify `kafka:9092` is resolvable from where the app runs
- **Cannot connect to Schema Registry**: verify `http://schema-registry:8081` is reachable
- **No output records**: confirm input JSON has top-level `payload` object
- **Records skipped**: check stderr logs for parse/validation messages

## Suggested next improvements

- Make bootstrap servers, topic names, and registry URL configurable via environment variables
- Add automated tests with `TopologyTestDriver`
- Add dead-letter topic handling for malformed records
- Add metrics and structured logging for observability
