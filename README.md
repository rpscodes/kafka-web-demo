# Kafka Web Demo (Plain Java, no Spring)

Simple Java app that:
- exposes a tiny web UI (port 8080) to produce records to Kafka
- runs a background consumer and shows recent messages

## Build locally
```
mvn -q -e -DskipTests package
java -jar target/app.jar
```
Visit http://localhost:8080

Environment:
- `KAFKA_BOOTSTRAP` (default `kafka-kafka-bootstrap:9092`)
- `KAFKA_TOPIC` (default `demo`)
- `KAFKA_GROUP` (default `web-demo`)
- `PORT` (default `8080`)
