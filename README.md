# Smart Lamps Monitoring System

Fault-tolerant telemetry collection and smart lamp control system using Apache Kafka multi-broker cluster.

## Demo Video

📹 [Watch Demo](https://drive.google.com/file/d/1aOekRsIsWNTziw2Vgu_XgXuCa9M8ql6C/view?usp=sharing)

## Quick Start

```bash
docker-compose up -d
docker-compose ps
```

## Endpoints

- Kafka UI: http://localhost:8080
- REST API: http://localhost:8000
- API Docs: http://localhost:8000/docs

## REST API

```bash
curl http://localhost:8000/api/lamps
curl http://localhost:8000/api/lamps/lamp_1

curl -X POST http://localhost:8000/api/lamps/lamp_1/command \
  -H "Content-Type: application/json" \
  -d '{"action": "set_brightness", "value": 75}'

curl http://localhost:8000/api/analytics
```

## Demo Commands

```bash
docker exec -it kafka-broker-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe

docker exec -it kafka-broker-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic lamp-telemetry \
  --from-beginning --max-messages 5

docker exec -it kafka-broker-1 /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --describe --entity-type topics --entity-name lamp-telemetry
```

## Fault Tolerance Demo

```bash
docker stop kafka-broker-2

docker exec -it kafka-broker-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic lamp-telemetry

curl http://localhost:8000/api/lamps

docker start kafka-broker-2
```

## DLQ Test

```bash
echo '{"command_id": "test", "lamp_id": "invalid", "action": "test", "timestamp": "2024-01-01T00:00:00Z"}' | \
  docker exec -i kafka-broker-1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic lamp-commands

docker exec -it kafka-broker-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic lamp-commands-dlq --from-beginning
```

## Stop

```bash
docker-compose down -v
```
