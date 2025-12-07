import os
import json
import uuid
from datetime import datetime, timezone
from typing import Optional, Any, Dict
import threading
import time

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import uvicorn
from confluent_kafka import Producer, Consumer, KafkaError

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    'KAFKA_BOOTSTRAP_SERVERS',
    'localhost:19092,localhost:19093,localhost:19094'
)

TELEMETRY_TOPIC = 'lamp-telemetry'
COMMANDS_TOPIC = 'lamp-commands'
ANALYTICS_TOPIC = 'lamp-analytics'

producer_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'acks': 'all',
    'enable.idempotence': True,
}

consumer_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 'rest-api-consumer',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': True,
}

lamp_states: Dict[str, Dict[str, Any]] = {}
latest_analytics: Dict[str, Any] = {}

app = FastAPI(title="Smart Lamps API", version="1.0.0")
producer = None


class CommandRequest(BaseModel):
    action: str = Field(..., examples=["set_brightness", "turn_on", "turn_off"])
    value: Optional[Any] = None
    priority: str = Field("normal", examples=["low", "normal", "high"])


class CommandResponse(BaseModel):
    command_id: str
    lamp_id: str
    action: str
    status: str
    message: str


def delivery_callback(err, msg):
    if err is not None:
        print(f"[ERROR] Command delivery failed: {err}")
    else:
        print(f"[OK] Command sent to {msg.topic()}")


def telemetry_consumer_thread():
    consumer = Consumer({
        **consumer_config,
        'group.id': 'rest-api-telemetry-' + str(uuid.uuid4())[:8],
    })
    consumer.subscribe([TELEMETRY_TOPIC])
    
    while True:
        try:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                continue
            
            try:
                telemetry = json.loads(msg.value().decode('utf-8'))
                lamp_id = telemetry.get('lamp_id')
                if lamp_id:
                    lamp_states[lamp_id] = {
                        'lamp_id': lamp_id,
                        'type': telemetry.get('type', 'unknown'),
                        'location': telemetry.get('location', 'unknown'),
                        'status': telemetry.get('status', 'unknown'),
                        'brightness': telemetry.get('brightness', 0),
                        'color_temp': telemetry.get('color_temp', 0),
                        'power_consumption': telemetry.get('power_consumption', 0),
                        'voltage': telemetry.get('voltage', 0),
                        'last_update': telemetry.get('timestamp', datetime.now(timezone.utc).isoformat())
                    }
            except json.JSONDecodeError:
                pass
        except Exception as e:
            print(f"[ERROR] Telemetry consumer: {e}")
            time.sleep(1)


def analytics_consumer_thread():
    global latest_analytics
    consumer = Consumer({
        **consumer_config,
        'group.id': 'rest-api-analytics-' + str(uuid.uuid4())[:8],
    })
    consumer.subscribe([ANALYTICS_TOPIC])
    
    while True:
        try:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                continue
            
            try:
                latest_analytics = json.loads(msg.value().decode('utf-8'))
            except json.JSONDecodeError:
                pass
        except Exception as e:
            print(f"[ERROR] Analytics consumer: {e}")
            time.sleep(1)


@app.on_event("startup")
async def startup_event():
    global producer
    producer = Producer(producer_config)
    
    threading.Thread(target=telemetry_consumer_thread, daemon=True).start()
    threading.Thread(target=analytics_consumer_thread, daemon=True).start()
    
    print(f"REST API started, Kafka: {KAFKA_BOOTSTRAP_SERVERS}")


@app.get("/")
async def root():
    return {
        "service": "Smart Lamps API",
        "version": "1.0.0",
        "endpoints": [
            "GET /api/lamps",
            "GET /api/lamps/{lamp_id}",
            "POST /api/lamps/{lamp_id}/command",
            "GET /api/analytics",
            "GET /health"
        ]
    }


@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "known_lamps": len(lamp_states)
    }


@app.get("/api/lamps")
async def get_all_lamps():
    return {
        "lamps": list(lamp_states.values()),
        "count": len(lamp_states),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.get("/api/lamps/{lamp_id}")
async def get_lamp(lamp_id: str):
    if lamp_id not in lamp_states:
        raise HTTPException(status_code=404, detail=f"Lamp '{lamp_id}' not found")
    return lamp_states[lamp_id]


@app.post("/api/lamps/{lamp_id}/command", response_model=CommandResponse)
async def send_command(lamp_id: str, command_request: CommandRequest):
    global producer
    
    if producer is None:
        raise HTTPException(status_code=503, detail="Producer not initialized")
    
    valid_lamp_ids = ['lamp_1', 'lamp_2', 'lamp_3', 'lamp_4', 'lamp_5']
    if lamp_id not in valid_lamp_ids:
        raise HTTPException(status_code=400, detail=f"Invalid lamp_id: {lamp_id}")
    
    command_id = f"cmd_{uuid.uuid4().hex[:8]}"
    command = {
        "command_id": command_id,
        "lamp_id": lamp_id,
        "action": command_request.action,
        "value": command_request.value,
        "priority": command_request.priority,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "rest-api"
    }
    
    try:
        producer.produce(
            topic=COMMANDS_TOPIC,
            key=lamp_id.encode('utf-8'),
            value=json.dumps(command).encode('utf-8'),
            callback=delivery_callback
        )
        producer.flush(timeout=5)
        
        print(f"[COMMAND] {lamp_id}: {command_request.action}")
        
        return CommandResponse(
            command_id=command_id,
            lamp_id=lamp_id,
            action=command_request.action,
            status="sent",
            message=f"Command sent to {COMMANDS_TOPIC}"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to send: {str(e)}")


@app.get("/api/analytics")
async def get_analytics():
    if not latest_analytics:
        return {"message": "No analytics data yet", "analytics": None}
    return latest_analytics


def main():
    print("=" * 60)
    print("REST API SERVICE")
    print("=" * 60)
    print(f"Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print("=" * 60)
    
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")


if __name__ == '__main__':
    main()
