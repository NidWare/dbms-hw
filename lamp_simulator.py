import os
import json
import time
import random
from datetime import datetime, timezone
from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    'KAFKA_BOOTSTRAP_SERVERS',
    'localhost:19092,localhost:19093,localhost:19094'
)

TELEMETRY_TOPIC = 'lamp-telemetry'

producer_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'acks': 'all',
    'retries': 5,
    'enable.idempotence': True,
    'delivery.timeout.ms': 30000,
    'linger.ms': 5,
    'batch.size': 16384,
}

LAMPS = [
    {'lamp_id': 'lamp_1', 'type': 'color_bulb', 'location': 'living_room'},
    {'lamp_id': 'lamp_2', 'type': 'white_bulb', 'location': 'bedroom'},
    {'lamp_id': 'lamp_3', 'type': 'led_strip', 'location': 'kitchen'},
    {'lamp_id': 'lamp_4', 'type': 'smart_spotlight', 'location': 'hallway'},
    {'lamp_id': 'lamp_5', 'type': 'dimmable_bulb', 'location': 'bathroom'},
]

lamp_states = {
    lamp['lamp_id']: {
        'status': 'on',
        'brightness': random.randint(50, 100),
        'color_temp': random.randint(2700, 6500),
    }
    for lamp in LAMPS
}


def delivery_callback(err, msg):
    if err is not None:
        print(f"[ERROR] Message delivery failed: {err}")
    else:
        print(f"[OK] Delivered to {msg.topic()} [partition={msg.partition()}, offset={msg.offset()}]")


def generate_telemetry(lamp_info: dict) -> dict:
    lamp_id = lamp_info['lamp_id']
    state = lamp_states[lamp_id]
    
    if random.random() < 0.1:
        state['status'] = 'off' if state['status'] == 'on' else 'on'
    
    if state['status'] == 'on':
        state['brightness'] = max(1, min(100, state['brightness'] + random.randint(-5, 5)))
        power = (state['brightness'] / 100) * random.uniform(8, 12)
    else:
        power = 0.1
    
    return {
        'lamp_id': lamp_id,
        'type': lamp_info['type'],
        'location': lamp_info['location'],
        'status': state['status'],
        'brightness': state['brightness'] if state['status'] == 'on' else 0,
        'color_temp': state['color_temp'],
        'power_consumption': round(power, 2),
        'voltage': round(random.uniform(218, 222), 1),
        'timestamp': datetime.now(timezone.utc).isoformat()
    }


def main():
    print("=" * 60)
    print("LAMP SIMULATOR")
    print("=" * 60)
    print(f"Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {TELEMETRY_TOPIC}")
    print(f"Lamps: {len(LAMPS)}")
    print("=" * 60)
    
    producer = Producer(producer_config)
    message_count = 0
    
    try:
        while True:
            for lamp in LAMPS:
                telemetry = generate_telemetry(lamp)
                message_count += 1
                
                print(f"[{message_count}] {lamp['lamp_id']}: status={telemetry['status']}, "
                      f"brightness={telemetry['brightness']}%, power={telemetry['power_consumption']}W")
                
                producer.produce(
                    topic=TELEMETRY_TOPIC,
                    key=telemetry['lamp_id'].encode('utf-8'),
                    value=json.dumps(telemetry).encode('utf-8'),
                    callback=delivery_callback
                )
                producer.poll(0)
            
            producer.flush()
            print("--- Waiting 15 seconds ---")
            time.sleep(15)
            
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        producer.flush(timeout=10)


if __name__ == '__main__':
    main()
