import os
import json
import time
import threading
from datetime import datetime, timezone
from collections import defaultdict
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    'KAFKA_BOOTSTRAP_SERVERS',
    'localhost:19092,localhost:19093,localhost:19094'
)

TELEMETRY_TOPIC = 'lamp-telemetry'
ANALYTICS_TOPIC = 'lamp-analytics'

consumer_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 'analytics-processor',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
    'isolation.level': 'read_committed',
}

producer_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'acks': 'all',
    'enable.idempotence': True,
}

lamp_data = defaultdict(lambda: {
    'last_seen': None,
    'status': 'unknown',
    'brightness_sum': 0,
    'power_sum': 0,
    'message_count': 0,
})

ANOMALY_THRESHOLD_SECONDS = 120


def delivery_callback(err, msg):
    if err is not None:
        print(f"[ERROR] Analytics delivery failed: {err}")
    else:
        print(f"[ANALYTICS] Sent to {msg.topic()}")


def process_telemetry(message_value: dict):
    lamp_id = message_value.get('lamp_id')
    if not lamp_id:
        return
    
    lamp_data[lamp_id]['last_seen'] = datetime.now(timezone.utc)
    lamp_data[lamp_id]['status'] = message_value.get('status', 'unknown')
    lamp_data[lamp_id]['brightness_sum'] += message_value.get('brightness', 0)
    lamp_data[lamp_id]['power_sum'] += message_value.get('power_consumption', 0)
    lamp_data[lamp_id]['message_count'] += 1
    lamp_data[lamp_id]['type'] = message_value.get('type', 'unknown')
    lamp_data[lamp_id]['location'] = message_value.get('location', 'unknown')


def detect_anomalies() -> list:
    anomalies = []
    now = datetime.now(timezone.utc)
    
    for lamp_id, data in lamp_data.items():
        if data['last_seen']:
            time_diff = (now - data['last_seen']).total_seconds()
            if time_diff > ANOMALY_THRESHOLD_SECONDS:
                anomalies.append({
                    'lamp_id': lamp_id,
                    'last_seen': data['last_seen'].isoformat(),
                    'seconds_since_response': int(time_diff),
                    'anomaly_type': 'no_response'
                })
    
    return anomalies


def calculate_analytics(period_start: datetime, period_end: datetime) -> dict:
    total_lamps = len(lamp_data)
    lamps_online = sum(1 for d in lamp_data.values() if d['status'] == 'on')
    lamps_offline = sum(1 for d in lamp_data.values() if d['status'] == 'off')
    
    total_power = sum(d['power_sum'] for d in lamp_data.values())
    total_brightness = sum(d['brightness_sum'] for d in lamp_data.values())
    total_messages = sum(d['message_count'] for d in lamp_data.values())
    
    avg_brightness = total_brightness / total_messages if total_messages > 0 else 0
    avg_power = total_power / total_messages if total_messages > 0 else 0
    
    return {
        'period_start': period_start.isoformat(),
        'period_end': period_end.isoformat(),
        'total_lamps': total_lamps,
        'lamps_online': lamps_online,
        'lamps_offline': lamps_offline,
        'lamps_unknown': total_lamps - lamps_online - lamps_offline,
        'total_power_consumption': round(total_power, 2),
        'avg_brightness': round(avg_brightness, 1),
        'avg_power_per_lamp': round(avg_power, 2),
        'messages_processed': total_messages,
        'anomalies': detect_anomalies()
    }


def reset_period_stats():
    for lamp_id in lamp_data:
        lamp_data[lamp_id]['brightness_sum'] = 0
        lamp_data[lamp_id]['power_sum'] = 0
        lamp_data[lamp_id]['message_count'] = 0


class AnalyticsTimer:
    def __init__(self, producer: Producer):
        self.producer = producer
        self.running = False
        self.thread = None
        self.period_start = datetime.now(timezone.utc)
    
    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
    
    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
    
    def _run(self):
        while self.running:
            time.sleep(60)
            if not self.running:
                break
            
            period_end = datetime.now(timezone.utc)
            analytics = calculate_analytics(self.period_start, period_end)
            
            print("\n" + "=" * 60)
            print("ANALYTICS REPORT")
            print("=" * 60)
            print(f"Period: {self.period_start.strftime('%H:%M:%S')} - {period_end.strftime('%H:%M:%S')}")
            print(f"Lamps: {analytics['total_lamps']} (online: {analytics['lamps_online']}, offline: {analytics['lamps_offline']})")
            print(f"Power: {analytics['total_power_consumption']}W, Avg brightness: {analytics['avg_brightness']}%")
            
            if analytics['anomalies']:
                print("[WARNING] Anomalies detected:")
                for a in analytics['anomalies']:
                    print(f"  - {a['lamp_id']}: no response for {a['seconds_since_response']}s")
            
            print("=" * 60 + "\n")
            
            self.producer.produce(
                topic=ANALYTICS_TOPIC,
                key='analytics'.encode('utf-8'),
                value=json.dumps(analytics).encode('utf-8'),
                callback=delivery_callback
            )
            self.producer.poll(0)
            
            reset_period_stats()
            self.period_start = period_end


def main():
    print("=" * 60)
    print("ANALYTICS PROCESSOR")
    print("=" * 60)
    print(f"Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Input: {TELEMETRY_TOPIC}, Output: {ANALYTICS_TOPIC}")
    print("=" * 60)
    
    consumer = Consumer(consumer_config)
    producer = Producer(producer_config)
    
    consumer.subscribe([TELEMETRY_TOPIC])
    
    analytics_timer = AnalyticsTimer(producer)
    analytics_timer.start()
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            
            try:
                value = json.loads(msg.value().decode('utf-8'))
                lamp_id = value.get('lamp_id', 'unknown')
                
                print(f"[TELEMETRY] {lamp_id}: status={value.get('status')}, brightness={value.get('brightness')}%")
                
                process_telemetry(value)
                consumer.commit(asynchronous=False)
                
            except json.JSONDecodeError as e:
                print(f"[ERROR] Parse error: {e}")
            except Exception as e:
                print(f"[ERROR] {e}")
                
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        analytics_timer.stop()
        consumer.close()
        producer.flush(timeout=10)


if __name__ == '__main__':
    main()
