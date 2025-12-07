import os
import json
from datetime import datetime, timezone
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    'KAFKA_BOOTSTRAP_SERVERS',
    'localhost:19092,localhost:19093,localhost:19094'
)

COMMANDS_TOPIC = 'lamp-commands'
DLQ_TOPIC = 'lamp-commands-dlq'

VALID_ACTIONS = ['set_brightness', 'set_color_temp', 'turn_on', 'turn_off', 'toggle', 'set_schedule', 'reset']
VALID_LAMP_IDS = ['lamp_1', 'lamp_2', 'lamp_3', 'lamp_4', 'lamp_5']

consumer_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 'command-processor',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
    'isolation.level': 'read_committed',
}

producer_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'acks': 'all',
    'enable.idempotence': True,
}


def delivery_callback(err, msg):
    if err is not None:
        print(f"[ERROR] DLQ delivery failed: {err}")
    else:
        print(f"[DLQ] Sent to {msg.topic()}")


class CommandValidationError(Exception):
    def __init__(self, message: str, error_code: str):
        super().__init__(message)
        self.error_code = error_code


def validate_command(command: dict) -> None:
    required_fields = ['command_id', 'lamp_id', 'action', 'timestamp']
    for field in required_fields:
        if field not in command:
            raise CommandValidationError(f"Missing field: {field}", "MISSING_FIELD")
    
    if command['lamp_id'] not in VALID_LAMP_IDS:
        raise CommandValidationError(f"Invalid lamp_id: {command['lamp_id']}", "INVALID_LAMP_ID")
    
    if command['action'] not in VALID_ACTIONS:
        raise CommandValidationError(f"Invalid action: {command['action']}", "INVALID_ACTION")
    
    action = command['action']
    value = command.get('value')
    
    if action == 'set_brightness':
        if value is None:
            raise CommandValidationError("set_brightness requires value", "MISSING_VALUE")
        if not isinstance(value, (int, float)) or value < 0 or value > 100:
            raise CommandValidationError(f"Brightness must be 0-100, got: {value}", "INVALID_VALUE")
    
    elif action == 'set_color_temp':
        if value is None:
            raise CommandValidationError("set_color_temp requires value", "MISSING_VALUE")
        if not isinstance(value, (int, float)) or value < 2700 or value > 6500:
            raise CommandValidationError(f"Color temp must be 2700-6500, got: {value}", "INVALID_VALUE")


def execute_command(command: dict) -> dict:
    print(f"\n{'=' * 50}")
    print(f"EXECUTING COMMAND")
    print(f"{'=' * 50}")
    print(f"  Command ID: {command['command_id']}")
    print(f"  Lamp: {command['lamp_id']}")
    print(f"  Action: {command['action']}")
    if command.get('value') is not None:
        print(f"  Value: {command['value']}")
    print(f"{'=' * 50}")
    
    return {
        'command_id': command['command_id'],
        'lamp_id': command['lamp_id'],
        'action': command['action'],
        'status': 'executed',
        'executed_at': datetime.now(timezone.utc).isoformat()
    }


def send_to_dlq(producer: Producer, original_message: bytes, error: Exception, error_context: dict = None):
    try:
        original_value = json.loads(original_message.decode('utf-8'))
    except:
        original_value = original_message.decode('utf-8', errors='replace')
    
    dlq_message = {
        'original_message': original_value,
        'error': {
            'type': type(error).__name__,
            'message': str(error),
            'code': getattr(error, 'error_code', 'UNKNOWN'),
        },
        'context': error_context or {},
        'failed_at': datetime.now(timezone.utc).isoformat(),
        'source_topic': COMMANDS_TOPIC,
        'processor': 'command-processor'
    }
    
    print(f"[DLQ] Sending failed command: {dlq_message['error']['message']}")
    
    producer.produce(
        topic=DLQ_TOPIC,
        key=b'error',
        value=json.dumps(dlq_message).encode('utf-8'),
        callback=delivery_callback
    )
    producer.poll(0)


def main():
    print("=" * 60)
    print("COMMAND PROCESSOR")
    print("=" * 60)
    print(f"Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Input: {COMMANDS_TOPIC}, DLQ: {DLQ_TOPIC}")
    print("=" * 60)
    
    consumer = Consumer(consumer_config)
    producer = Producer(producer_config)
    
    consumer.subscribe([COMMANDS_TOPIC])
    
    commands_processed = 0
    commands_failed = 0
    
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
            
            print(f"\n[RECEIVED] Command from partition {msg.partition()}, offset {msg.offset()}")
            
            try:
                command = json.loads(msg.value().decode('utf-8'))
                validate_command(command)
                execute_command(command)
                commands_processed += 1
                print(f"[SUCCESS] Command {command['command_id']} executed")
                consumer.commit(asynchronous=False)
                
            except json.JSONDecodeError as e:
                commands_failed += 1
                print(f"[ERROR] JSON parse error: {e}")
                send_to_dlq(producer, msg.value(), e, {'partition': msg.partition()})
                consumer.commit(asynchronous=False)
                
            except CommandValidationError as e:
                commands_failed += 1
                print(f"[ERROR] Validation failed: {e}")
                send_to_dlq(producer, msg.value(), e, {'partition': msg.partition()})
                consumer.commit(asynchronous=False)
                
            except Exception as e:
                commands_failed += 1
                print(f"[ERROR] Unexpected: {e}")
                send_to_dlq(producer, msg.value(), e, {'partition': msg.partition()})
                consumer.commit(asynchronous=False)
                
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        producer.flush(timeout=10)
        consumer.close()
        print(f"Stats - Processed: {commands_processed}, Failed: {commands_failed}")


if __name__ == '__main__':
    main()
