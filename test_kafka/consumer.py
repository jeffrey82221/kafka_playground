from kafka import KafkaConsumer
import json

# Configuration for the Kafka consumer
# Must match the producer's broker and topic
KAFKA_BROKER = 'linyixundeMacBook-Air.local:8092'
KAFKA_TOPIC = 'test-topic'
CONSUMER_GROUP = 'internal-group'

def create_consumer():
    """Creates and returns a KafkaConsumer instance."""
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest', # 'earliest',  # Start from the beginning if no offset exists
        enable_auto_commit=True,       # Automatically commit offsets
        # group_id=CONSUMER_GROUP,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    return consumer

def consume_messages(consumer):
    """Consumes messages from the Kafka topic."""
    print(f"Starting consumer for topic '{KAFKA_TOPIC}'...")
    print(f"Waiting for messages... (Press Ctrl+C to stop)\n")
    
    try:
        for message in consumer:
            print(f"Received message:")
            print(f"  Topic: {message.topic}")
            print(f"  Partition: {message.partition}")
            print(f"  Offset: {message.offset}")
            print(f"  Key: {message.key}")
            print(f"  Timestamp: {message.timestamp}")
            print(f"  Value: {message.value}")
            print(f"  Type of Value: {type(message.value)}")
            print("-" * 60)
            
    except KeyboardInterrupt:
        print("\n\nConsumer stopped by user.")
    except Exception as e:
        print(f"Error consuming messages: {e}")
    finally:
        consumer.close()
        print("Consumer closed.")

if __name__ == "__main__":
    consumer = create_consumer()
    consume_messages(consumer)
