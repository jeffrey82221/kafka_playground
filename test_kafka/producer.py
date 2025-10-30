from kafka import KafkaProducer
import json
import time

# Configuration for the Kafka producer
# Replace 'localhost:9092' with your Kafka broker address(es) if different
KAFKA_BROKER = 'linyixundeMacBook-Air.local:8092'
KAFKA_TOPIC = 'test-topic2'

def create_producer():
    """Creates and returns a KafkaProducer instance."""
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    return producer

def send_message(producer, topic, message):
    """Sends a message to the specified Kafka topic."""
    try:
        future = producer.send(topic, message)
        record_metadata = future.get(timeout=10) # Block until message is sent
        print(f"Message sent successfully to topic: {record_metadata.topic}, "
              f"partition: {record_metadata.partition}, "
              f"offset: {record_metadata.offset}")
    except Exception as e:
        print(f"Error sending message: {e}")

if __name__ == "__main__":
    producer = create_producer()

    # Send a few sample messages
    for i in range(100):
        data = {
            'id': i,
            'message': f'Hello Kafka from Python! Message number {i}',
            'timestamp': time.time()
        }
        send_message(producer, KAFKA_TOPIC, data)
        time.sleep(0.1) # Simulate some delay between messages

    # Ensure all messages are sent before closing the producer
    producer.flush()
    producer.close()
    print("Producer closed.")