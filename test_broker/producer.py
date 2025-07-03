from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewPartitions

import json, time

# Initialise a producer that serialises keys and values to bytes.
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=5,          # small batch delay to improve throughput
    acks="all"            # wait for broker confirmation
)


def batch_increase_partitions(topic_partition_map):
    """
    Increase partitions for multiple topics at once
    
    Args:
        topic_partition_map (dict): Dictionary mapping topic names to desired partition counts
    """
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092")
    
    # Create batch update request
    partition_updates = {}
    for topic_name, partition_count in topic_partition_map.items():
        partition_updates[topic_name] = NewPartitions(total_count=partition_count)
    
    try:
        result = admin_client.create_partitions(partition_updates)
        
        # Process results for each topic
        for topic, future in result.topic_errors.items():
            try:
                future.result()
                print(f"✓ Updated {topic} to {topic_partition_map[topic]} partitions")
            except Exception as e:
                print(f"✗ Failed to update {topic}: {e}")
                
    except Exception as e:
        print(f"✗ Batch update failed: {e}")
    finally:
        admin_client.close()

# Example usage
topics_to_update = {
    "demo-topic": 2,
    "demo-topic-hi": 1
}

batch_increase_partitions(topics_to_update)

for i in range(100):
    key = f"id-{i}"
    value = {"number": i, "text": f"hello-{i}"}
    topic = "demo-topic"
    future = producer.send(topic, key=key, value=value)
    result = future.get(timeout=10)              # block until the broker acks
    time.sleep(0.2)
    print(f"✔ sent {value} to {topic} partition {result.partition}, offset {result.offset}")
for i in range(20):
    topic = "demo-topic-hi"
    future = producer.send(topic, key=key, value=value)
    result = future.get(timeout=10)              # block until the broker acks
    print(f"✔ sent {value} to {topic} partition {result.partition}, offset {result.offset}")
    

producer.flush()   # make sure all buffered records are written
producer.close()