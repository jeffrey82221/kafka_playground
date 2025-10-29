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

for i in range(10):
    key = f"id-{i}"
    value = {"number": i, "text": f"hello-{i}"}
    topic = "test"
    future = producer.send(topic, key=key, value=value)
    result = future.get(timeout=10)              # block until the broker acks
    time.sleep(0.2)
    print(f"✔ sent {value} to {topic} partition {result.partition}, offset {result.offset}")
producer.flush()   # make sure all buffered records are written
producer.close()