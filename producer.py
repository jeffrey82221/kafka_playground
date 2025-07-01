from kafka import KafkaProducer
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
    topic = "demo-topic"
    future = producer.send(topic, key=key, value=value)
    result = future.get(timeout=10)              # block until the broker acks
    print(f"✔ sent {value} to {topic} partition {result.partition}, offset {result.offset}")
    topic = "demo-topic-hi"
    future = producer.send(topic, key=key, value=value)
    result = future.get(timeout=10)              # block until the broker acks
    print(f"✔ sent {value} to {topic} partition {result.partition}, offset {result.offset}")
    time.sleep(0.1)

producer.flush()   # make sure all buffered records are written
producer.close()