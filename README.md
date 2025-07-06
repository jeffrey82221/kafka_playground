# How to solve priority queue problem? 


1. Dispatcher using KQ approach to send job to Kafka and consumer the job via 
    workers (there should be )
    - [ ] There are high-priority queue and low-pririty queue
            for sending high priority jobs and low priority jobs
            (also high priority worker and low priority worker)
    - [ ] The high priority job direclty using delay to start celery job.
    - [ ] The low priority job will check whether the celery qeueu is full and decide 
        whether to start the celery job. 



# Image build

```bash
docker run -d -p 9092:9092 --name broker apache/kafka:latest
```

# Python client install

```bash
pip install kafka-python
```

# Producer

```python
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

topic = "demo-topic"

for i in range(10):
    key = f"id-{i}"
    value = {"number": i, "text": f"hello-{i}"}
    future = producer.send(topic, key=key, value=value)
    result = future.get(timeout=10)              # block until the broker acks
    print(f"✔ sent {value} to partition {result.partition}, offset {result.offset}")
    time.sleep(0.1)

producer.flush()   # make sure all buffered records are written
producer.close()
```


# Consumer: 

```python
from kafka import KafkaConsumer
import json
import time

consumer = KafkaConsumer(
    "demo-topic",
    bootstrap_servers="localhost:9092",
    group_id="demo-consumer-group",
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",   # start at beginning if no committed offset
    enable_auto_commit=True
)

print("⏳ waiting for messages…\n")
for message in consumer:
    print(
        f"📨 partition={message.partition}  "
        f"offset={message.offset}  "
        f"key={message.key}  value={message.value}"
    )
    time.sleep(5)
```
