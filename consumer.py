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
    time.sleep(0.5)
