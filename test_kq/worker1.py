import logging
from kafka import KafkaConsumer
from kq import Worker

# Set up Kafka consumer
consumer = KafkaConsumer(
    bootstrap_servers="127.0.0.1:9092",
    group_id="group",
    auto_offset_reset="latest"
)

# Set up worker
worker = Worker(
    topic="topic1", consumer=consumer
)
worker.start()






