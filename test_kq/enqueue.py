import requests
from kafka import KafkaProducer
from kq import Queue
# Set up Kafka producer

# Set up queue

# Enqueue a function call

def process_data1(data):
    """
    Process the job by executing the function with its arguments.
    """
    import time
    print("[process_data1] start with topic1 with data:", data)
    time.sleep(0.1)  
    print("[process_data1] end with topic1 with data:", data)


def process_data2(data):
    """
    Process the job by executing the function with its arguments.
    """
    import time
    print("[process_data2] start with topic2 with data:", data)
    time.sleep(0.1)  
    print("[process_data2] end with topic2 with data:", data)

producer = KafkaProducer(bootstrap_servers="127.0.0.1:9092")

queue1 = Queue(topic="topic1", producer=producer)
# You can specify job timeout, Kafka message key and partition
for i in range(10):
    job = queue1.enqueue(
        process_data1, f"To topic 1 the sent data: {i}"
    )

queue2 = Queue(topic="topic2", producer=producer)
for i in range(10):
    # You can specify job timeout, Kafka message key and partition
    job = queue2.using().enqueue(
        process_data2, f"To topic2 the sent data: {i}"
    )
