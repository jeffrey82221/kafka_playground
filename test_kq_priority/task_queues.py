from kafka import KafkaProducer
from kq import Queue
from icecream import ic
# Set up Kafka producer

# Set up queue

# Enqueue a function call

def high_priority_function(data):
    """
    Process the job by executing the function with its arguments.
    """
    import time
    ic(f"[high_priority_function] start with data: {data}")
    time.sleep(0.1)
    ic(f"[high_priority_function] end with data: {data}")
    


def low_priority_function(data):
    """
    Process the job by executing the function with its arguments.
    """
    import time
    print("[low_priority_function] start with data:", data)
    time.sleep(2)
    print("[low_priority_function] end data:", data)

producer = KafkaProducer(bootstrap_servers="127.0.0.1:9092")

high_priority_queue = Queue(topic="topic-hi", producer=producer)
low_priority_queue = Queue(topic="topic-low", producer=producer)

