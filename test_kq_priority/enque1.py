from icecream import ic
from task_queues import (
    low_priority_function, low_priority_queue)
for i in range(10):
    # You can specify job timeout, Kafka message key and partition
    job = ic(low_priority_queue.enqueue(
        low_priority_function, f"topic-low send data: {i}"
    ))

