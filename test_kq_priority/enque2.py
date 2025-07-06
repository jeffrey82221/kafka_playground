from icecream import ic
from task_queues import (
    high_priority_function, high_priority_queue)
for i in range(100):
    # You can specify job timeout, Kafka message key and partition
    job = ic(high_priority_queue.enqueue(
        high_priority_function, f"topic-hi send data: {i}"
    ))

