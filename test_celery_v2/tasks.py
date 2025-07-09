from celery import Celery
from kombu import Exchange, Queue

import time
# app = Celery('tasks', broker='kafka://localhost:9092')
app = Celery('tasks', broker='amqp://guest:guest@localhost:5672//', 
             backend='rpc://')

app.conf.task_queues = [
    Queue(
        'low_priority',
        Exchange('low_priority'),
        routing_key='low_priority',
        queue_arguments = {'x-max-priority': 255}
    ),
    Queue(
        'high_priority',
        Exchange('high_priority'),
        routing_key='high_priority',
        queue_arguments = {'x-max-priority': 0}
    )
]
app.conf.task_queue_max_priority = 10
app.conf.task_default_priority = 5

app.conf.worker_prefetch_multiplier = 1
app.conf.task_acks_late = True



@app.task
def process_job(data, sleep_time):
    # Task logic goes here
    print("Start Processing data:", data)
    time.sleep(sleep_time)  # Simulate a time-consuming task
    print("Finish Processing data:", data)
