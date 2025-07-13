# celery_app.py
from celery import Celery
from kombu import Exchange, Queue
import time

# Configure RabbitMQ connection
app = Celery('tasks', broker='amqp://guest:guest@localhost:5672//', 
             backend='rpc://')

# Priority Queue Configuration
app.conf.task_queues = (
    
    # Single queue approach - alternative configuration
    Queue('priority_queue', 
          Exchange('priority_queue', type='direct'), 
          routing_key='priority_queue',
          queue_arguments={'x-max-priority': 10}),
)

# Essential settings for priority queue functionality
app.conf.task_acks_late = True
app.conf.worker_prefetch_multiplier = 1
app.conf.task_default_queue = 'priority_queue'
app.conf.task_default_priority = 5


# Auto-discover tasks
app.autodiscover_tasks(['tasks'])



@app.task
def process_job(data, sleep_time):
    # Task logic goes here
    print("Start Processing data:", data)
    time.sleep(sleep_time)  # Simulate a time-consuming task
    print("Finish Processing data:", data)

