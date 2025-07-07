from celery import Celery
from kombu import Exchange, Queue
import time
# app = Celery('tasks', broker='kafka://localhost:9092')
# app = Celery('tasks', broker='amqp://guest:guest@localhost:5672//')
app = Celery('tasks', 
             broker='redis://localhost:6379/0', backend='redis://')
#app.conf.task_queues = [
#    Queue('tasks', Exchange('tasks'), routing_key='tasks',
#          queue_arguments={'x-max-priority': 10}),
#]
# app.conf.task_queue_max_priority = 10
# app.conf.task_default_priority = 5

@app.task
def process_data(data, sleep_time):
    # Task logic goes here
    print("Start Processing data:", data)
    time.sleep(sleep_time)  # Simulate a time-consuming task
    print("Finish Processing data:", data)

@app.task
def process_data_fail(data):
    raise ValueError("This task is fail simulation.")