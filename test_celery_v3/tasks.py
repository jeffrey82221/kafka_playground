from celery import Celery
import time

# app = Celery('tasks', broker='kafka://localhost:9092')
from celery import Celery

app = Celery(
    "tasks",
    broker="redis://localhost:6379/0",
    broker_connection_retry_on_startup=True,
)

app.conf.broker_transport_options = {
    "priority_steps": list(range(10)),
    "sep": ":",
    "queue_order_strategy": "priority",
}


@app.task
def process_data(data, sleep_time):
    # Task logic goes here
    print("Start Processing data:", data)
    time.sleep(sleep_time)  # Simulate a time-consuming task
    print("Finish Processing data:", data)

@app.task
def process_data_fail(data):
    raise ValueError("This task is fail simulation.")