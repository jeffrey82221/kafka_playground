from celery import Celery
import time
app = Celery('tasks', broker='kafka://localhost:9092')
# app = Celery('tasks', broker='amqp://guest:guest@localhost:5672//')

@app.task
def process_data(data):
    # Task logic goes here
    print("Start Processing data:", data)
    time.sleep(1)  # Simulate a time-consuming task
    print("Finish Processing data:", data)