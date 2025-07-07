from celery import Celery
import time
app = Celery('tasks', 
             broker='redis://localhost:6379/0', backend='redis://')


@app.task(queue='queue_a')
def task_a():
    print("[task_a] Start processing task A")

@app.task(queue='queue_b')
def task_b():
    print("[task_b] Start processing task B")