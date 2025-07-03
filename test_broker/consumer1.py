import threading
from kafka import KafkaConsumer
import json
import time

def worker_thread(worker_id):
    """Worker thread with its own consumer instance in the same group"""
    
    consumer = KafkaConsumer(
        "demo-topic",
        bootstrap_servers="localhost:9092",
        group_id="shared-worker-group",  # Same group for load balancing
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True
    )
    
    print(f"Worker {worker_id} started...")
    
    try:
        for message in consumer:
            print(f"[Worker-{worker_id}] Processing: {message.value} on partition: {message.partition}")
            # Simulate work
            time.sleep(0.1)
            
    except KeyboardInterrupt:
        print(f"Worker {worker_id} stopping...")
    finally:
        consumer.close()

def main():
    # Create two worker threads sharing the same consumer group
    workers = []
    for i in range(2):
        worker = threading.Thread(target=worker_thread, args=(i+1,))
        workers.append(worker)
        worker.start()
    
    try:
        for worker in workers:
            worker.join()
    except KeyboardInterrupt:
        print("Shutting down...")

if __name__ == "__main__":
    main()