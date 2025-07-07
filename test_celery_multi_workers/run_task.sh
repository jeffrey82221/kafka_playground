celery -A tasks worker --concurrency=1 -Q queue_a -n worker_a@%h
celery -A tasks worker --concurrency=1 -Q queue_b -n worker_b@%h