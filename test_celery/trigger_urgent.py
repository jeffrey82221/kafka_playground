from tasks import process_data
for i in range(30):
    print(f"Triggering high priority task {i}...")
    process_data.apply_async(args=[f'High Priority Data: {i}'], priority=1)  # Trigger the Celery