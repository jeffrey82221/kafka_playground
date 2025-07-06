from tasks import process_data

for i in range(30):
    print(f"Triggering low priority task {i}...")
    process_data.apply_async(args=[f'Low Priority Data: {i}'], priority=6)  # Trigger the Celery