from tasks import process_job
from icecream import ic
for i in range(10):
    result = ic(process_job.apply_async(
        args=[f'Low Priority Data: {i}', 5], 
        queue='low_priority',
        routing_key='low_priority',
        priority=255))
    # ic(result.get())
    # ic(result.successful())
