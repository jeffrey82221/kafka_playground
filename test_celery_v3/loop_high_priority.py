from tasks import process_job
from icecream import ic
for i in range(100):
    result = ic(process_job.apply_async(args=[f'High Priority Data: {i}', 0.1], 
                                         priority=10))
    # ic(result.get())
    # ic(result.successful())
