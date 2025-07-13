from tasks import process_job
from icecream import ic
for i in range(10):
    result = ic(process_job.apply_async(args=[f'Low Priority Data: {i}', 2], 
                                         priority=0))
    # ic(result.get())
    # ic(result.successful())
