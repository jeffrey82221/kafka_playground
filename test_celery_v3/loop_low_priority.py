from tasks import process_data
from icecream import ic
for i in range(10):
    result = ic(process_data.apply_async(args=[f'Low Priority Data: {i}', 2], 
                                         priority=255))
    # ic(result.get())
    # ic(result.successful())
