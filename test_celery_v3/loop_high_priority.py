from tasks import process_data
from icecream import ic
for i in range(100):
    result = ic(process_data.apply_async(args=[f'High Priority Data: {i}', 0.1], 
                                         priority=0))
    # ic(result.get())
    # ic(result.successful())
