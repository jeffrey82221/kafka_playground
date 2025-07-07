from tasks import process_data
from icecream import ic
for i in range(10):
    ic(process_data.apply_async(args=[f'Low Priority Data: {i}', 2], priority=10))

for i in range(100):
    ic(process_data.apply_async(args=[f'High Priority Data: {i}', 0.1], priority=0))