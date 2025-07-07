from tasks import process_data
from icecream import ic

result = ic(process_data.delay('A block trigger', 5))

ans = ic(result.get())
ic(result.successful())

