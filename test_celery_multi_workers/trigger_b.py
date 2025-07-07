from tasks import task_b
from icecream import ic

result = ic(task_b.delay())

ans = ic(result.get())
ic(result.successful())

