from tasks import task_a
from icecream import ic

result = ic(task_a.delay())

ans = ic(result.get())
ic(result.successful())

