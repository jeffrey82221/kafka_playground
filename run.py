from celery_app import add
result = add.delay(2, 40)          # produces a record to topic "celery"
print(result.get())                # -> 42