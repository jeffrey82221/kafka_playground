from celery import Celery

app = Celery(
    "multi_topic_demo",
    broker="kafka://localhost:9092/",
    backend="rpc://",
)

# Configure task routing to map tasks to specific topics
app.conf.task_routes = {
    'tasks.image_processing.*': {'queue': 'image_processing'},
    'tasks.email_notifications.*': {'queue': 'email_notifications'},
    'tasks.data_analytics.*': {'queue': 'data_analytics'},
    'tasks.file_uploads.*': {'queue': 'file_uploads'},
}

# Kafka transport options
app.conf.broker_transport_options = {
    "acks": "all",
    "consumer": {
        "group.id": "celery-workers",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    },
    "producer": {
        "linger.ms": 5,
        "batch.num.messages": 1000,
    },
}