import logging
from kafka import KafkaConsumer
from kq import Worker
from kq.message import Message
import threading
from icecream import ic
import queue

# Set up logging
formatter = logging.Formatter("[%(levelname)s] %(message)s")
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger = logging.getLogger("kq.worker")
logger.setLevel(logging.DEBUG)
logger.addHandler(stream_handler)

from typing import Optional, List
class PriorityWorker(Worker):
    def start(
        self, max_messages: Optional[int] = None, commit_offsets: bool = True
    ) -> int:
        """Start processing Kafka messages and executing jobs.

        :param max_messages: Maximum number of Kafka messages to process before
            stopping. If not set, worker runs until interrupted.
        :type max_messages: int | None
        :param commit_offsets: If set to True, consumer offsets are committed
            every time a message is processed (default: True).
        :type commit_offsets: bool
        :return: Total number of messages processed.
        :rtype: int
        """
        self._logger.info(f"Starting {self} ...")

        self._consumer.unsubscribe()
        self._consumer.subscribe([self.topic])

        messages_processed = 0
        while max_messages is None or messages_processed < max_messages:
            record = next(self._consumer)

            message = Message(
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
                key=record.key,
                value=record.value,
            )
            # TODO:
            # - [ ] Put message in a queue 
            # - [ ] Consume the queue in a separate thread
            self._process_message(message)
            # enqueue 

            if commit_offsets:
                self._consumer.commit()

            messages_processed += 1

        return messages_processed
# Set up Kafka consumer
consumer1 = KafkaConsumer(
    bootstrap_servers="127.0.0.1:9092",
    group_id="group",
    auto_offset_reset="latest"
)
consumer2 = KafkaConsumer(
    bootstrap_servers="127.0.0.1:9092",
    group_id="group",
    auto_offset_reset="latest"
)
worker1 = PriorityWorker(
    topic='topic-hi',
    consumer=consumer1
)
worker2 = PriorityWorker(
    topic='topic-low',
    consumer=consumer2
)
thread1 = threading.Thread(target=lambda: worker1.start())
thread2 = threading.Thread(target=lambda: worker2.start())

thread1.start()
thread2.start()
