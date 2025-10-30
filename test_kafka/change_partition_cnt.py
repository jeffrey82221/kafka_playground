from kafka.admin.new_partitions import NewPartitions
from kafka.admin import KafkaAdminClient
BOOTSTRAP_SERVERS = 'linyixundeMacBook-Air.local:8092'
admin = KafkaAdminClient(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    request_timeout_ms=10000
)
admin.create_partitions({'test-topic2': NewPartitions(total_count=2)})