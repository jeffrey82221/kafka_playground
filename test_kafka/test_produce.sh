docker exec -it producer-internal /opt/kafka/bin/kafka-console-producer.sh \
  --topic test-topic \
  --bootstrap-server kafka:9093