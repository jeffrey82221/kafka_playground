docker run -d -p 9092:9092 --name broker apache/kafka:latest
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management