docker rm -f zookeeper
docker rm -f kafka
docker rm -f schema-registry

docker run -d --name zookeeper -p 2181:2181 confluent/zookeeper
docker run -d --name kafka -p 9092:9092 --link zookeeper:zookeeper -e KAFKA_ADVERTISED_HOST_NAME=localhost confluent/kafka
while ! nc -z localhost 9092 </dev/null; do echo 'waiting for Kafka...'; sleep 1; done
sleep 2
docker run -d --name schema-registry -p 8081:8081 --link zookeeper:zookeeper --link kafka:kafka confluent/schema-registry
while ! nc -z localhost 8081 </dev/null; do echo 'waiting for Zookeeper...'; sleep 1; done
docker exec kafka kafka-topics --zookeeper zookeeper:2181 --create --topic req_topic --partitions 1 --replication-factor 1
docker exec kafka kafka-topics --zookeeper zookeeper:2181 --create --topic resp_topic --partitions 1 --replication-factor 1
