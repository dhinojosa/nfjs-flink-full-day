docker exec broker kafka-topics --create --topic my_orders --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec broker kafka-topics --create --topic my_high_end_orders --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
