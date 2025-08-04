docker exec broker kafka-topics --delete --topic my_orders --bootstrap-server localhost:9092
docker exec broker kafka-topics --delete --topic my_high_end_orders --bootstrap-server localhost:9092
