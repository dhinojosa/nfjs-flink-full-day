docker exec -it broker kafka-console-consumer \
    --topic my_orders \
    --bootstrap-server broker:9092 \
    --property print.key=true \
    --from-beginning \
    --property key.separator=" : " \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.IntegerDeserializer
