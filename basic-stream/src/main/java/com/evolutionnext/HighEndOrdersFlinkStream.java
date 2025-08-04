package com.evolutionnext;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class HighEndOrdersFlinkStream {

    public static class MyRecord {
        private final String key;
        private final int value;

        public MyRecord(String key, int value) {
            this.key = key;
            this.value = value;
        }

        public String key() {
            return key;
        }

        public int value() {
            return value;
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<MyRecord> kafkaSource = KafkaSource.<MyRecord>builder()
            .setBootstrapServers("broker:29092")
            .setTopics("my_orders")
            .setGroupId("my_group_id")
            .setDeserializer(myRecordDeserializer())
            .build();

        KafkaSink<MyRecord> kafkaSink = KafkaSink.<MyRecord>builder()
            .setBootstrapServers("broker:29092")
            .setRecordSerializer(myRecordSerializer())
            .build();

        DataStream<MyRecord> inputStream = env.fromSource(kafkaSource,
            WatermarkStrategy.forMonotonousTimestamps(), "Kafka Source")
            .setParallelism(3);

        DataStream<MyRecord> filteredStream = inputStream
            .filter(record -> record.value > 40000).setParallelism(3);

        filteredStream.sinkTo(kafkaSink).setParallelism(3);

        env.execute("High Value Orders Stream");
    }

    private static KafkaRecordDeserializationSchema<MyRecord> myRecordDeserializer() {
        return new KafkaRecordDeserializationSchema<>() {
            @Override
            public TypeInformation<MyRecord> getProducedType() {
                return TypeInformation.of(MyRecord.class);
            }

            @Override
            public void deserialize(ConsumerRecord<byte[], byte[]> record,
                                    Collector<MyRecord> collector) throws IOException {
                String key = new String(record.key(), StandardCharsets.UTF_8);
                collector.collect(new MyRecord(key, ByteBuffer.wrap(record.value()).getInt()));
            }
        };
    }

    private static KafkaRecordSerializationSchema<MyRecord> myRecordSerializer() {
        return (myRecord, kafkaSinkContext, aLong) -> {
            byte[] valueArray = ByteBuffer.allocate(4).putInt(myRecord.value()).array();
            return new ProducerRecord<>("my_high_end_orders",
                myRecord.key().getBytes(), valueArray);
        };
    }
}
