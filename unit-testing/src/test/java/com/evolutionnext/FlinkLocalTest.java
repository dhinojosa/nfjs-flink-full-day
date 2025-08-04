package com.evolutionnext;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;


public class FlinkLocalTest extends AbstractTestBase {

    @Test
    public void testPipeline() throws Exception {
        // Create a local StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // Simplify testing with a single thread

        // Input test data
        List<String> inputData = List.of("order_1,500", "order_2,600", "order_3,300");

        DataStream<String> inputStream = env.fromData(inputData);

        // Pipeline: Transform the input
        DataStream<String> resultStream = inputStream
            .map(line -> {
                String[] parts = line.split(",");
                String orderId = parts[0];
                int amount = Integer.parseInt(parts[1]);
                return orderId + ":" + (amount > 400 ? "high" : "low");
            });

        // Collect output for assertions
        CollectingSink.values.clear(); // Clear any previous runs
        resultStream.addSink(new CollectingSink());

        // Execute the pipeline
        env.execute();

        // Verify the output
        List<String> expectedOutput = List.of("order_1:high", "order_2:high", "order_3:low");
        assertThat(expectedOutput).containsExactlyElementsOf(CollectingSink.values);
    }

    // Custom Sink to collect output for assertions
    private static class CollectingSink implements SinkFunction<String> {
        static final List<String> values = new ArrayList<>();

        @Override
        public synchronized void invoke(String value, Context context) {
            values.add(value);
        }
    }
}
