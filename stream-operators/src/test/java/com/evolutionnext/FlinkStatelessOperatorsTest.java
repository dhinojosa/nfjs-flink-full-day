package com.evolutionnext;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.CloseableIterator;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;


public class FlinkStatelessOperatorsTest extends AbstractTestBase {

    @Test
    public void testMap() throws Exception {
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

        try (CloseableIterator<String> stringCloseableIterator = resultStream.executeAndCollect()) {
            // Collect results into a List for verification
            List<String> actualOutput = iteratorToList(stringCloseableIterator);

            // Verify the output
            List<String> expectedOutput = List.of("order_1:high", "order_2:high", "order_3:low");
            assertThat(actualOutput).containsExactlyElementsOf(expectedOutput);
        }
    }

    @Test
    public void testFlatMap() throws Exception {
        // Create a local StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // Simplify testing with a single thread

        // Input test data
        List<String> inputData = List.of("1", "2", "3");

        DataStream<String> inputStream = env.fromData(inputData);

        // Pipeline: Transform the input
        DataStream<String> resultStream = inputStream
            .flatMap((FlatMapFunction<String, String>) (value, out) -> {
                out.collect(value);
                out.collect(value + "!");
            }, TypeInformation.of(String.class));

        try (CloseableIterator<String> stringCloseableIterator = resultStream.executeAndCollect()) {
            // Collect results into a List for verification
            List<String> actualOutput = iteratorToList(stringCloseableIterator);

            // Verify the output
            List<String> expectedOutput = List.of("1", "1!", "2", "2!", "3", "3!");
            assertThat(actualOutput).containsExactlyElementsOf(expectedOutput);
        }
    }

    @Test
    public void testFilter() throws Exception {
        // Create a local StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // Simplify testing with a single thread

        // Input test data
        List<Integer> inputData = List.of(1, 2, 3, 4);

        DataStream<Integer> inputStream = env.fromData(inputData);
        // Pipeline: Transform the input
        DataStream<Integer> resultStream = inputStream
            .filter(value -> value % 2 == 0);
        DataStream<Integer> expectedStream = env.fromData(List.of(2, 4));
        assertStreamEquals(resultStream, expectedStream);
    }



    @Test
    public void testKeyByNoAssertion() throws Exception {
        // Create a local StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2); //Two threads

        DataStream<Long> inputStream = env.fromSequence(0, 10000);

        // partition by even or odd
        var keyedStream = inputStream.keyBy(
            value -> value % 2,
            TypeInformation.of(Long.class));

        SingleOutputStreamOperator<String> mappedStream = keyedStream.map(new RichMapFunction<>() {
            @Override
            public String map(Long value) throws Exception {
                System.out.println("In Map");
                int partition = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
                return "Partition " + partition + ": " + value;
            }
        });
        System.out.println("Parallelism is " + mappedStream.getParallelism());

        mappedStream.sinkTo(new PrintSink<>("sink"));
        Thread.sleep(1000);
        env.execute("key by no assertion");
    }

    @Test
    void testForkingOrFanOut() throws Exception {
        // Create a local StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // Simplify testing with a single thread

        DataStream<Long> inputStream = env.fromSequence(0, 100);
        inputStream.map(value -> value % 2 == 0 ? "even" : "odd").sinkTo(new PrintSink<>("even-odd"));
        inputStream.flatMap((value, out) -> {
            out.collect(value);
            out.collect(value + 1);
            out.collect(value - 1);
            out.collect(value * 2);
        }, TypeInformation.of(Long.class)).sinkTo(new PrintSink<>("explode"));

        env.execute("forking or fan out");
    }

    private static <T> void assertStreamEquals(DataStream<T> resultStream, DataStream<T> expectedStream) throws Exception {
        try (CloseableIterator<T> actualIterator = resultStream.executeAndCollect();
             CloseableIterator<T> expectedIterator = expectedStream.executeAndCollect()) {
            List<T> actualOutput = iteratorToList(actualIterator);
            List<T> expectedOutput = iteratorToList(expectedIterator);
            assertThat(actualOutput).containsExactlyElementsOf(expectedOutput);
        }
    }

    private static @NotNull <T> List<T> iteratorToList(CloseableIterator<T> closeableIterator) {
        List<T> actualOutput = new ArrayList<>();
        while (closeableIterator.hasNext()) {
            actualOutput.add(closeableIterator.next());
        }
        return actualOutput;
    }

}
