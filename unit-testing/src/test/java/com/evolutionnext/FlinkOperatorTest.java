package com.evolutionnext;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

public class FlinkOperatorTest {

    private OneInputStreamOperatorTestHarness<Long, Long> testHarness;

    @BeforeEach
    public void setup() throws Exception {
        StreamMap<Long, Long> mapOperator = new StreamMap<>(new SimpleIncrementMap());
        testHarness = new OneInputStreamOperatorTestHarness<>(mapOperator);
        testHarness.open();
    }

    @Test
    public void testHarnessTest() throws Exception {
        testHarness.processElement(1L, 100L);
        testHarness.processElement(2L, 200L);
        testHarness.processElement(3L, 300L);

        testHarness.processWatermark(1000L);

        assertThat(testHarness.extractOutputValues()).containsExactly(2L, 3L, 4L);
    }

    private static class SimpleIncrementMap implements MapFunction<Long, Long> {
        @Override
        public Long map(Long value) {
            return value + 1;
        }
    }
}
