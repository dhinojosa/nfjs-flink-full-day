package com.evolutionnext;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

public class FlinkWindowOperatorsTest {

    @Test
    public void testWindowedAggregation() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        WatermarkStrategy<StockTrade> watermarkStrategy = WatermarkStrategy
            .<StockTrade>forMonotonousTimestamps()
            .withTimestampAssigner((trade, timestamp) -> trade.getTimestamp());

        List<StockTrade> inputTrades = List.of(   // Bucket            _
            new StockTrade("AAPL", 100, 1000L),   // Event Time = 1s
            new StockTrade("AAPL", 200, 2000L),   // Event Time = 2s    AAPL = 700
            new StockTrade("GOOGL", 300, 3000L),  // Event Time = 3s    GOOGL = 300
            new StockTrade("AAPL", 400, 4000L),   // Event Time = 4s   _
            new StockTrade("AAPL", 400, 5000L),   // Event Time = 5s
            new StockTrade("GOOGL", 400, 6000L),  // Event Time = 6s   AAPL = 800
            new StockTrade("AAPL", 400, 7000L),   // Event Time = 7s   GOOGL = 1300
            new StockTrade("GOOGL", 900, 8000L)   // Event Time = 8s
        );

        DataStream<StockTrade> tradeStream = env.fromData(inputTrades)
            .assignTimestampsAndWatermarks(watermarkStrategy);

        DataStream<StockTradeAggregate> aggregatedStream = tradeStream
            .keyBy(StockTrade::getSymbol)
            .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
            .aggregate(new TradeAggregator());

        List<StockTradeAggregate> results = new ArrayList<>();
        try (var iterator = aggregatedStream.executeAndCollect()) {
            iterator.forEachRemaining(results::add);
        }

        System.out.println(results);

        assertThat(results).containsExactly(
            new StockTradeAggregate("AAPL", 700),
            new StockTradeAggregate("GOOGL", 300),
            new StockTradeAggregate("AAPL", 800),
            new StockTradeAggregate("GOOGL", 1300)
        );
    }

    static class StockTrade {
        private final String symbol;
        private final int quantity;
        private final long timestamp;

        public StockTrade(String symbol, int quantity, long timestamp) {
            this.symbol = symbol;
            this.quantity = quantity;
            this.timestamp = timestamp;
        }

        public String getSymbol() { return symbol; }
        public int getQuantity() { return quantity; }
        public long getTimestamp() { return timestamp; }
    }

    static class StockTradeAggregate {
        private final String symbol;
        private final int totalQuantity;

        public StockTradeAggregate(String symbol, int totalQuantity) {
            this.symbol = symbol;
            this.totalQuantity = totalQuantity;
        }

        public String getSymbol() { return symbol; }
        public int getTotalQuantity() { return totalQuantity; }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof StockTradeAggregate)) return false;
            StockTradeAggregate other = (StockTradeAggregate) obj;
            return this.symbol.equals(other.symbol) && this.totalQuantity == other.totalQuantity;
        }

        @Override
        public int hashCode() { return symbol.hashCode() + totalQuantity; }

        @Override
        public String toString() { return "StockTradeAggregate{" + "symbol='" + symbol + "', totalQuantity=" + totalQuantity + '}'; }
    }

    static class TradeAggregator implements AggregateFunction<StockTrade, Tuple2<String, Integer>, StockTradeAggregate> {

        @Override
        public Tuple2<String, Integer> createAccumulator() {
            return new Tuple2<>("", 0);
        }

        @Override
        public Tuple2<String, Integer> add(StockTrade trade, Tuple2<String, Integer> accumulator) {
            return new Tuple2<>(trade.getSymbol(), accumulator.f1 + trade.getQuantity());
        }

        @Override
        public StockTradeAggregate getResult(Tuple2<String, Integer> accumulator) {
            return new StockTradeAggregate(accumulator.f0, accumulator.f1);
        }

        @Override
        public Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
            return new Tuple2<>(a.f0, a.f1 + b.f1);
        }
    }
}
