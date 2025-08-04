package com.evolutionnext;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class FlinkStatefulOperatorsTest extends AbstractTestBase {
    @Test
    public void testStatefulAggregation() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<StockTrade> inputTrades = List.of(
            new StockTrade("AAPL", 100),
            new StockTrade("AAPL", 200),
            new StockTrade("GOOGL", 300),
            new StockTrade("AAPL", 400)
        );

        DataStream<StockTrade> tradeStream = env.fromData(inputTrades);

        DataStream<StockTradeAggregate> aggregatedStream = tradeStream
            .keyBy(StockTrade::getSymbol)
            .process(new StatefulStockAggregator());

        List<StockTradeAggregate> results = new ArrayList<>();
        try (var iterator = aggregatedStream.executeAndCollect()) {
            iterator.forEachRemaining(results::add);
        }

        assertThat(results).containsExactly(
            new StockTradeAggregate("AAPL", 100),
            new StockTradeAggregate("AAPL", 300),
            new StockTradeAggregate("GOOGL", 300),
            new StockTradeAggregate("AAPL", 700)
        );
    }

    static class StockTrade {
        private final String symbol;
        private final int quantity;

        public StockTrade(String symbol, int quantity) {
            this.symbol = symbol;
            this.quantity = quantity;
        }

        public String getSymbol() { return symbol; }
        public int getQuantity() { return quantity; }
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

    static class StatefulStockAggregator extends KeyedProcessFunction<String, StockTrade, StockTradeAggregate> {
        private transient ValueState<Integer> sumState;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            sumState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("stock-sum", Types.INT)
            );
        }

        @Override
        public void processElement(StockTrade trade, Context ctx, Collector<StockTradeAggregate> out) throws Exception {
            Integer currentTotal = sumState.value();
            if (currentTotal == null) currentTotal = 0;
            currentTotal += trade.getQuantity();
            sumState.update(currentTotal);

            System.out.println("State for stock " + trade.getSymbol() + " → " + currentTotal);

            out.collect(new StockTradeAggregate(trade.getSymbol(), currentTotal));
        }
    }
}
