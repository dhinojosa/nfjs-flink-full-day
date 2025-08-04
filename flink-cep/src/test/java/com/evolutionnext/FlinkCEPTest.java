package com.evolutionnext;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class FlinkCEPTest {
    public static class LoginEvent {
        public String userId;
        public String ip;
        public String status; // "FAIL" or "SUCCESS"
        public long timestamp;

        public LoginEvent(String userId, String ip, String status, long timestamp) {
            this.userId = userId;
            this.ip = ip;
            this.status = status;
            this.timestamp = timestamp;
        }
    }

    @Test
    public void testCEPPatternDetection() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<LoginEvent> loginStream = env
            .fromData(
                new LoginEvent("user1", "192.168.0.1", "FAIL", 1000L),
                new LoginEvent("user1", "192.168.0.1", "FAIL", 2000L),
                new LoginEvent("user1", "10.0.0.1", "SUCCESS", 3000L)
            )
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<LoginEvent>forMonotonousTimestamps()
                    .withTimestampAssigner((event, timestamp) -> event.timestamp)
            );

        Pattern<LoginEvent, ?> suspiciousLoginPattern = Pattern.<LoginEvent>begin("firstFail")
            .where(new IterativeCondition<>() {
                @Override
                public boolean filter(LoginEvent value, Context<LoginEvent> ctx) throws Exception {
                    return value.status.equals("FAIL");
                }
            })
            .next("secondFail")
            .where(new IterativeCondition<>() {
                @Override
                public boolean filter(LoginEvent value, Context<LoginEvent> ctx) throws Exception {
                    return value.status.equals("FAIL");
                }
            })
            .next("success")
            .where(new IterativeCondition<>() {
                @Override
                public boolean filter(LoginEvent value, Context<LoginEvent> ctx) throws Exception {
                    return value.status.equals("SUCCESS");
                }
            })
            .within(Duration.of(10, ChronoUnit.MINUTES));

        PatternStream<LoginEvent> patternStream = CEP.pattern(
            loginStream.keyBy(e -> e.userId), // CEP runs per user
            suspiciousLoginPattern
        );

        patternStream.select((PatternSelectFunction<LoginEvent, String>) pattern -> {
            LoginEvent fail1 = pattern.get("firstFail").get(0);
            LoginEvent fail2 = pattern.get("secondFail").get(0);
            LoginEvent success = pattern.get("success").get(0);

            boolean ipChanged = !fail2.ip.equals(success.ip);
            if (ipChanged) {
                return "Suspicious login detected for " + success.userId +
                       " from new IP " + success.ip;
            } else {
                return "Regular login sequence.";
            }
        }).print();

        env.execute("Suspicious Login Detection");
    }
}
