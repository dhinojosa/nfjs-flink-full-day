mvn package
~/Downloads/flink-1.20.0/bin/flink run -m localhost:8081 target/basic-stream-1.0.jar

# flink run -m localhost:8081 -c com.evolutionnext.HighEndOrdersFlinkStream target/basic-stream-1.0.jar
