# Build stage
# Build stage
FROM maven:3-eclipse-temurin-21 as builder
WORKDIR /app
COPY . .
RUN mvn -o -DskipTests -fn package

# Runtime stage
FROM apache/flink:2.0.0-scala_2.12-java21
COPY --from=builder /app/target/basic-stream-*.jar /opt/flink/usrlib/basic-stream.jar

ENV FLINK_JOBMANAGER_URL=http://jobmanager:8081
USER flink
CMD ["flink", "run", "--jobmanager", "$FLINK_JOBMANAGER_URL", "/opt/flink/usrlib/basic-stream.jar"]

