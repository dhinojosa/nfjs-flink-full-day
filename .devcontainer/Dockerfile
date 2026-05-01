FROM mcr.microsoft.com/devcontainers/base:ubuntu

ARG TARGETOS
ARG TARGETARCH
ARG TARGETVARIANT

RUN echo "Building for OS=$TARGETOS ARCH=$TARGETARCH VARIANT=$TARGETVARIANT"

# Install required tools
RUN apt-get update && \
    apt-get install -y curl zip unzip tar && \
    rm -rf /var/lib/apt/lists/*

RUN if [ "$TARGETOS" != "linux" ]; then \
        echo "Unsupported OS: $TARGETOS"; \
        exit 1; \
    elif [ "$TARGETARCH" = "amd64" ]; then \
        JDK_URL="https://download.java.net/java/GA/jdk21.0.2/f2283984656d49d69e91c558476027ac/13/GPL/openjdk-21.0.2_linux-x64_bin.tar.gz"; \
        JDK_FILE="openjdk-21.0.2_linux-x64_bin.tar.gz"; \
    elif [ "$TARGETARCH" = "arm64" ]; then \
        JDK_URL="https://download.java.net/java/GA/jdk21.0.2/f2283984656d49d69e91c558476027ac/13/GPL/openjdk-21.0.2_linux-aarch64_bin.tar.gz"; \
        JDK_FILE="openjdk-21.0.2_linux-aarch64_bin.tar.gz"; \
    else \
        echo "Unsupported architecture: $TARGETARCH"; \
        exit 1; \
    fi && \
    curl -fL -o "/tmp/$JDK_FILE" "$JDK_URL" && \
    tar -xzf "/tmp/$JDK_FILE" -C /usr/local/ && \
    rm "/tmp/$JDK_FILE" && \
    mv /usr/local/jdk-21.0.2 /usr/local/openjdk-21

ENV JAVA_HOME=/usr/local/openjdk-21
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install Maven
RUN MAVEN_VERSION=3.9.8 \
    && MAVEN_BASE_URL="https://archive.apache.org/dist/maven/maven-$(echo "$MAVEN_VERSION" | cut -d'.' -f1)" \
    && curl -fL -o /tmp/apache-maven-bin.tar.gz "${MAVEN_BASE_URL}/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz" \
    && tar -xzf /tmp/apache-maven-bin.tar.gz -C /usr/local/ \
    && rm /tmp/apache-maven-bin.tar.gz \
    && mv "/usr/local/apache-maven-${MAVEN_VERSION}" /usr/local/maven

ENV MAVEN_HOME=/usr/local/maven
ENV PATH="${MAVEN_HOME}/bin:${PATH}"

# Install Flink
RUN FLINK_VERSION=2.1.1 \
    && FLINK_SCALA_VERSION=2.12 \
    && curl -fL -o /tmp/flink-bin.tgz "https://dlcdn.apache.org/flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-scala_${FLINK_SCALA_VERSION}.tgz" \
    && tar -xzf /tmp/flink-bin.tgz -C /usr/local/ \
    && rm /tmp/flink-bin.tgz \
    && mv "/usr/local/flink-${FLINK_VERSION}" /usr/local/flink

ENV FLINK_HOME=/usr/local/flink
ENV PATH="${FLINK_HOME}/bin:${PATH}"

USER vscode
