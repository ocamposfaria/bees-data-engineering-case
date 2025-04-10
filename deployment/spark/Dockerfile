# Use a single stage build to avoid issues
FROM openjdk:11.0.11-jre-slim-buster

# Install essential packages
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Python setup
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    && ln -s /usr/bin/python3 /usr/bin/python

# Environment variables
ENV SPARK_VERSION=3.4.0 \
    HADOOP_VERSION=3 \
    SPARK_HOME=/opt/spark \
    PYTHONHASHSEED=1

# Download and install Spark
RUN wget --no-verbose -O apache-spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && mkdir -p /opt/spark \
    && tar -xf apache-spark.tgz -C /opt/spark --strip-components=1 \
    && rm apache-spark.tgz

# Download AWS SDK and Hadoop AWS jars directly into spark jars folder
RUN mkdir -p /opt/spark/jars \
    && cd /opt/spark/jars \
    && wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar

# Verify the downloads
RUN ls -la /opt/spark/jars/hadoop-aws-*.jar /opt/spark/jars/aws-java-sdk-*.jar

# Spark environment setup
ENV SPARK_MASTER_PORT=7077 \
    SPARK_MASTER_WEBUI_PORT=8080 \
    SPARK_LOG_DIR=/opt/spark/logs \
    SPARK_MASTER_LOG=/opt/spark/logs/spark-master.out \
    SPARK_WORKER_LOG=/opt/spark/logs/spark-worker.out \
    SPARK_WORKER_WEBUI_PORT=8080 \
    SPARK_WORKER_PORT=7000 \
    SPARK_MASTER="spark://spark-master:7077" \
    SPARK_WORKLOAD="master"

# Create logs directory
RUN mkdir -p ${SPARK_LOG_DIR} \
    && touch ${SPARK_MASTER_LOG} ${SPARK_WORKER_LOG} \
    && ln -sf /dev/stdout ${SPARK_MASTER_LOG} \
    && ln -sf /dev/stdout ${SPARK_WORKER_LOG}

# Copy start script
COPY start-spark.sh /

# Final verification
RUN echo "Verificando JARs da AWS:" \
    && ls -la /opt/spark/jars/*aws* \
    && echo "Conteúdo da pasta jars:" \
    && ls -la /opt/spark/jars/

EXPOSE 8080 7077 6066

CMD ["/bin/bash", "/start-spark.sh"]