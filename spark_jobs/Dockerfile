# syntax=docker/dockerfile:1

# Spark 3.5.3 (Scala 2.12, Java 17, Python 3) + the JARs needed to read/write
# Delta tables on MinIO over S3A. Versions are matched deliberately — this is the
# combination that trips people up:
#   Spark 3.5.x  <-> Delta 3.2.x            (Delta's compatibility matrix)
#   Spark 3.5.x   -> Hadoop 3.3.4 client    -> hadoop-aws 3.3.4 + aws-sdk-bundle
# JARs are baked into the image (not fetched via --packages at runtime) so job
# runs are reproducible and need no network.
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

ARG DELTA_VERSION=3.2.0
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.530
# Kafka connector for Structured Streaming (Day 10). Versions are the exact
# transitive set for Spark 3.5.3's spark-sql-kafka-0-10.
ARG SPARK_VERSION=3.5.3
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1
ARG MAVEN=https://repo1.maven.org/maven2

# curl may not be present in the base image; install it, fetch the jars, clean up.
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl; \
    rm -rf /var/lib/apt/lists/*; \
    cd /opt/spark/jars; \
    curl -fSL -O "${MAVEN}/io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar"; \
    curl -fSL -O "${MAVEN}/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar"; \
    curl -fSL -O "${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"; \
    curl -fSL -O "${MAVEN}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"; \
    curl -fSL -O "${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar"; \
    curl -fSL -O "${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar"; \
    curl -fSL -O "${MAVEN}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"; \
    curl -fSL -O "${MAVEN}/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar"

USER spark
