# =============================================================================
# flink/Dockerfile
# Image Flink 1.18 custom pour TaaSim
# Ajoute les JARs manquants dans flink:1.18-scala_2.12 :
#   - hadoop-aws          → S3A pour checkpoints vers MinIO
#   - aws-java-sdk-bundle → requis par hadoop-aws
#   - flink-connector-cassandra → sink vers table vehicle_positions
# =============================================================================

FROM flink:1.18-scala_2.12

ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.367
ENV CASSANDRA_CONNECTOR_VERSION=3.2.0-1.18

# Flink charge automatiquement tout JAR placé dans /opt/flink/lib/
RUN wget -P /opt/flink/lib/ \
    "https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
    "https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
    "https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-cassandra_2.12/${CASSANDRA_CONNECTOR_VERSION}/flink-connector-cassandra_2.12-${CASSANDRA_CONNECTOR_VERSION}.jar"

USER flink