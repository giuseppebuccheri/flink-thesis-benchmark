# 1. Fissiamo la versione base di Flink alla 1.18.1
FROM flink:1.18.1

USER root

# 2. Fissiamo la versione di PyFlink esattamente alla 1.18.1
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip3 install apache-flink==1.18.1

# 3. Scarichiamo il connettore Kafka per la 1.18
ADD https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar /opt/flink/lib/
RUN chmod 644 /opt/flink/lib/flink-sql-connector-kafka*.jar

# 4. Scarichiamo il Reporter Prometheus per la 1.18
RUN mkdir -p /opt/flink/plugins/prometheus
ADD https://repo.maven.apache.org/maven2/org/apache/flink/flink-metrics-prometheus/1.18.1/flink-metrics-prometheus-1.18.1.jar /opt/flink/plugins/prometheus/
RUN chown -R flink:flink /opt/flink/plugins/prometheus && chmod 644 /opt/flink/plugins/prometheus/*.jar

USER flink