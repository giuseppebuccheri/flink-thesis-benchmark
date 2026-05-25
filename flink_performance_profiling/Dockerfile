# 1. Base Flink 1.18.1
FROM flink:1.18.1

USER root

# 2. Installazione PyFlink
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip3 install apache-flink==1.18.1

# 3. Download Connettori e Driver
# -> Connettore Kafka (Per input e output)
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar

# -> Connettore Cassandra Nativo (Per Sink)
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-cassandra_2.12/3.2.0-1.18/flink-connector-cassandra_2.12-3.2.0-1.18.jar

# -> Connettore JDBC
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar

# -> Driver JDBC per Postgres
RUN wget -P /opt/flink/lib/ https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

# -> Driver Nativo Datastax (Richiesto per il Sink su Cassandra)
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/com/datastax/cassandra/cassandra-driver-core/3.11.2/cassandra-driver-core-3.11.2.jar
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/com/google/guava/guava/19.0/guava-19.0.jar

# 4. Reporter Prometheus
RUN mkdir -p /opt/flink/plugins/prometheus && \
    wget -P /opt/flink/plugins/prometheus/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-metrics-prometheus/1.18.1/flink-metrics-prometheus-1.18.1.jar && \
    chown -R flink:flink /opt/flink/plugins/prometheus && \
    chmod 644 /opt/flink/plugins/prometheus/*.jar

# Sistemiamo i permessi di tutti i jar
RUN chmod 644 /opt/flink/lib/*.jar

USER flink