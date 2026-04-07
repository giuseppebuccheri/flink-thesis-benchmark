FROM flink:latest

USER root

# Installiamo solo Python e le dipendenze per PyFlink
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip3 install apache-flink --break-system-packages

USER flink