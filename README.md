# Apache Flink Benchmark & Monitoring Infrastructure

This repository contains the containerized infrastructure and Python scripts used to test and monitor the performance of Apache Flink (Stream Processing). This project is part of a master's thesis focused on the comparative analysis of Big Data processing frameworks.

## 🏗️ System Architecture
The environment is entirely Docker-based and isolated within a dedicated network (`flink-net`). It includes the following services:
* **Apache Flink (v1.18+):** A cluster consisting of 1 JobManager and 1 TaskManager. The official Docker image was extended to include `python3` and the `apache-flink` library (PyFlink).
* **JMX Prometheus Exporter:** A Java agent injected directly into the Flink JVM to export highly reliable metrics, including Garbage Collection overhead and Throughput.
* **Prometheus:** Configured to scrape Flink metrics via port `7070`.
* **Grafana:** Provides real-time dashboard visualization for system performance profiling.

## 🚀 Prerequisites
* Docker and Docker Compose installed on your host machine (e.g., Docker Desktop for Windows/Mac or Docker Engine for Linux).
* **Dataset:** The CSV files required for the benchmark are not tracked in this Git repository due to size constraints.

## ⚙️ Setup & Quick Start

**1. Clone the repository and prepare the data**
```bash
git clone <your-repo-url>
cd flink-thesis-benchmark