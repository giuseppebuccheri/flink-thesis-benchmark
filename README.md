# Apache Flink Benchmark & Monitoring Infrastructure

This repository contains the containerized infrastructure and Python scripts used to test and monitor the performance of Apache Flink (Stream Processing). This project is part of a master's thesis focused on the comparative analysis of Big Data processing frameworks.

## 🏗️ System Architecture
The environment is entirely Docker-based and isolated within a dedicated network (`flink-net`). It includes the following services:
* **Apache Flink (v1.18+):** A cluster consisting of 1 JobManager and 1 TaskManager. The official Docker image was extended to include `python3` and the `apache-flink` library (PyFlink).
* **JMX Prometheus Exporter:** A Java agent injected directly into the Flink JVM to export highly reliable metrics, including Garbage Collection overhead and Throughput.
* **Prometheus:** Configured to scrape Flink metrics via port `7070`.
* **Grafana:** Provides real-time dashboard visualization for system performance profiling.

## 🚀 Prerequisites
* **Docker & Docker Compose** installed on your host machine (e.g., Docker Desktop for Windows/Mac or Docker Engine for Linux).
* **Dataset:** The CSV files required for the benchmark are not tracked in this Git repository due to size constraints. Place your CSV data in the `./data` directory.

## ⚙️ Setup & Quick Start

**1. Clone the repository and prepare the data**
```bash
git clone <your-repo-url>
cd flink-thesis-benchmark
```

**2. Start the infrastructure**
Since a custom `Dockerfile` is used to support PyFlink, the initial startup requires building the image:
```bash
docker compose up -d --build
```

**3. Access the Web Interfaces**
Once the containers are in the **Up** state, you can access the following services:
* **Grafana:** [http://localhost:3000](http://localhost:3000) (Credentials: `admin` / `admin`)
* **Prometheus:** [http://localhost:9090](http://localhost:9090)
* **Flink Web UI:** [http://localhost:8081](http://localhost:8081)

## 🏃‍♂️ Running the Benchmarks

The core benchmark script is written in PyFlink (`/jobs/flink_benchmark.py`) and tests three distinct scenarios: *Low Selectivity Filter*, *High Selectivity Point Lookup*, and *Stateful Aggregation*.

Methodology Note (State vs. Data): Unlike database benchmarks that require thousands of looped queries to measure state retrieval latency, Flink is tested as a continuous stream processing engine. The script is executed once, ingesting and processing the entire 300,000-row dataset in a single pass to accurately measure sustained throughput (Op rate).
```bash
docker exec -it flink-jobmanager flink run -py /jobs/flink_benchmark.py
```

## 📊 Collecting Metrics on Grafana
While the script is running, monitor Grafana to extract the JVM Overhead times:
* **Target Metric for GC Time:** `jvm_gc_collection_seconds_sum`
* **Calculation Method:** Measure the delta (**Final Value - Initial Value**) of the "step" generated during the execution of a specific query on the `G1 Young Generation` line of the `flink-taskmanager`.

## 🧹 Teardown

To stop the infrastructure and remove the isolated network:
```bash
docker compose down
```

If you want to perform a deep clean by destroying the anonymous volumes as well:
```bash
docker compose down -v --remove-orphans
```

---
