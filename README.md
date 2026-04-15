
# Apache Flink & Kafka Stream Processing Benchmark

This repository contains the containerized infrastructure and Python scripts used to test and monitor the performance of Apache Flink in a pure **Stream Processing** environment. This project is part of a master's thesis focused on the comparative analysis of computational costs (Throughput and Garbage Collection overhead) across different Big Data processing architectures.

## 🏗️ System Architecture

The environment is entirely Docker-based, isolated within a dedicated network (`flink-net`), and relies on the `Data in Motion` paradigm. It includes the following services:

* **Apache Flink (v1.18.1):** A cluster consisting of 1 JobManager and 1 TaskManager. The official Docker image was extended to enforce Python 3.10 and the `apache-flink==1.18.1` library (PyFlink) to prevent dependency issues.
* **Apache Kafka (KRaft Mode):** Message broker acting as the streaming source (topic: `sensor_events`). Runs without Zookeeper.
* **Prometheus:** Configured to scrape Flink metrics via the Flink Native Prometheus Reporter on port `9249`. JMX is no longer used.
* **Grafana:** Provides real-time dashboard visualization for system performance profiling (Dashboards are auto-provisioned).
* **Kafka Data Generator (`kafka_generator.py`):** A host-machine Python script that acts as a true streaming producer (`acks=1`, `batch_size=1`, `linger_ms=0`).

## 🚀 Prerequisites

* Docker & Docker Compose installed on your host machine.
* Python 3 installed on your host machine with the `kafka-python` library (`pip install kafka-python`).
* **Dataset:** The CSV files required for the benchmark are not tracked in this Git repository due to size constraints. Place your CSV data in the `./data/events_3parts` directory.

## ⚙️ Setup & Quick Start

**1. Clone the repository and prepare the data**
```bash
git clone <your-repo-url>
cd flink-thesis-benchmark
```

**2. Start the infrastructure**
Since a custom Dockerfile is used to support PyFlink and specific connectors, the initial startup requires building the image:
```bash
docker compose up -d --build
```

**3. Access the Web Interfaces**
* **Grafana:** `http://localhost:3000` (Credentials: admin / admin)
* **Prometheus:** `http://localhost:9090`
* **Flink Web UI:** `http://localhost:8081`

## 🏃‍♂️ Benchmark Execution Protocols

The core benchmark script is written in PyFlink (`/jobs/flink_benchmark.py`) and tests three distinct scenarios using Flink SQL:
* **Q1:** Low Selectivity Filter
* **Q2:** High Selectivity Point Lookup
* **Q3:** Stateful Aggregation

<<<<<<< HEAD
> **⚠️ CRITICAL: Full Reset Between Runs**
> Flink JVM Garbage Collector counters are cumulative. To prevent GC values from Q1 polluting Q2 and Q3, you **must** reset the cluster before every single run:
> ```bash
> docker compose restart flink-jobmanager flink-taskmanager
> ```

To thoroughly evaluate the engine, this repository supports two different execution protocols:

### Protocol A: Live Streaming (Steady-State Throughput)
This test measures the engine's stability and GC overhead while keeping pace with a real-time data ingestion rate.

1. Ensure the Kafka topic exists: `docker exec kafka kafka-topics --create --topic sensor_events --bootstrap-server localhost:9092`
2. Uncomment the desired Query in `flink_benchmark.py` and save.
3. Restart the Flink cluster and wait 15 seconds.
4. Submit the Flink Job:
   ```bash
   docker exec -it flink-jobmanager flink run -py /jobs/flink_benchmark.py
   ```
5. Start the Kafka Producer to inject data:
   ```bash
   python3 scripts/kafka_generator.py
   ```
6. **Measurement:** Read the average throughput from Grafana during the "plateau" phase, and record the Total Execution Time from the python script's output.

### Protocol B: Maximum Throughput (Stress Test / Backlog Processing)
This test measures the absolute maximum processing power of Flink (Maximum Op/Rate) by forcing it to process a massive backlog of data without network ingestion bottlenecks.

1. Restart the Flink cluster. Ensure **no jobs** are running.
2. Fill the Kafka topic completely by running the generator *first*:
   ```bash
   python3 scripts/kafka_generator.py
   ```
3. Wait for the script to finish (100% of data is now *at rest* in Kafka).
4. Submit the Flink Job (`flink run ...`). Since the startup mode is set to `earliest-offset`, Flink will devour the 300,000 records at maximum CPU capacity.
5. **Measurement:** The execution time will drop to a few seconds, and the throughput will spike dramatically.

=======
Methodology Note (State vs. Data): Unlike database benchmarks that require thousands of looped queries to measure state retrieval latency, Flink is tested as a continuous stream processing engine. The script is executed once, ingesting and processing the entire 300,000-row dataset in a single pass to accurately measure sustained throughput (Op rate).
```bash
docker exec -it flink-jobmanager flink run -py /jobs/flink_benchmark.py
```

>>>>>>> 3cfde120dea7d6b5ecfe8813993c90a5bf2e69b4
## 📊 Collecting Metrics on Grafana

Monitor the auto-provisioned Grafana dashboards to extract the required computational costs:

* **Throughput (rec/s):** Evaluated using `sum(rate(flink_taskmanager_job_task_operator_numRecordsOut[1m]))`. Look at the peak or the steady-state mean depending on the protocol used.
* **Total Exec Time:** Use the generator script output for Protocol A, or measure the base of the throughput spike for Protocol B.
* **GC Total Time:** Calculate the delta (Final Value - Initial Value) from the Stat panel running `sum(flink_taskmanager_Status_JVM_GarbageCollector_G1_Young_Generation_Time)`.

## 🧹 Teardown

To stop the infrastructure and remove the isolated network:
```bash
docker compose down
```
To perform a deep clean (destroying all data and Kafka topics):
```bash
docker compose down -v --remove-orphans
<<<<<<< HEAD
=======
```

---
>>>>>>> 3cfde120dea7d6b5ecfe8813993c90a5bf2e69b4
