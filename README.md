***

# Profiling Apache Flink: ADL Grammar Mapping and State Cost Analysis

> **Context:** This benchmark profiles the core building blocks of our Architecture Description Language (ADL). We evaluate the computational cost ($f_{in}$) and memory overhead (GC Time) of mapping high-level operators to a physical Apache Flink engine.

## 1. Infrastructure & Automated Pipeline
The testing environment is a fully containerized, API-driven testbed.
*   **Engine:** Apache Flink 1.18.1 (1 JobManager, 1 TaskManager with 4 slots).
*   **Data Backbone:** Apache Kafka (KRaft mode) with 4 partitions per topic.
*   **State Representation:** PostgreSQL 15 used as the physical implementation for the **STATE** nodes (Anagraphic registries and persistent metrics).
*   **Monitoring:** Prometheus aggressively scraping Flink TaskManagers every 1s.

## 2. Methodology: ADL Pattern Mapping
We transitioned from measuring operator performance to mapping the four fundamental data flows defined in our research grammar:

1.  **DATA → DATA (Stateless):** Pure transformation without memory retention.
2.  **DATA → STATE (Stateful Write):** Continuous update of external materialized views.
3.  **STATE → DATA (Stateful Read):** Stream enrichment via external lookups.
4.  **DATA + DATA → DATA (Heavy Stateful):** Bilateral stream joining with temporal windows.

---

## 3. Benchmark Results: Mapping the Grammar

### Summary Table
| ADL Flow | Action | In Throughput ($f_{in}$) | Out Throughput ($f_{out}$) | GC Time (ms) |
| :--- | :--- | :--- | :--- | :--- |
| **DATA → DATA** | Filter | 31,778.31 r/s | 25,944.55 r/s | 53 ms |
| **DATA → STATE** | Aggregate | 19,774.88 r/s | 19,057.73 r/s | 139 ms |
| **STATE → DATA** | Enrichment | 21,569.94 r/s | 21,570.15 r/s | 321 ms |
| **DATA + DATA** | Join | 30,915.74 r/s | 10,338.28 r/s | 3,960 ms |

---

### Matrix A: Stateless Baseline (Filter)
*   **Pattern:** DATA → DATA
*   **Observation:** This flow represents the engine's maximum physical capacity. With a selectivity of ~80%, the output rate closely follows the ingestion rate.
*   **Finding:** GC overhead is negligible (53ms), confirming that stateless operations do not pressure the JVM Heap.

### Matrix B: Stateful Writing (Aggregation)
*   **Pattern:** DATA → STATE
*   **Observation:** Flink performs continuous "Upserts" into PostgreSQL.
*   **Finding:** We observe a ~38% drop in ingestion throughput compared to the stateless baseline. This quantifies the I/O friction of maintaining synchronized state in an external RDBMS.

### Matrix C: Stateful Reading (Enrichment)
*   **Pattern:** STATE → DATA
*   **Observation:** The engine performs a `Lookup Join` against a PostgreSQL registry.
*   **Finding:** Throughput remains higher than the "Write" pattern (~21.5k r/s). Flink's internal lookup cache successfully mitigates the latency of external synchronous calls.

### Matrix D: Heavy State Penalty (Interval Join)
*   **Pattern:** DATA + DATA → DATA
*   **Observation:** Joining two infinite streams within a ±5-minute window.
*   **Critical Finding:** While ingestion ($f_{in}$) is high, the output ($f_{out}$) drops to ~10k r/s.
*   **Memory Pressure:** The **GC Time explodes to 3,960 ms**.
*   **Takeaway:** This flow is the most resource-intensive. The engine must buffer thousands of events in the JVM Heap to find matches. The massive GC time proves that "Heavy Stateful" operators are the primary bottleneck for scalability in ADL design.

---

## 4. Architectural Conclusions for the Optimizer
*   **Cost of State:** Writing to a State node is ~1.5x more expensive than reading from it in terms of throughput degradation.
*   **The Join Gap:** The significant delta between $f_{in}$ and $f_{out}$ in Joins is a result of **Backpressure**. The engine slows down emission because the bilateral state lookups are computationally expensive.
*   **Parametric Recommendation:** For high-volume scenarios, the ADL optimizer should prioritize "Lookup Joins" (Enrichment) over "Stream-to-Stream Joins" whenever semantic requirements allow, to avoid the 70x increase in GC overhead.

***