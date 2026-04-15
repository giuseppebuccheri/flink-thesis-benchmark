from pyflink.table import EnvironmentSettings, TableEnvironment

def run_benchmark():
    print("Initializing Flink Environment in STREAMING mode (Kafka)...")
    
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    # Abilita il tracking della latenza per Prometheus
    t_env.get_config().set("pipeline.latency-tracking.interval", "1000")

    # FORZA IL CARICAMENTO DEL CONNETTORE KAFKA
    t_env.get_config().set("pipeline.jars", "file:///opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar")
    
    # 1. DEFINE SOURCE TABLE (Kafka Stream)
    # Leggiamo dal topic 'sensor_events' generato dal nostro script Python
    t_env.execute_sql("""
        CREATE TABLE events_by_user (
            user_id BIGINT,
            event_id BIGINT,
            ts STRING,
            event_type STRING,
            amount DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'sensor_events',
            'properties.bootstrap.servers' = 'kafka:29092',
            'properties.group.id' = 'benchmark-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """)

    # 2. DEFINE SINKS (Blackhole)
    t_env.execute_sql("""
        CREATE TABLE benchmark_sink (
            user_id BIGINT, event_id BIGINT, ts STRING, event_type STRING, amount DOUBLE
        ) WITH ('connector' = 'blackhole')
    """)
    
    t_env.execute_sql("""
        CREATE TABLE agg_sink (
            user_id BIGINT, n_events BIGINT
        ) WITH ('connector' = 'blackhole')
    """)

    # ==========================================
    # BENCHMARK EXECUTION (UNBOUNDED)
    # ==========================================
    # IMPORTANTE: Avviamo UNA SOLA query alla volta per evitare che 
    # si rubino risorse a vicenda sul singolo TaskManager.

    # --- QUERY 1 ---
    #print("\nStarting Query 1: Filter...")
    #job1 = t_env.execute_sql("INSERT INTO benchmark_sink SELECT * FROM events_by_user WHERE user_id = 12345")
    #print(f"Job submitted! ID: {job1.get_job_client().get_job_id()}")

    # --- QUERY 2 (Decommenta per testarla) ---
    #print("\nStarting Query 2: Point Lookup...")
    #job2 = t_env.execute_sql("INSERT INTO benchmark_sink SELECT * FROM events_by_user WHERE user_id = 12345 AND event_id = 987")
    #print(f"Job submitted! ID: {job2.get_job_client().get_job_id()}")

    # --- QUERY 3 (Decommenta per testarla) ---
    print("\nStarting Query 3: State-centric Aggregation...")
    job3 = t_env.execute_sql("INSERT INTO agg_sink SELECT user_id, COUNT(*) AS n_events FROM events_by_user GROUP BY user_id")
    print(f"Job submitted! ID: {job3.get_job_client().get_job_id()}")

    print("\nIl Job è in esecuzione nel cluster! Flink resterà in ascolto su Kafka.")
    print("Vai su Grafana/Prometheus per leggere il Throughput in tempo reale.")

if __name__ == '__main__':
    run_benchmark()