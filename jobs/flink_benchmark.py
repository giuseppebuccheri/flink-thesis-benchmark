import sys
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

    # 2. DEFINE SINK TABLES (print per debug / blackhole per test)
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
    # LETTURA ARGOMENTI DA RIGA DI COMANDO
    # ==========================================
    # Se non passo nessun numero, mi fermo con un errore
    if len(sys.argv) < 2:
        print("\n[ERRORE] Devi specificare quale query eseguire!")
        print("Esempio: flink run -py /jobs/flink_benchmark.py 1")
        sys.exit(1)
        
    query_choice = sys.argv[1]

    if query_choice == "1":
        print("\nStarting Query 1: Filter...")
        job = t_env.execute_sql("INSERT INTO benchmark_sink SELECT * FROM events_by_user WHERE user_id = 12345")
        print(f"Job submitted! ID: {job.get_job_client().get_job_id()}")
        
    elif query_choice == "2":
        print("\nStarting Query 2: Point Lookup...")
        job = t_env.execute_sql("INSERT INTO benchmark_sink SELECT * FROM events_by_user WHERE user_id = 12345 AND event_id = 987")
        print(f"Job submitted! ID: {job.get_job_client().get_job_id()}")
        
    elif query_choice == "3":
        print("\nStarting Query 3: State-centric Aggregation...")
        job = t_env.execute_sql("INSERT INTO agg_sink SELECT user_id, COUNT(*) AS n_events FROM events_by_user GROUP BY user_id")
        print(f"Job submitted! ID: {job.get_job_client().get_job_id()}")
        
    else:
        print(f"\n[ERRORE] Scelta '{query_choice}' non valida. Usa 1, 2 o 3.")
        sys.exit(1)

    print("\nIl Job è in esecuzione nel cluster! Flink resterà in ascolto su Kafka.")

if __name__ == '__main__':
    run_benchmark()