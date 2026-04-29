import sys
from pyflink.table import EnvironmentSettings, TableEnvironment

print("Initializing Flink Environment in STREAMING mode (Kafka)...")

env_settings = EnvironmentSettings.in_streaming_mode() # Configurazione per modalità streaming
t_env = TableEnvironment.create(env_settings)

t_env.get_config().set("pipeline.jars", "file:///opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar")
# ACCENSIONE LATENCY TRACKING (Marker ogni 2 secondi) - al momento non usato perchè il sink è un blackhole, ma lo lasciamo per future estensioni con sink reali
t_env.get_config().set("pipeline.latency-tracking.interval", "2000")

# 1. DEFINE SOURCE TABLE 1 (Events)
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

# 2. DEFINE SOURCE TABLE 2 (Purchases)
t_env.execute_sql("""
    CREATE TABLE purchases (
        user_id BIGINT,
        related_event_id BIGINT,
        purchase_id STRING,
        status STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'purchases',
        'properties.bootstrap.servers' = 'kafka:29092',
        'properties.group.id' = 'benchmark-group',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json'
    )
""")

# 3. SINK TABLES (Blackhole per evitare overhead di scrittura, ci interessano solo le metriche di throughput e GC)
t_env.execute_sql("CREATE TABLE benchmark_sink (user_id BIGINT, event_id BIGINT, ts STRING, event_type STRING, amount DOUBLE) WITH ('connector' = 'blackhole')")
t_env.execute_sql("CREATE TABLE agg_sink (user_id BIGINT, n_events BIGINT) WITH ('connector' = 'blackhole')")
t_env.execute_sql("CREATE TABLE join_sink (user_id BIGINT, event_type STRING, amount DOUBLE, purchase_id STRING) WITH ('connector' = 'blackhole')")

# --- ESECUZIONE DELLA QUERY SELEZIONATA ---
if len(sys.argv) < 2:
    print("\n[ERRORE] Devi specificare quale query eseguire!")
    sys.exit(1)
    
query_choice = sys.argv[1]

if query_choice == "1":
    print("\nStarting Query 1: Filter (Stateless)...")
    job = t_env.execute_sql("INSERT INTO benchmark_sink SELECT * FROM events_by_user WHERE user_id = 12345")
    
elif query_choice == "2":
    print("\nStarting Query 2: Point Lookup (Stateless)...")
    job = t_env.execute_sql("INSERT INTO benchmark_sink SELECT * FROM events_by_user WHERE user_id = 12345 AND event_id = 987")
    
elif query_choice == "3":
    print("\nStarting Query 3: Aggregation (Stateful)...")
    job = t_env.execute_sql("INSERT INTO agg_sink SELECT user_id, COUNT(*) AS n_events FROM events_by_user GROUP BY user_id")

elif query_choice == "4":
    print("\nStarting Query 4: Regular Join (Heavy Stateful)...")
    # Uniamo gli eventi e gli acquisti dello stesso utente
    # In flink_benchmark.py, sostituisci Q4 con:
    job = t_env.execute_sql("""
        INSERT INTO join_sink 
        SELECT e.user_id, e.event_type, e.amount, p.purchase_id 
        FROM events_by_user e
        INNER JOIN purchases p ON e.event_id = p.related_event_id
    """)
    
else:
    print(f"\n[ERRORE] Scelta non valida.")
    sys.exit(1)

print(f"Job submitted! ID: {job.get_job_client().get_job_id()}")
print("\nIl Job è in esecuzione nel cluster! Flink resterà in ascolto su Kafka.")
