import sys
from pyflink.table import EnvironmentSettings, TableEnvironment

# ============================================================================
# TABLES DEFINITIONS
# ============================================================================

#Sources

def create_kafka_source(t_env):
    t_env.execute_sql("""
        CREATE TABLE events (
            sensor_id BIGINT,
            ts STRING,
            temperature DOUBLE,
            proctime AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'sensor_events',
            'properties.bootstrap.servers' = 'kafka:29092',
            'properties.group.id' = 'benchmark-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """)

def create_kafka_source_purchases(t_env):
    """Source Kafka per eventi di acquisto, usata nello Stream-to-Stream Join"""
    t_env.execute_sql("""
        CREATE TABLE purchases (
            user_id BIGINT,
            related_event_id BIGINT,
            purchase_id STRING,
            status STRING,
            proctime AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'purchases',
            'properties.bootstrap.servers' = 'kafka:29092',
            'properties.group.id' = 'benchmark-purchases-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """)

def create_postgres_lookup_source(t_env):
    """Source per arricchimento da Postgres, usata nell'Enrichment (Lookup Join)"""
    t_env.execute_sql("""
        CREATE TABLE user_registry (
            user_id BIGINT,
            discount_rate DOUBLE,
            tier STRING,
            PRIMARY KEY (user_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres-state:5432/benchmark_db',
            'table-name' = 'registry',
            'username' = 'flinkuser',
            'password' = 'flinkpassword',
            'lookup.cache.max-rows' = '10000',
            'lookup.cache.ttl' = '10 min'
        )
    """)

#Sinks

def create_kafka_sink(t_env, topic_name, schema):
    """Per stream Append-Only (Filter, Join, Enrichment)"""
    t_env.execute_sql(f"""
        CREATE TABLE kafka_sink (
            {schema}
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{topic_name}',
            'properties.bootstrap.servers' = 'kafka:29092',
            'format' = 'json'
        )
    """)

def create_upsert_kafka_sink(t_env, topic_name, schema):
    """Per stream con Aggiornamenti (Aggregate). Richiede una PRIMARY KEY."""
    t_env.execute_sql(f"""
        CREATE TABLE kafka_sink (
            {schema},
            PRIMARY KEY (user_id) NOT ENFORCED
        ) WITH (
            'connector' = 'upsert-kafka',
            'topic' = '{topic_name}',
            'properties.bootstrap.servers' = 'kafka:29092',
            'key.format' = 'json',
            'value.format' = 'json'
        )
    """)

def create_state_sink(t_env):
    """Sink per il test Aggregate su Postgres"""
    t_env.execute_sql("""
        CREATE TABLE database_sink (
            user_id BIGINT,
            n_events BIGINT,
            PRIMARY KEY (user_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres-state:5432/benchmark_db',
            'table-name' = 'user_metrics',
            'username' = 'flinkuser',
            'password' = 'flinkpassword'
        )
    """)

# ============================================================================
# ACTION IMPLEMENTATIONS
# ============================================================================

def run_filter(t_env, io_mode):
    """Filter (Stateless) - DATA-to-DATA (Temperature IoT)"""
    create_kafka_source(t_env)
    create_kafka_sink(t_env, 'sink_filter', 'sensor_id BIGINT, ts STRING, temperature DOUBLE')
    return "INSERT INTO kafka_sink SELECT sensor_id, ts, temperature FROM events WHERE temperature > 30.0"

def run_filter_stateful(t_env, io_mode):
    """Filter - DATA-to-STATE (Temperature IoT su Postgres)"""
    
    # Source Kafka
    t_env.execute_sql("""
        CREATE TABLE events_stateful (
            sensor_id BIGINT,
            ts STRING,
            temperature DOUBLE,
            proctime AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'sensor_events',
            'properties.bootstrap.servers' = 'kafka:29092',
            'properties.group.id' = 'benchmark-group-iot-state',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """)
    
    # Sink Postgres
    t_env.execute_sql("""
        CREATE TABLE extreme_heat_alerts (
            sensor_id BIGINT,
            ts STRING,
            temperature DOUBLE,
            PRIMARY KEY (sensor_id, ts) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres-state:5432/benchmark_db',
            'table-name' = 'extreme_heat_alerts',
            'username' = 'flinkuser',
            'password' = 'flinkpassword'
        )
    """)
    
    # query
    return "INSERT INTO extreme_heat_alerts SELECT sensor_id, ts, temperature FROM events_stateful WHERE temperature > 30.0"

def run_aggregate(t_env, io_mode):
    """Aggregate (Stateful) - Data-to-Data vs Data-to-State"""
    create_kafka_source(t_env)
    
    if io_mode == 'data_to_data':
        create_upsert_kafka_sink(t_env, 'sink_aggregate', 'user_id BIGINT, n_events BIGINT')
        return "INSERT INTO kafka_sink SELECT user_id, COUNT(*) AS n_events FROM events GROUP BY user_id"
        
    elif io_mode == 'data_to_state':
        create_state_sink(t_env)
        return "INSERT INTO database_sink SELECT user_id, COUNT(*) AS n_events FROM events GROUP BY user_id"

def run_join_stream(t_env, io_mode):
    """Stream-to-Stream Join (Heavy Stateful) - Solo Data-to-Data"""
    create_kafka_source(t_env)
    create_kafka_source_purchases(t_env)
    create_kafka_sink(t_env, 'sink_join', 'user_id BIGINT, amount DOUBLE, purchase_id STRING')
    
    return """
        INSERT INTO kafka_sink 
        SELECT e.user_id, e.amount, p.purchase_id 
        FROM events e
        INNER JOIN purchases p 
        ON e.event_id = p.related_event_id
        AND e.proctime BETWEEN p.proctime - INTERVAL '5' MINUTE AND p.proctime + INTERVAL '5' MINUTE
    """

def run_enrichment(t_env, io_mode):
    """Lookup Join (State-to-Data) - Arricchimento dello stream interrogando DB"""
    create_kafka_source(t_env)
    create_postgres_lookup_source(t_env)
    create_kafka_sink(t_env, 'sink_enrichment', 'user_id BIGINT, amount DOUBLE, tier STRING, discount_rate DOUBLE')
    
    return """
        INSERT INTO kafka_sink 
        SELECT e.user_id, e.amount, r.tier, r.discount_rate
        FROM events AS e
        JOIN user_registry FOR SYSTEM_TIME AS OF e.proctime AS r
        ON e.user_id = r.user_id
    """

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("\nErrore: Specifica azione e modalità I/O")
        sys.exit(1)
    
    action = sys.argv[1].lower()
    io_mode = sys.argv[2].lower()
    
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)
    t_env.get_config().set("pipeline.jars", "file:///opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar")
    
    # Mappa delle azioni alle rispettive funzioni
    action_map = {
        'filter': run_filter,
        'filter_stateful': run_filter_stateful,
        'aggregate': run_aggregate,
        'join_stream': run_join_stream,
        'enrichment': run_enrichment
    }
    
    if action not in action_map:
        print(f"Azione '{action}' non riconosciuta")
        sys.exit(1)
        
    query = action_map[action](t_env, io_mode)
    job = t_env.execute_sql(query)
    print(f"Job submitted, ID: {job.get_job_client().get_job_id()}")