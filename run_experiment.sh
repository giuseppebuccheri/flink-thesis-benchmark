#!/bin/bash

if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <action> <io_mode> [events] [users] [extra_param]"
    exit 1
fi

ACTION=$1
IO_MODE=$2
EVENTS=${3:-500000}
USERS=${4:-10000}
PARAM=${5:-0.05}

# Configurazioni globali
KAFKA_BROKER="localhost:9092"
TOPICS=("sensor_events" "purchases" "sink_filter" "sink_aggregate" "sink_join" "sink_enrichment")

log() {
    echo "[*] $1"
}

init_infrastructure() {
    log "Checking Docker infrastructure..."
    if ! docker compose ps | grep -q "kafka"; then
        log "Starting Docker Compose (waiting 45s)..."
        docker compose up -d
        sleep 45
    fi
}

setup_databases() {
    log "Waiting for databases to be ready..."
    sleep 5 # Piccolo buffer

    log "Setting up Postgres schema..."
    # Creiamo la tabella per l'Aggregate in Postgres e la svuotiamo se esiste già
    docker exec postgres-state psql -U flinkuser -d benchmark_db -c "
        CREATE TABLE IF NOT EXISTS user_metrics (user_id BIGINT PRIMARY KEY, n_events BIGINT);
        TRUNCATE TABLE user_metrics;
    " >/dev/null 2>&1
    
    # Mantengo anche lo schema di Cassandra se vuoi tenerlo acceso
    docker exec cassandra-state cqlsh -e "
        CREATE KEYSPACE IF NOT EXISTS benchmark_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
        USE benchmark_ks;
        CREATE TABLE IF NOT EXISTS user_metrics (user_id bigint PRIMARY KEY, n_events bigint);
        CREATE TABLE IF NOT EXISTS registry (user_id bigint PRIMARY KEY, discount_rate double, tier text);
    " >/dev/null 2>&1 || true
}

cleanup_state() {
    log "Cleaning Kafka consumer groups and topics..."
    docker exec kafka kafka-consumer-groups --bootstrap-server $KAFKA_BROKER --delete --group benchmark-group >/dev/null 2>&1 || true
    
    for topic in "${TOPICS[@]}"; do
        docker exec kafka kafka-topics --bootstrap-server $KAFKA_BROKER --delete --topic $topic --if-exists >/dev/null 2>&1
    done
    sleep 5
    
    for topic in "${TOPICS[@]}"; do
        docker exec kafka kafka-topics --bootstrap-server $KAFKA_BROKER --create --topic $topic --partitions 4 --if-not-exists >/dev/null 2>&1
    done
    sleep 3
}

reset_flink_prometheus() {
    log "Restarting Flink and Prometheus (waiting 25s)..."
    docker compose restart flink-jobmanager flink-taskmanager prometheus >/dev/null 2>&1
    sleep 25
}

prepare_data() {
    if [[ "$ACTION" == "enrichment" ]]; then
        log "Populating Postgres (State) with $USERS users..."
        python3 scripts/state_populator.py --users $USERS
    fi

    log "Populating Kafka (Data) with $EVENTS events..."
    python3 scripts/kafka_generator.py --events $EVENTS --users $USERS --selectivity $PARAM --join-rate $PARAM
    sleep 5
}

submit_flink_job() {
    log "Submitting Flink job..."
    docker exec -d flink-jobmanager flink run -py /jobs/actions_benchmark.py $ACTION $IO_MODE

    log "Waiting for job to reach RUNNING state..."
    for _ in {1..60}; do
        STATUS=$(curl -s http://localhost:8081/jobs/overview 2>/dev/null | python3 -c \
            "import sys,json; jobs=json.load(sys.stdin).get('jobs',[]); print(jobs[0]['state'] if jobs else 'NONE')" 2>/dev/null || echo "NONE")
        
        if [ "$STATUS" = "RUNNING" ]; then
            log "Job RUNNING. Waiting 10s for warm-up..."
            sleep 10
            return 0
        fi
        sleep 1
    done
    return 1
}

wait_for_processing() {
    log "Processing... sleeping for 60s."
    sleep 60
}

extract_metrics() {
    log "Extracting metrics from Prometheus..."
    python3 scripts/extract_metrics.py $ACTION $IO_MODE
}

cancel_flink_job() {
    log "Cancelling Flink job..."
    JOB_ID=$(curl -s http://localhost:8081/jobs/overview 2>/dev/null | python3 -c \
        "import sys,json; jobs=json.load(sys.stdin).get('jobs',[]); print(jobs[0]['jid'] if jobs else '')" 2>/dev/null || echo "")
    
    if [ -n "$JOB_ID" ]; then
        curl -s -X PATCH "http://localhost:8081/jobs/${JOB_ID}?mode=cancel" >/dev/null
    fi
    sleep 5
}

main() {
    echo "--- STARTING BENCHMARK: $ACTION | $IO_MODE ---"
    init_infrastructure
    setup_databases
    cleanup_state
    reset_flink_prometheus
    prepare_data
    submit_flink_job
    wait_for_processing
    extract_metrics
    cancel_flink_job
    echo "--- RUN COMPLETED ---"
}

main