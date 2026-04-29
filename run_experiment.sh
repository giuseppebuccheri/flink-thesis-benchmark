#!/bin/bash

if [ "$#" -lt 1 ]; then
    echo "Uso: $0 <numero_query> [eventi] [utenti] [selectivity] [join_rate]"
    exit 1
fi

QUERY=$1
EVENTS=${2:-1000000}     # Fissato a 1 Milione per dare a Prometheus 60 secondi di vita
USERS=${3:-10000}
SELECTIVITY=${4:-0.05}
JOIN_RATE=${5:-0.0}

init_infrastructure() {
    echo "🔍 Controllo stato infrastruttura Docker..."
    if ! docker compose ps | grep -q "kafka"; then
        docker compose up -d
        sleep 30
    fi
}

run_single_query() {
    echo "=================================================================="
    echo " 🚀 AVVIO BENCHMARK - QUERY $QUERY | Ev: $EVENTS | Usr: $USERS | Sel: $SELECTIVITY | Join: $JOIN_RATE"
    echo "=================================================================="

    echo "[1/6] 🗑️  Pulizia e Re-inizializzazione dei Topic Kafka..."
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic sensor_events --if-exists
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic purchases --if-exists
    sleep 3
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic sensor_events --partitions 4 --if-not-exists
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic purchases --partitions 4 --if-not-exists

    echo "[2/6] 🧹 Hard Reset di Flink e Prometheus..."
    docker compose restart flink-jobmanager flink-taskmanager prometheus
    sleep 25

    echo "[3/6] ⚙️ Sottomissione del Job PyFlink..."
    docker exec -d flink-jobmanager flink run -py /jobs/flink_benchmark.py $QUERY

    echo "    ⏳ Attendo che il job Flink sia in stato RUNNING..."
    for i in $(seq 1 60); do
        STATUS=$(curl -s http://localhost:8081/jobs/overview | python3 -c \
            "import sys,json; jobs=json.load(sys.stdin).get('jobs',[]); \
            print(jobs[0]['state'] if jobs else 'NONE')" 2>/dev/null)
        echo "    [${i}s] Job status: $STATUS"
        if [ "$STATUS" = "RUNNING" ]; then
            echo "    ✅ Job RUNNING! Aspetto 5s per stabilizzazione..."
            sleep 5
            break
        fi
        sleep 1
    done

    echo "[4/6] 🌊 Avvio Ingestione Dati CONTINUA (Python Kafka Producer)..."
    # Python terrà Flink impegnato per circa 60 secondi
    python3 scripts/kafka_generator.py --events $EVENTS --users $USERS --selectivity $SELECTIVITY --join-rate $JOIN_RATE

    echo "[5/6] ⏱️ Attesa finale e Scraping di Prometheus..."
    sleep 25

    echo "[6/6] 📊 Estrazione Automatica e Salvataggio Metriche..."
    python3 scripts/extract_metrics.py $QUERY
    
    echo " ✅ RUN COMPLETATA!"
    echo ""
}

init_infrastructure
run_single_query $QUERY