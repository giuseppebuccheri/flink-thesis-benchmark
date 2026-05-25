import json
import time
import argparse
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import uuid

BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_EVENTS      = 'sensor_events'
TOPIC_PURCHASES   = 'purchases'

def setup_topics(admin_client, topics):
    """Assicura che i topic esistano prima di iniziare a produrre"""
    existing_topics = admin_client.list_topics()
    topics_to_create = []
    for topic in topics:
        if topic not in existing_topics:
            topics_to_create.append(NewTopic(name=topic, num_partitions=4, replication_factor=1))
    
    if topics_to_create:
        try:
            admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
            print(f"Topic creati: {[t.name for t in topics_to_create]}")
        except Exception as e:
            print(f"Errore creazione topic: {e}")


# argparse per i parametri dinamici
parser = argparse.ArgumentParser(description="Generatore Parametrico Kafka per Flink (Dual-Topic)")
parser.add_argument('--events', type=int, default=300000, help='Volume totale di eventi (su topic 1)')
parser.add_argument('--users', type=int, default=1000, help='Cardinalità: Numero di user_id unici')
parser.add_argument('--target-user', type=int, default=12345, help='Target user_id per le query di Filter')
parser.add_argument('--selectivity', type=float, default=0.01, help='Percentuale (0.0 a 1.0) di eventi con target_user')
parser.add_argument('--join-rate', type=float, default=0.0, help='Percentuale di eventi che genera un "acquisto" correlato sul topic 2')
args = parser.parse_args()

# Inizializza Admin Client per creare i topic se necessario
admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
setup_topics(admin_client, [TOPIC_EVENTS, TOPIC_PURCHASES])
admin_client.close()

# Inizializza Kafka Producer con configurazione ottimizzata per l'alta velocità
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=10,             
    batch_size=65536,         
    compression_type='lz4',   
    acks=1
)

print(f"============================================================")
print(f"[STREAMING MODE] Generazione Dinamica Temperature IoT in corso...")
print(f"Volume Totale      : {args.events:,} letture (Topic: {TOPIC_EVENTS})")
print(f"Sensori Unici      : {args.users:,}")
print(f"============================================================\n")

start_time = time.time()
events_sent = 0

# Base time per simulare 1 giorno
current_time = datetime(2026, 5, 20, 8, 0, 0)

for i in range(args.events):
    # Genera una temperatura casuale tra 18.5 e 32.0
    temp = round(random.uniform(18.5, 32.0), 2)
    
    event = {
        "sensor_id": 1,
        "ts": current_time.strftime("%Y-%m-%d %H:%M:%S"),
        "temperature": temp
    }
    
    producer.send(TOPIC_EVENTS, event)
    events_sent += 1
    
    current_time += timedelta(seconds=1)

    # Logging
    if (i + 1) % 20_000 == 0:
        elapsed = time.time() - start_time
        rate = events_sent / elapsed
        print(f"  {events_sent:,} letture inviate | avg rate: {rate:,.0f} msg/s")

producer.flush()
elapsed = time.time() - start_time
print(f"\n✅ Fine: {events_sent:,} messaggi totali in {elapsed:.2f}s ({events_sent / elapsed:,.0f} msg/s avg)")