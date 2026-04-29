import json
import time
import argparse
import random
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import uuid

# CONFIGURAZIONE
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
            print(f"✅ Topic creati: {[t.name for t in topics_to_create]}")
        except Exception as e:
            print(f"⚠️ Errore creazione topic: {e}")


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
print(f"[STREAMING MODE] Generazione Dinamica Eventi in corso...")
print(f"Volume Totale      : {args.events:,} eventi (Topic: {TOPIC_EVENTS})")
print(f"Cardinalità Utenti : {args.users:,} utenti unici")
print(f"Selectivity Filter : {args.selectivity * 100:.1f}% per user_id {args.target_user}")
if args.join_rate > 0:
    print(f"Join Correlation   : {args.join_rate * 100:.1f}% (Topic: {TOPIC_PURCHASES})")
print(f"============================================================\n")

start_time = time.time()
events_sent = 0
purchases_sent = 0

for i in range(args.events):
    # 1. Logica Selectivity
    if random.random() < args.selectivity:
        uid = args.target_user
    else:
        uid = random.randint(1, args.users)
        if uid == args.target_user:
            uid += 1
    
    # 2. Generazione Evento Principale
    event_id = i
    event = {
        "user_id": uid,
        "event_id": event_id,
        "ts": "2026-01-01 10:00:00",
        "event_type": random.choice(["click", "view", "share"]),
        "amount": round(random.uniform(1.0, 50.0), 2)
    }
    producer.send(TOPIC_EVENTS, event)
    events_sent += 1

    # 3. Logica Join / Correlazione
    if args.join_rate > 0 and random.random() < args.join_rate:
        purchase = {
            "user_id": uid, # Chiave di Join
            "related_event_id": event_id,
            "purchase_id": str(uuid.uuid4()), # Questo non va ancora a Flink, può restare UUID
            "status": "completed"
        }
        producer.send(TOPIC_PURCHASES, purchase)
        purchases_sent += 1

    # Logging
    if (i + 1) % 100_000 == 0:
        elapsed = time.time() - start_time
        rate = (events_sent + purchases_sent) / elapsed
        print(f"  {events_sent:,} eventi base | {purchases_sent:,} acquisti correlati | avg rate: {rate:,.0f} msg/s")

producer.flush()
elapsed = time.time() - start_time
total_sent = events_sent + purchases_sent
print(f"\n✅ Fine: {total_sent:,} messaggi totali in {elapsed:.2f}s ({total_sent / elapsed:,.0f} msg/s avg)")