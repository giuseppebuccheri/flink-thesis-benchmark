import csv
import json
import time
import os
import glob
from kafka import KafkaProducer

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME        = 'sensor_events'
CSV_DIR           = './data/events_3parts'

# TRUE STREAMING PRODUCER (REAL-WORLD CONFIG):
# - linger_ms=10   → aspetta fino a 10ms prima di inviare, per raggruppare i messaggi
# - batch_size     → raggruppa fino a 64KB di dati per scatolone
# - compression    → usa lz4 (standard industriale) per ridurre la banda
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=10,             
    batch_size=65536,         
    compression_type='lz4',   
    acks=1
)

print(f"[STREAMING MODE] Sending one event at a time to '{TOPIC_NAME}'...")

csv_files = sorted(glob.glob(os.path.join(CSV_DIR, '*.csv')))

if not csv_files:
    print(f"Error: No CSV files found in {CSV_DIR}")
    exit(1)

total_count = 0
start_time  = time.time()

for file_path in csv_files:
    print(f"Reading file: {file_path}")
    with open(file_path, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            row['amount'] = float(row['amount']) if row.get('amount') else 0.0

            # Un evento alla volta — questo è il cuore del true streaming
            producer.send(TOPIC_NAME, row)

            total_count += 1

            if total_count % 100_000 == 0:
                elapsed = time.time() - start_time
                rate    = total_count / elapsed
                print(f"  {total_count:,} events sent | avg rate: {rate:,.0f} msg/s")

producer.flush()
end_time = time.time()
elapsed  = end_time - start_time
print(f"\nDone: {total_count:,} events in {elapsed:.2f}s "
      f"({total_count/elapsed:,.0f} msg/s avg)")