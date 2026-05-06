import psycopg2
import argparse
import random
import time

def populate_postgres(users_count):
    print(f"🔄 Connessione a Postgres... (Target: {users_count} utenti)")
    # Si connette a localhost mappato da Docker
    conn = psycopg2.connect(
        host="localhost", database="benchmark_db", user="flinkuser", password="flinkpassword"
    )
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS registry (
            user_id BIGINT PRIMARY KEY,
            discount_rate DOUBLE PRECISION,
            tier VARCHAR(50)
        )
    """)
    cur.execute("TRUNCATE TABLE registry;")
    
    tiers = ['Bronze', 'Silver', 'Gold', 'Platinum', 'Diamond']
    print("   🚀 Inserimento massivo in Postgres...")
    start_time = time.time()
    
    # Inserimento veloce tramite batch
    args_str = ','.join(cur.mogrify("(%s,%s,%s)", (i, round(random.uniform(0.0, 0.4), 2), random.choice(tiers))).decode('utf-8') for i in range(1, users_count + 1))
    cur.execute("INSERT INTO registry (user_id, discount_rate, tier) VALUES " + args_str)
    
    cur.execute("INSERT INTO registry (user_id, discount_rate, tier) VALUES (12345, 0.50, 'VIP') ON CONFLICT DO NOTHING;")
    
    conn.commit()
    cur.close()
    conn.close()
    print(f"   ✅ Popolamento Postgres completato in {time.time() - start_time:.2f} secondi!")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--users', type=int, default=10000)
    populate_postgres(parser.parse_args().users)