import requests
import sys
import csv
import os

# --- CONFIGURAZIONE PROMETHEUS ---
PROMETHEUS_URL = "http://localhost:9090/api/v1/query"

# Funzione per eseguire query PromQL e restituire il risultato come float
def query_prometheus(promql_query):
    try:
        response = requests.get(PROMETHEUS_URL, params={'query': promql_query})
        response.raise_for_status()
        data = response.json()
        
        if data['status'] == 'success' and data['data']['result']:
            return float(data['data']['result'][0]['value'][1])
        return 0.0
    except Exception as e:
        print(f"⚠️ Errore API Prometheus: {e}")
        return 0.0

def collect_benchmark_results(query_id):
    print("--- RACCOLTA METRICHE AUTOMATIZZATA ---")
    
    # 1. Throughput cc: max_over_time su 2 minuti cattura il vero plateau di elaborazione del backlog
    # Versione robusta: somma TUTTI gli operatori Source (senza filtro su label)
    # 1. Throughput cc: max_over_time isolando SOLO l'operatore Source/Kafka
    throughput_query = '''max_over_time(
        sum(rate(flink_taskmanager_job_task_operator_numRecordsOut{operator_name=~"(?i).*source.*|(?i).*kafka.*"}[15s]))
        [3m:5s]
    )'''
    throughput = query_prometheus(throughput_query)
    
    # 2. GC Time Overhead: Somma dei delta calcolati in una finestra di 2 minuti
    #gc_query = 'sum(delta(flink_taskmanager_Status_JVM_GarbageCollector_G1_Young_Generation_Time[2m:5s]))'
    gc_query = '''sum(increase(flink_taskmanager_Status_JVM_GarbageCollector_G1_Young_Generation_Time[2m]))
            + sum(increase(flink_taskmanager_Status_JVM_GarbageCollector_G1_Old_Generation_Time[2m]))'''    
    gc_time = query_prometheus(gc_query)
    
    # Pulizia dai valori negativi accidentali (a volte dovuti al reset del contatore in PromQL)
    if gc_time < 0:
        gc_time = 0

    print(f"Max Total Input Throughput (cc): {throughput:,.2f} records/sec")
    print(f"GC Time Overhead: {gc_time:,.0f} ms")

    # --- SALVATAGGIO IN CSV ---
    csv_filename = "results.csv"
    file_exists = os.path.isfile(csv_filename)
    
    with open(csv_filename, mode='a', newline='') as file:
        writer = csv.writer(file)
        if not file_exists:
            writer.writerow(["Query_ID", "Throughput_cc", "GC_Time_ms"])
        writer.writerow([query_id, round(throughput, 2), round(gc_time, 0)])

if __name__ == '__main__':
    q_id = sys.argv[1] if len(sys.argv) > 1 else "Unknown"
    collect_benchmark_results(q_id)