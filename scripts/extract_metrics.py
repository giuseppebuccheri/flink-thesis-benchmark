import requests
import sys
import csv
import os

# --- CONFIGURAZIONE PROMETHEUS ---
PROMETHEUS_URL = "http://localhost:9090/api/v1/query"

def query_prometheus(promql_query):
    """Funzione per eseguire query PromQL e restituire il risultato come float."""
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

def collect_benchmark_results(action, io_mode):
    print("--- RACCOLTA METRICHE AUTOMATIZZATA ---")
    
    # 1. Throughput CC (In)
    throughput_query = '''max_over_time(
        sum(rate(flink_taskmanager_job_task_operator_numRecordsOut{operator_name=~"(?i).*Source.*"}[15s]))
        [3m:5s]
    )'''
    
    # 2. Sink Throughput Reale (Out)
    sink_records_query = '''max_over_time(
        sum(rate(flink_taskmanager_job_task_operator_numRecordsIn{
            operator_name=~"(?i).*Sink.*"}[15s]))
        [3m:5s]
    )'''
    
    # 3. GC Time
    gc_query = '''sum(increase(flink_taskmanager_Status_JVM_GarbageCollector_G1_Young_Generation_Time[2m]))
                + sum(increase(flink_taskmanager_Status_JVM_GarbageCollector_G1_Old_Generation_Time[2m]))'''    
    
    throughput = query_prometheus(throughput_query)
    sink_throughput = query_prometheus(sink_records_query)
    gc_time = query_prometheus(gc_query)
    
    if gc_time < 0:
        gc_time = 0.0

    print(f"Action: {action.upper()} | Mode: {io_mode.upper()}")
    print(f"In Throughput: {throughput:,.2f} r/s | Out Throughput: {sink_throughput:,.2f} r/s | GC Time: {gc_time:,.0f} ms")
    
    csv_filename = "results_io_benchmark.csv"
    file_exists = os.path.isfile(csv_filename)
    
    with open(csv_filename, mode='a', newline='') as file:
        writer = csv.writer(file)
        if not file_exists:
            # Rimosso Latency_p99 dall'header
            writer.writerow(["Action", "IO_Mode", "Throughput_cc", "GC_Time_ms", "Sink_Throughput"])
        
        # Rimosso latency_p99 dai dati salvati
        writer.writerow([action, io_mode, round(throughput, 2), round(gc_time, 0), round(sink_throughput, 2)])

if __name__ == '__main__':
    # Prende Action e IO_Mode da linea di comando (passati dal bash script)
    action_arg = sys.argv[1] if len(sys.argv) > 1 else "unknown_action"
    io_mode_arg = sys.argv[2] if len(sys.argv) > 2 else "unknown_mode"
    
    collect_benchmark_results(action_arg, io_mode_arg)