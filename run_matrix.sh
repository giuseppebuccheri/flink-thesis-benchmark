#!/bin/bash

chmod +x run_experiment.sh

echo "🧹 Inizializzazione file dei risultati (results.csv)..."
echo "Matrice,Query_ID,Throughput_cc,GC_Time_ms,Events,Users,Selectivity,Correlation" > results.csv.tmp

# Matrice A (Stateless - velocissima): minimo 10M eventi
EVENTS_A=1000000

# Matrice B (Stateful - Aggregation): 5M è già sufficiente
# perché il GC rallenta Flink, soprattutto a cardinalità alta
EVENTS_B=500000

# Matrice C (Join - molto pesante): 3M sono probabilmente sufficienti
# perché il join unbounded satura la memoria rapidamente
EVENTS_C=300000

echo "==================================================="
echo " 🧪 INIZIO MATRICE A: Test Selectivity (Stateless)"
echo "==================================================="
for SEL in 0.01 0.50 0.99; do
    echo "▶️ Lancio Test A con Selectivity = $SEL ($EVENTS_A EVENTI)"
    ./run_experiment.sh 1 $EVENTS_A 10000 $SEL 0.0
    
    LAST_Q=$(tail -n 1 results.csv | tr -d '\r\n')
    echo "Matrice_A_Selectivity,$LAST_Q,$EVENTS_A,10000,$SEL,0.0" >> results.csv.tmp
    rm results.csv
done

echo "==================================================="
echo " 🧪 INIZIO MATRICE B: Test Cardinality (Stateful)"
echo "==================================================="
for USR in 100 10000 100000; do
    echo "▶️ Lancio Test B con Users = $USR ($EVENTS_B EVENTI)"
    ./run_experiment.sh 3 $EVENTS_B $USR 0.05 0.0
    
    LAST_Q=$(tail -n 1 results.csv | tr -d '\r\n')
    echo "Matrice_B_Cardinality,$LAST_Q,$EVENTS_B,$USR,0.05,0.0" >> results.csv.tmp
    rm results.csv
done

echo "==================================================="
echo " 🧪 INIZIO MATRICE C: Test Join Correlation (Heavy Stateful)"
echo "==================================================="
for CORR in 0.10 0.50 1.00; do
    echo "▶️ Lancio Test C con Join Correlation = $CORR ($EVENTS_C EVENTI)"
    ./run_experiment.sh 4 $EVENTS_C 100000 0.05 $CORR
    
    LAST_Q=$(tail -n 1 results.csv | tr -d '\r\n')
    echo "Matrice_C_JoinCorrelation,$LAST_Q,$EVENTS_C,100000,0.05,$CORR" >> results.csv.tmp
    rm results.csv
done

# Finalizza il CSV
mv results.csv.tmp results.csv

python3 scripts/plot_results.py results.csv