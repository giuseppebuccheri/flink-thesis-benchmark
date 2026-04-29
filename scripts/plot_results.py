import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

def load_and_clean_data(filename):
    """
    Carica il CSV formattato correttamente.
    Dato che la Matrice C ha una colonna in più ('Correlation'), passiamo esplicitamente
    i nomi delle colonne per far gestire a Pandas le righe di lunghezza diversa.
    """
    col_names = ['Matrice', 'Query_ID', 'Throughput', 'GC_Time', 'Events', 'Users', 'Selectivity', 'Correlation']
    
    # skiprows=1 salta la riga di intestazione testuale originale del file
    df = pd.read_csv(filename, names=col_names, skiprows=1)
    
    # La colonna può arrivare già numerica oppure come stringa con prefisso JoinRate_.
    # La normalizziamo sempre a float per rendere il parsing robusto.
    df['Correlation'] = pd.to_numeric(
        df['Correlation'].astype(str).str.replace('JoinRate_', '', regex=False),
        errors='coerce'
    )
    
    return df

def plot_metrics(df):
    # Stile professionale per la tesi
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    fig.suptitle('Apache Flink Stream Processing Profiling - Cost Functions', fontsize=16, fontweight='bold', y=1.05)

    # --- MATRICE A: SELECTIVITY (Stateless) ---
    df_a = df[df['Matrice'].str.contains('Matrice_A', na=False)].sort_values('Selectivity')
    ax1 = axes[0]
    ax1_gc = ax1.twinx() # Secondo asse Y per il GC Time
    
    ax1.plot(df_a['Selectivity'] * 100, df_a['Throughput'], 'b-o', linewidth=2, label='Throughput (cc)')
    ax1_gc.plot(df_a['Selectivity'] * 100, df_a['GC_Time'], 'r--s', linewidth=2, label='GC Time (ms)')
    
    ax1.set_title('Matrix A: Filter Selectivity (Stateless)', fontsize=12)
    ax1.set_xlabel('Selectivity (%)')
    ax1.set_ylabel('Throughput (records/sec)', color='b')
    ax1_gc.set_ylabel('GC Time (ms)', color='r')
    ax1.set_ylim(0, max(df_a['Throughput']) * 1.2)
    ax1_gc.set_ylim(0, 500) # Limite basso fisso per far capire che è trascurabile

    # --- MATRICE B: CARDINALITY (Stateful) ---
    df_b = df[df['Matrice'].str.contains('Matrice_B', na=False)].sort_values('Users')
    ax2 = axes[1]
    ax2_gc = ax2.twinx()
    
    # Asse X logaritmico (10^2, 10^4, 10^6)
    ax2.plot(np.log10(df_b['Users']), df_b['Throughput'], 'b-o', linewidth=2)
    ax2_gc.plot(np.log10(df_b['Users']), df_b['GC_Time'], 'r--s', linewidth=2)
    
    ax2.set_title('Matrix B: State Cardinality (Stateful)', fontsize=12)
    ax2.set_xlabel('Unique Users (Log10)')
    ax2.set_ylabel('Throughput (records/sec)', color='b')
    ax2_gc.set_ylabel('GC Time (ms)', color='r')
    ax2.set_ylim(0, max(df_b['Throughput']) * 1.2)
    ax2_gc.set_ylim(0, max(df_b['GC_Time']) * 1.2)

    # --- MATRICE C: JOIN CORRELATION (Heavy Stateful) ---
    df_c = df[df['Matrice'].str.contains('Matrice_C', na=False)].sort_values('Correlation')
    ax3 = axes[2]
    ax3_gc = ax3.twinx()
    
    ax3.plot(df_c['Correlation'] * 100, df_c['Throughput'], 'b-o', linewidth=2)
    ax3_gc.plot(df_c['Correlation'] * 100, df_c['GC_Time'], 'r--s', linewidth=2)
    
    ax3.set_title('Matrix C: Join Correlation (Heavy Stateful)', fontsize=12)
    ax3.set_xlabel('Join Correlation Rate (%)')
    ax3.set_ylabel('Throughput (records/sec)', color='b')
    ax3_gc.set_ylabel('GC Time (ms)', color='r')
    ax3.set_ylim(0, max(df_c['Throughput']) * 1.2)
    ax3_gc.set_ylim(0, max(df_c['GC_Time']) * 1.2)

    # Legenda globale in alto
    lines_1, labels_1 = ax1.get_legend_handles_labels()
    lines_2, labels_2 = ax1_gc.get_legend_handles_labels()
    fig.legend(lines_1 + lines_2, labels_1 + labels_2, loc='upper center', bbox_to_anchor=(0.5, 0.95), ncol=2)

    # Salvataggio e render
    plt.tight_layout()
    plt.savefig('tesi_grafici_flink.png', dpi=300, bbox_inches='tight')
    print("✅ Grafici generati con successo! Controlla il file 'tesi_grafici_flink.png'")
    plt.show()

if __name__ == '__main__':
    # Esegui il parsing e disegna i grafici
    df = load_and_clean_data('results.csv')
    print("Dati importati da CSV:")
    print(df.to_string())
    plot_metrics(df)