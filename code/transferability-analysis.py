import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
import networkx as nx
import matplotlib as mpl

# Αγνόηση warnings για pandas
warnings.simplefilter(action='ignore', category=pd.errors.SettingWithCopyWarning)

# Δημιουργία φακέλου για αποθήκευση αποτελεσμάτων
os.makedirs('transferability_results', exist_ok=True)

# ---------------- Φόρτωση δεδομένων ---------------- #
path = os.path.join('fewshot_results', 'finetune_all.csv')
full = pd.read_csv(path)
full.set_index("ticker")
print(full)
print()

path = os.path.join('fewshot_results', 'finetune_itself.csv')
itself = pd.read_csv(path)
itself.set_index("ticker")
print(itself)

# Λίστα μετοχών
tickers = ["AAPL", "TSLA", "XOM", "SPY", "JNJ", "AMD", "PG"]

# Λίστα γεγονότων (δεν χρησιμοποιούνται παρακάτω, αλλά ορίζονται)
event_titles = ["AAPL – Crash & Rebound (2020-03-10)",
                "TSLA – High-Beta Cooling (2021-01-15)",
                "XOM – Oil Cycle Peak (2022-06-01)",
                "SPY – Drawdown Chop (2022-09-15)",
                "JNJ – Low-Volatility Stretch (2019-08-01)",
                "AMD – Tech Selloff (2018-10-10)",
                "PG – Macro-Irrelevant Calm (2015-06-15)"]

# Αρχή/τέλος περιόδων για γεγονότα
starts = ["2019-09-02","2020-07-01","2021-10-01","2022-01-03","2018-11-01","2018-04-02","2014-12-01"]
ends   = ["2020-06-01","2021-06-30","2022-09-30","2022-12-30","2020-01-01","2019-03-29","2016-01-01"]

# Μετρικές
metrics = ["rmse", "mae", "mape", "hit_rate", "precision", "recall", "f1"]
results_dir = "fewshot_results"

# ---------------- Heatmaps για κάθε metric ---------------- #
for metric in metrics:
    transfer_matrix = pd.DataFrame(index=tickers, columns=tickers)

    for from_ticker in tickers:
        path = os.path.join(results_dir, f'finetune_on_{from_ticker}.csv')
        df = pd.read_csv(path, index_col="ticker")
        for to_ticker in tickers:
            transfer_matrix.loc[from_ticker, to_ticker] = df.loc[to_ticker, metric]

    transfer_matrix = transfer_matrix.astype(float)

    # Επιλογή καλύτερου μοντέλου (min για σφάλματα, max για classification metrics)
    best_models = transfer_matrix.idxmin() if metric in ["rmse", "mae", "mape"] else transfer_matrix.idxmax()

    plt.figure(figsize=(10, 8))
    ax = sns.heatmap(
        transfer_matrix, 
        annot=True, 
        fmt=".3f", 
        cmap="viridis", 
        linewidths=0.5,
        cbar_kws={"label": metric.upper()}
    )

    # Σχεδίαση πλαισίου στο καλύτερο σημείο
    for col_idx, col in enumerate(tickers):
        best_row = tickers.index(best_models[col])
        ax.add_patch(plt.Rectangle((col_idx, best_row), 1, 1, fill=False, edgecolor='white', lw=2.5))

    plt.title(f"Transferability Heatmap ({metric.upper()})")
    plt.xlabel("Predicted Ticker")
    plt.ylabel("Trained On")
    plt.tight_layout()
    plt.savefig(f"./transferability_results/{metric}_heatmap.png")
    plt.show()

# ---------------- Barplots με μέσες επιδόσεις ---------------- #
itself_df = pd.read_csv(os.path.join(results_dir, "finetune_itself.csv"), index_col="ticker")

for metric in metrics:
    transfer_matrix = pd.DataFrame(index=tickers, columns=tickers)
    for from_ticker in tickers:
        df = pd.read_csv(os.path.join(results_dir, f'finetune_on_{from_ticker}.csv'), index_col="ticker")
        for to_ticker in tickers:
            transfer_matrix.loc[from_ticker, to_ticker] = df.loc[to_ticker, metric]
    transfer_matrix = transfer_matrix.astype(float)

    # Μέσος όρος επιδόσεων
    avg_scores = (
        transfer_matrix.mean(axis=1)
        .sort_values(ascending=(metric in ["rmse", "mae", "mape","hit_rate","precision","recall","f1"]))
    )
    itself_scores = itself_df.loc[avg_scores.index, metric]

    x = range(len(avg_scores))
    width = 0.35

    plt.figure(figsize=(10, 5))
    plt.bar(x, avg_scores, width=width, label='Avg Across All Targets', color='skyblue', edgecolor='black')
    plt.xticks([i + width/2 for i in x], avg_scores.index)
    plt.title(f"{metric.upper()} – Average")
    plt.ylabel(metric.upper())
    plt.xlabel("Source Ticker (Finetuned)")
    plt.grid(axis='y', linestyle="--", alpha=0.5)
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"./transferability_results/{metric}_avg.png")
    plt.show()

    # Clustered heatmap
    sns.clustermap(
        transfer_matrix, 
        cmap="coolwarm", 
        annot=True, 
        fmt=".3f", 
        figsize=(8, 6)
    )
    plt.suptitle(f"Clustered Transferability Matrix ({metric.upper()})", y=1.02)
    plt.savefig(f"./transferability_results/{metric}_cluster.png")
    plt.show()

# ---------------- Γραφoι (graphs) ---------------- #
for metric in metrics:
    transfer_matrix = pd.DataFrame(index=tickers, columns=tickers)
    for from_ticker in tickers:
        path = os.path.join(results_dir, f'finetune_on_{from_ticker}.csv')
        df = pd.read_csv(path, index_col="ticker")
        for to_ticker in tickers:
            transfer_matrix.loc[from_ticker, to_ticker] = df.loc[to_ticker, metric]
    transfer_matrix = transfer_matrix.astype(float)

    # Λίστα ακμών με βάρη
    edges_list = []
    values = []
    for from_ticker in tickers:
        for to_ticker in tickers:
            if from_ticker != to_ticker:
                val = transfer_matrix.loc[from_ticker, to_ticker]
                edges_list.append((from_ticker, to_ticker, val))
                values.append(val)
    
    vmin, vmax = min(values), max(values)
    norm = lambda x: (x - vmin) / (vmax - vmin)

    # Δημιουργία directed graph
    G = nx.DiGraph()
    for from_ticker, to_ticker, val in edges_list:
        G.add_edge(from_ticker, to_ticker, weight=val, norm_weight=norm(val))

    pos = nx.spring_layout(G, seed=42)
    
    plt.figure(figsize=(12, 10))
    nx.draw_networkx_nodes(G, pos, node_color='skyblue', node_size=1200)
    nx.draw_networkx_labels(G, pos, font_size=10)

    # Χρώμα ακμών ανάλογα με το βάρος
    edge_colors = [d["norm_weight"] for _, _, d in G.edges(data=True)]
    nx.draw_networkx_edges(
        G, pos,
        edge_color=edge_colors,
        edge_cmap=plt.cm.viridis,
        width=2,
        arrows=True,
    )

    # Colorbar
    sm = mpl.cm.ScalarMappable(cmap=plt.cm.viridis, norm=plt.Normalize(vmin=vmin, vmax=vmax))
    sm.set_array([])
    ax = plt.gca()
    cbar = plt.colorbar(sm, ax=ax)
    cbar.set_label(f"{metric.upper()} (Normalized)")

    plt.title(f"Transfer Graph – {metric.upper()}")
    plt.axis("off")
    plt.tight_layout()
    plt.savefig(f"./transferability_results/{metric}_graph.png")
    plt.show()
