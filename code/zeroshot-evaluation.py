import math
import os
import torch
import time
import ast
import joblib
import warnings
import random
import seaborn as sns
import numpy  as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import pearsonr
from sklearn.metrics import precision_score, recall_score, f1_score

from torch.optim              import AdamW
from torch.optim.lr_scheduler import OneCycleLR
from transformers             import EarlyStoppingCallback, Trainer, TrainingArguments, set_seed
from tsfm_public              import TimeSeriesForecastingPipeline 
from tsfm_public              import TimeSeriesPreprocessor
from tsfm_public              import TinyTimeMixerForPrediction
from tsfm_public              import TrackingCallback
from tsfm_public              import count_parameters
from tsfm_public              import get_datasets
from tsfm_public.toolkit.time_series_preprocessor import prepare_data_splits

# Αγνόηση warnings για pandas
warnings.simplefilter(action='ignore', category=pd.errors.SettingWithCopyWarning)

# Ορισμός συσκευής (GPU αν υπάρχει αλλιώς CPU)
device             = "cuda" if torch.cuda.is_available() else "cpu"

# Μοντέλο που θα φορτώσουμε
load_path          =  "ibm-granite/granite-timeseries-ttm-r2"

# Φάκελος για αποθήκευση αποτελεσμάτων
os.makedirs('baseline_results', exist_ok=True)

# Συνάρτηση για υπολογισμό μετρικών πρόβλεψης
def metrics(actual, prediction):
    a = np.stack(actual).flatten()
    p = np.stack(prediction).flatten()
    mask = ~np.isnan(a) & ~np.isnan(p)
    a, p = a[mask], p[mask]

    # Λάθη
    mae  = np.mean(np.abs(a - p))
    rmse = np.sqrt(np.mean((a - p)**2))
    mape = np.mean(np.abs((a - p) / (a + 1e-8))) * 100

    # Κατεύθυνση μεταβολής
    actual_diff = np.sign(np.diff(a))
    pred_diff   = np.sign(np.diff(p))
    hit_rate = np.mean(actual_diff == pred_diff)

    actual_up = actual_diff > 0
    pred_up   = pred_diff   > 0
    precision = precision_score(actual_up, pred_up, zero_division=0)
    recall    = recall_score(actual_up, pred_up, zero_division=0)
    f1        = f1_score(actual_up, pred_up, zero_division=0)

    return dict(mae=mae, rmse=rmse, mape=mape,
                hit_rate=hit_rate,
                precision=precision, recall=recall, f1=f1)


# Μετοχές για αξιολόγηση
tickers = ["AAPL","TSLA","XOM","SPY","JNJ","AMD","PG"]

# Τίτλοι γεγονότων
event_titles = ["AAPL – Crash & Rebound (2020-03-10)",
                "TSLA – High-Beta Cooling (2021-01-15)",
                "XOM – Oil Cycle Peak (2022-06-01)",
                "SPY – Drawdown Chop (2022-09-15)",
                "JNJ – Low-Volatility Stretch (2019-08-01)",
                "AMD – Tech Selloff (2018-10-10)",
                "PG – Macro-Irrelevant Calm (2015-06-15)"
                ]

# Περίοδοι αρχής και τέλους για γεγονότα
starts = ["2019-09-02","2020-07-01","2021-10-01","2022-01-03","2018-11-01","2018-04-02","2014-12-01"]
ends   = ["2020-06-01","2021-06-30","2022-09-30","2022-12-30","2020-01-01","2019-03-29","2016-01-01"]

# Ορισμός στηλών δεδομένων
column_specifiers = {
        "timestamp_column": "date",
        "id_columns": [],
        "target_columns": ["close"],
        "control_columns": [] 
        }

# Βρόχος για κάθε μετοχή
for ticker,start,end,event_title in zip(tickers,starts,ends,event_titles):
    # Φόρτωση δεδομένων
    df = pd.read_csv(f"./processed_data/{ticker}.csv", parse_dates=["date"])

    # Προεπεξεργασία δεδομένων
    preprocessor = TimeSeriesPreprocessor(
        **column_specifiers,
        context_length     = 512,
        prediction_length  = 96,
        scaling            = True,
        encode_categorical = False,
        scaler_type        = "standard",
    )
    preprocessor.train(df)

    # Φόρτωση μοντέλου
    model = TinyTimeMixerForPrediction.from_pretrained(
        load_path , 
        num_input_channels             = preprocessor.num_input_channels,
        prediction_channel_indices     = preprocessor.prediction_channel_indices,
        exogenous_channel_indices      = preprocessor.exogenous_channel_indices,
        fcm_use_mixer                  = False,
        enable_forecast_channel_mixing = False,
        decoder_mode                   = "mix_channel",
    )

    # Δημιουργία train/test split
    _, _, test_df = prepare_data_splits(
        df,
        context_length=512,
        split_config={"train": 0.0, "test": 0.4}
    )
    
    # Pipeline πρόβλεψης
    pipeline = TimeSeriesForecastingPipeline(
        model,
        device            = device,
        feature_extractor = preprocessor,
        batch_size        = 64,
    )

    # Δημιουργία προβλέψεων
    forecast = pipeline(test_df)
    forecast["date"]     = pd.to_datetime(forecast["date"])
    forecast["y_true"]   = forecast["close"].str[0]
    forecast["y_pred"]   = forecast["close_prediction"].str[0]
    forecast["residual"] = forecast["y_true"] - forecast["y_pred"]
    output_path          = os.path.join('baseline_results', f'{ticker}_results.csv')
    forecast.to_csv(output_path, index=False)

    # Γράφημα πραγματικών/προβλέψεων και residuals
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 6), sharex=True, gridspec_kw={'height_ratios': [3, 1]})
    ax1.plot(forecast["date"], forecast["y_true"], label="y_true", color='crimson')
    ax1.plot(forecast["date"], forecast["y_pred"], label="y_pred", color='green')
    ax1.set_title(f"Zero-Shot – {ticker}")
    ax1.set_ylabel("Close Price")
    ax1.grid(True)
    ax1.legend()
    
    ax2.plot(forecast["date"], forecast["residual"], label="Residual (y_true - y_pred)", color='gray')
    ax2.axhline(0, linestyle='--', color='black', linewidth=1)
    ax2.set_xlabel("Date")
    ax2.set_ylabel("Residual")
    ax2.grid(True)
    ax2.legend()
    plt.savefig(f'./baseline_results/{ticker}.png', dpi=300, bbox_inches='tight')
    plt.tight_layout()
    plt.show()

    # Φιλτράρισμα γεγονότος
    start = pd.to_datetime(start)
    end = pd.to_datetime(end)
    forecast         = pipeline(df)
    mask             = (forecast["date"] >= start) & (forecast["date"] <= end)
    event            = forecast.loc[mask]
    event["y_true"]  = event["close"].str[0]
    event["y_pred"]  = event["close_prediction"].str[0]
    output_path      = os.path.join('baseline_results', f'{ticker}_event_results.csv')
    event.to_csv(output_path, index=False)
    
    # Γράφημα γεγονότος
    plt.figure(figsize=(10, 4))
    plt.plot(event["date"], event["y_true"], label="y_true", color='crimson')
    plt.plot(event["date"], event["y_pred"], label="y_pred", color='green')
    plt.title(f"zeroshot - {event_title}")
    plt.xlabel("Date")
    plt.ylabel("Close Price")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(f'./baseline_results/{ticker}_event.png', dpi=300, bbox_inches='tight')
    plt.show()

# ---------------- Υπολογισμός μετρικών για όλα τα tickers ---------------- #

results = []
for ticker in tickers:
    df = pd.read_csv(f'baseline_results/{ticker}_results.csv')
    df = df.dropna(subset=["y_true", "y_pred"])
    m = metrics(df["y_true"].values, df["y_pred"].values)
    m["ticker"] = ticker
    results.append(m)

# Συγκεντρωτικός πίνακας baseline
baseline_df = pd.DataFrame(results).set_index("ticker")
print(baseline_df)
output_path = os.path.join("baseline_results", "baseline_output.csv")
baseline_df.to_csv(output_path)

# Μετρικές για γεγονότα
results = []
for ticker in tickers:
    df = pd.read_csv(f'baseline_results/{ticker}_event_results.csv')
    df = df.dropna(subset=["y_true", "y_pred"])
    m = metrics(df["y_true"].values, df["y_pred"].values)
    m["ticker"] = ticker
    results.append(m)

events_df = pd.DataFrame(results).set_index("ticker")
print(events_df)
output_path = os.path.join("baseline_results", "events_output.csv")
events_df.to_csv(output_path)

# ---------------- Ανάλυση ανά κατηγορία αγοράς (regimes) ---------------- #

regime_map = {
    "TSLA": "High-Volatility",
    "AMD": "High-Volatility",
    "SPY": "Sideways/Chop",
    "PG": "Low-Volatility",
    "JNJ": "Low-Volatility",
    "AAPL": "Post-Trend Reversal",
    "XOM": "Post-Trend Reversal"
}

baseline_df['regime_type'] = baseline_df.index.map(regime_map)
grouped_df = baseline_df.groupby('regime_type').mean(numeric_only=True)
output_path = 'baseline_results/regime_output.csv'
grouped_df.to_csv(output_path)
print(grouped_df)

# ---------------- Γραφήματα μετρικών ---------------- #

sns.set_style("whitegrid")
metrics_to_plot = ["mae", "rmse", "mape", "hit_rate", "precision", "recall", "f1"]
bar_color = "#66c2a5"
edge_color = "#2b2b2b"

for metric in metrics_to_plot:
    df_sorted = baseline_df.sort_values(metric, ascending=False)
    fig, ax = plt.subplots(figsize=(9, 5), dpi=120)
    bars = ax.bar(df_sorted.index, df_sorted[metric], color=bar_color, edgecolor=edge_color)
    for bar in bars:
        height = bar.get_height()
        ax.annotate(f'{height:.2f}',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 4),
                    textcoords="offset points",
                    ha='center',
                    va='bottom',
                    fontsize=9,
                    color='black')
    ax.set_title(f"{metric.upper()} per Asset", fontsize=14, weight='bold', pad=10)
    ax.set_xlabel("Ticker", fontsize=12)
    ax.set_ylabel(metric.upper(), fontsize=12)
    ax.tick_params(axis='x', rotation=45)
    ax.spines[['top', 'right']].set_visible(False)
    plt.tight_layout()
    ax.yaxis.grid(True, linestyle='--', alpha=0.5)
    plt.savefig(f"baseline_results/{metric}_per_asset.png", dpi=300, bbox_inches='tight')
    plt.show()

# ---------------- Συσχέτιση Volatility με Error ---------------- #

pear = []
for ticker in tickers:
    df = pd.read_csv(f"baseline_results/{ticker}_results.csv")
    df['log_return'] = np.log(df['y_true']).diff()
    df['rolling_volatility'] = df['log_return'].rolling(window=5).std()
    df['abs_error'] = np.abs(df['y_true"] - df['y_pred'])
    df_clean = df.dropna(subset=['rolling_volatility', 'abs_error'])
    
    plt.figure(figsize=(6, 4), dpi=120)
    plt.scatter(df_clean['rolling_volatility'], df_clean['abs_error'], alpha=0.6, color='slateblue', edgecolor='black')
    plt.title(f"Volatility vs Error Magnitude for {ticker}")
    plt.xlabel("5-day Rolling Volatility (log return std)")
    plt.ylabel("Absolute Prediction Error")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(f'./baseline_results/{ticker}_rolling_volatility.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    corr, pval = pearsonr(df_clean['rolling_volatility'], df_clean['abs_error'])
    pear.append(corr)

print(f"Pearson Correlation (Volatility vs Error) ")
for i, ticker in enumerate(tickers):
    print(f'{ticker}: ', pear[i])    

# ---------------- Συγκεντρωτικά plots ---------------- #

sns.set_style("whitegrid")
group1 = ["mae", "rmse", "mape"]
group2 = ["hit_rate", "precision", "recall", "f1"]
colors1 = sns.color_palette("Set2", len(group1))
colors2 = sns.color_palette("Set2", len(group2))
df = baseline_df.copy()

def plot_grouped_metrics(metrics, colors, title_suffix, filename):
    tickers = df.index.tolist()
    x = np.arange(len(tickers)) 
    width = 0.15  
    fig, ax = plt.subplots(figsize=(10, 5), dpi=120)
    for i, metric in enumerate(metrics):
        ax.bar(x + i * width, df[metric], width, label=metric.upper(), color=colors[i], edgecolor="black")
    ax.set_title(f"{title_suffix} per Ticker", fontsize=14, weight='bold')
    ax.set_xlabel("Ticker", fontsize=12)
    ax.set_ylabel("Metric Value", fontsize=12)
    ax.set_xticks(x + width * (len(metrics)-1)/2)
    ax.set_xticklabels(tickers, rotation=45)
    ax.legend()
    ax.spines[['top', 'right']].set_visible(False)
    ax.yaxis.grid(True, linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.savefig(f"baseline_results/{filename}.png", dpi=300, bbox_inches='tight')
    plt.show()

plot_grouped_metrics(group1, colors1, "Error Metrics", "group1_error_metrics")
plot_grouped_metrics(group2, colors2, "Directional Metrics", "group2_classification_metrics")





