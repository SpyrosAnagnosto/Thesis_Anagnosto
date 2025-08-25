import math
import os
import torch
import numpy  as np
import pandas as pd
import matplotlib.pyplot as plt
import joblib
import warnings
from mpl_toolkits.mplot3d import Axes3D
import matplotlib.cm as cm
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
from sklearn.metrics import precision_score, recall_score, f1_score
import ast

# Αγνόηση warnings για pandas
warnings.simplefilter(action='ignore', category=pd.errors.SettingWithCopyWarning)

# Ορισμός συσκευής
device             = "cuda" if torch.cuda.is_available() else "cpu"
# Προεκπαιδευμένο μοντέλο
load_path          =  "ibm-granite/granite-timeseries-ttm-r2"

# Λίστα μετοχών
tickers = ["AAPL","TSLA","XOM","SPY","JNJ","AMD","PG"]

# Προδιαγραφές στηλών
column_specifiers = {
        "timestamp_column": "date",
        "id_columns": [],
        "target_columns": ["close"],
        "control_columns": [] 
        } 

# Τίτλοι γραφημάτων για Train
t_titles = [
    "Train Loss vs Epoch (Fewshot Fraction = 6%)",
    "Train Loss vs Epoch (Fewshot Fraction = 12%)",
    "Train Loss vs Epoch (Fewshot Fraction = 18%)",
    "Train Loss vs Epoch (Fewshot Fraction = 24%)",
    "Train Loss vs Epoch (Fewshot Fraction = 30%)",
    "Train Loss vs Epoch (Fewshot Fraction = 36%)",
    "Train Loss vs Epoch (Fewshot Fraction = 42%)",
    "Train Loss vs Epoch (Fewshot Fraction = 48%)",
    "Train Loss vs Epoch (Fewshot Fraction = 54%)",
]

# Τίτλοι γραφημάτων για Eval
e_titles = [
    "Eval Loss vs Epoch (Fewshot Fraction = 6%)",
    "Eval Loss vs Epoch (Fewshot Fraction = 12%)",
    "Eval Loss vs Epoch (Fewshot Fraction = 18%)",
    "Eval Loss vs Epoch (Fewshot Fraction = 24%)",
    "Eval Loss vs Epoch (Fewshot Fraction = 30%)",
    "Eval Loss vs Epoch (Fewshot Fraction = 36%)",
    "Eval Loss vs Epoch (Fewshot Fraction = 42%)",
    "Eval Loss vs Epoch (Fewshot Fraction = 48%)",
    "Eval Loss vs Epoch (Fewshot Fraction = 54%)",
]

# Διαφορετικά ποσοστά fewshot
fewshot_fractions = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]

# Εκπαίδευση για κάθε ποσοστό fewshot
for frac in fewshot_fractions:
    logs      = []
    for ticker in tickers:
        # Φόρτωση δεδομένων
        df = pd.read_csv(f"./processed_data/{ticker}.csv", parse_dates=["date"])
        
        # Προεπεξεργασία
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
            decoder_mode                   = "direct",
        )

        # Διαχωρισμός δεδομένων
        train_df, valid_df, test_df = prepare_data_splits(
            df,
            context_length=512,
            split_config={"train": 0.6,"test": 0.4}
        )
    
        # Επιτρέπουμε να γίνουν fine-tuned όλα τα layers
        for param in model.backbone.parameters():
            param.requires_grad = True
    
        # Δημιουργία train/valid/test sets
        train_set, valid_set, test_set = get_datasets(
            preprocessor,
            df,
            {"train": 0.6,"test": 0.4},
            fewshot_fraction    = frac,
            fewshot_location    = "first",
            use_frequency_token = model.config.resolution_prefix_tuning,
        )
    
        # Υπερπαράμετροι
        learning_rate  = 0.004
        num_epochs     = 5
        patience       = 10
        batch_size     = 64
    
        # Ορισμός παραμέτρων εκπαίδευσης
        args = TrainingArguments(
            output_dir                  = os.path.join('fewshot_percent_results', "output"),
            overwrite_output_dir        = True,
            learning_rate               = learning_rate,
            num_train_epochs            = num_epochs,
            do_eval                     = True,
            eval_strategy               = "epoch",
            per_device_train_batch_size = batch_size,
            per_device_eval_batch_size  = batch_size,
            dataloader_num_workers      = 4,
            report_to                   = None,
            save_strategy               = "epoch",
            logging_strategy            = "epoch",
            save_total_limit            = 1,
            logging_dir                 = os.path.join('fewshot_percent_results', "logs"),  
            load_best_model_at_end      = True,  
            metric_for_best_model       = "eval_loss",  
            greater_is_better           = False,  
            use_cpu                     = device != "cuda",
        )
    
        # Early stopping
        early_stopping_callback = EarlyStoppingCallback(
            early_stopping_patience=patience,
            early_stopping_threshold=0.00001, 
            )
        
        # Callback καταγραφής
        tracking_callback = TrackingCallback()
    
        # Optimizer & Scheduler
        optimizer = AdamW(model.parameters(), lr=learning_rate)
        scheduler = OneCycleLR(optimizer, learning_rate, epochs=num_epochs, steps_per_epoch=math.ceil(len(train_set) / (batch_size)),)
    
        # Trainer
        trainer = Trainer(
            model         = model,
            args          = args,
            train_dataset = train_set,
            eval_dataset  = valid_set,
            callbacks     = [early_stopping_callback, tracking_callback],
            optimizers    = (optimizer, scheduler),
        )
        
        # Εκπαίδευση
        trainer.train()
        log_df = pd.DataFrame(trainer.state.log_history)
    
        # Καθαρισμός logs
        cols = log_df.columns.tolist()
        start = cols.index('eval_loss')
        end = cols.index('train_loss')
        target_cols = cols[start:end+1]
        k= num_epochs-1
        
        for i in range(0,num_epochs):
            for col in target_cols:
                log_df.at[i, col] = log_df.at[i+1, col]
            log_df = log_df.drop(index=i+1).reset_index(drop=True)
        log_df.insert(0, 'ticker', ticker)
        logs.append(log_df)
        
    # Αποθήκευση για κάθε ποσοστό
    combined_df = pd.concat(logs, ignore_index=True)
    combined_df.to_csv(f"./fewshot_percent_results/frac_{frac}.csv", index=False)

# ---------------- Γραφήματα Train Loss ---------------- #
for frac in fewshot_fractions:
    df = pd.read_csv(f"./fewshot_percent_results/frac_{frac}.csv")  
    tickers = df['ticker'].dropna().unique()
    eval_loss_array = np.zeros((len(tickers), 5))
    
    for i, ticker in enumerate(tickers):
        ticker_df = df[(df['ticker'] == ticker) & (df['epoch'].notna())]
        eval_loss_array[i, :] = ticker_df['loss'].values[:5]
    
    epochs = np.arange(1, 6)
    plt.figure(figsize=(8, 5))
    for i, ticker in enumerate(tickers):
        plt.plot(epochs, eval_loss_array[i], label=ticker, marker='o')
    
    plt.xlabel("Epoch")
    plt.ylabel("Train Loss")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()

# ---------------- Γραφήματα Eval Loss ---------------- #
for frac in fewshot_fractions:
    df = pd.read_csv(f"./fewshot_percent_results/frac_{frac}.csv")  
    tickers = df['ticker'].dropna().unique()
    eval_loss_array = np.zeros((len(tickers), 5))
    
    for i, ticker in enumerate(tickers):
        ticker_df = df[(df['ticker'] == ticker) & (df['epoch'].notna())]
        eval_loss_array[i, :] = ticker_df['eval_loss'].values[:5]
    
    epochs = np.arange(1, 6)
    plt.figure(figsize=(8, 5))
    for i, ticker in enumerate(tickers):
        plt.plot(epochs, eval_loss_array[i], label=ticker, marker='o')
    
    plt.xlabel("Epoch")
    plt.ylabel("Eval Loss")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()

# ---------------- Συνδυασμένα plots Train/Eval ---------------- #
for frac,t,e in zip(fewshot_fractions,t_titles,e_titles):
    df = pd.read_csv(f"./fewshot_percent_results/frac_{frac}.csv")  
    tickers = df['ticker'].dropna().unique()
    epochs = np.arange(1, 6)
    cmap = cm.get_cmap('tab10', len(tickers))

    train_loss_array = np.zeros((len(tickers), 5))
    eval_loss_array = np.zeros((len(tickers), 5))

    for i, ticker in enumerate(tickers):
        ticker_df = df[(df['ticker'] == ticker) & (df['epoch'].notna())]
        train_loss_array[i, :] = ticker_df['loss'].values[:5]
        eval_loss_array[i, :] = ticker_df['eval_loss'].values[:5]

    fig, axes = plt.subplots(1, 2, figsize=(14, 5), sharey=False)
    
    for i, ticker in enumerate(tickers):
        axes[0].plot(epochs, train_loss_array[i], label=ticker, marker='o', color=cmap(i))
    axes[0].set_title(t)
    axes[0].set_xlabel("Epoch")
    axes[0].set_ylabel("Train Loss")
    axes[0].legend()
    axes[0].grid(True)

    for i, ticker in enumerate(tickers):
        axes[1].plot(epochs, eval_loss_array[i], label=ticker, marker='o', color=cmap(i))
    axes[1].set_title(e)
    axes[1].set_xlabel("Epoch")
    axes[1].set_ylabel("Eval Loss")
    axes[1].legend()
    axes[1].grid(True)
    
    plt.tight_layout()
    plt.savefig(f"./fewshot_percent_results/plot_{frac}.png")
    plt.show()

# ---------------- Υπολογισμός Gradient Norms ---------------- #
norms = []
for frac in fewshot_fractions:
    df = pd.read_csv(f"./fewshot_percent_results/frac_{frac}.csv")  
    tickers = df['ticker'].dropna().unique()
    grad = np.zeros((len(tickers), 5))
    
    for i, ticker in enumerate(tickers):
        ticker_df = df[(df['ticker'] == ticker) & (df['epoch'].notna())]
        grad[i, :] = ticker_df['grad_norm'].values[:5]
    avg_grad = grad.mean(axis=0).reshape(1, 5)
    norms.append(avg_grad)
    print(avg_grad)

# 3D plot για norms
norms_array = np.vstack(norms)
epochs = np.arange(1, 6)
fractions = np.array(fewshot_fractions)
X, Y = np.meshgrid(epochs, fractions)

fig = plt.figure()
ax = fig.add_subplot(111, projection='3d')
ax.plot_surface(X, Y, norms_array, cmap='inferno')
ax.set_xlabel('Epoch')
ax.set_ylabel('Fewshot Fraction')
ax.set_zlabel('Avg Grad Norm')
ax.set_title('Gradient Norms over Epochs and Fewshot Fractions')
plt.savefig(f"./fewshot_percent_results/norm.png")
plt.show()

# ---------------- Generalization Gap ---------------- #
gaps = []  
stds = [] 

for frac in fewshot_fractions:
    df = pd.read_csv(f"./fewshot_percent_results/frac_{frac}.csv")
    tickers = df['ticker'].dropna().unique()
    eval_ = np.zeros((len(tickers), 5))
    loss  = np.zeros((len(tickers), 5))

    for i, ticker in enumerate(tickers):
        ticker_df = df[(df['ticker'] == ticker) & (df['epoch'].notna())]
        eval_[i, :] = ticker_df['eval_loss'].values[:5]
        loss[i, :]  = ticker_df['loss'].values[:5]

    last_column = eval_[:, -1]
    last_column2 = loss[:, -1]
    gap = last_column2 - last_column

    gaps.append(gap.mean())
    stds.append(gap.std())

# Διάγραμμα generalization gap
gaps = np.array(gaps)
stds = np.array(stds)
fractions = np.array(fewshot_fractions)

plt.figure(figsize=(8, 5))
plt.plot(fractions, gaps, marker='o', label='Mean Gap')
plt.fill_between(fractions, gaps - stds, gaps + stds, alpha=0.3, label='±1 Std Dev')
plt.xlabel("Fewshot Fraction")
plt.ylabel("Generalization Gap (Train - Eval)")
plt.title("Generalization Gap vs Fewshot Fraction")
plt.grid(True)
plt.legend()

# Προσαρμογή x-axis σε ποσοστά
percent_labels = [f"{int(f * 60)}%" for f in fractions]
plt.xticks(fractions, percent_labels)

plt.tight_layout()
plt.savefig(f"./fewshot_percent_results/gengap.png")
plt.show()
