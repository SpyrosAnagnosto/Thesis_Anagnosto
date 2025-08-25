import math
import os
import torch
import numpy  as np
import pandas as pd
import matplotlib.pyplot as plt
import joblib
import warnings
import pandas_ta as ta

from torch.optim              import AdamW
from torch.optim.lr_scheduler import OneCycleLR
from transformers             import EarlyStoppingCallback, Trainer, TrainingArguments, set_seed
from tsfm_public              import TimeSeriesForecastingPipeline 
from tsfm_public              import TimeSeriesPreprocessor
from tsfm_public              import TinyTimeMixerForPrediction
from tsfm_public              import TrackingCallback
from tsfm_public              import count_parameters
from tsfm_public              import get_datasets

from torch.utils.data import DataLoader
from tsfm_public.toolkit.time_series_preprocessor import prepare_data_splits

from sklearn.metrics import precision_score, recall_score, f1_score
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler
from sklearn.inspection import permutation_importance  # για permutation importance
import shap
import ast

# Αγνόηση συγκεκριμένων warnings από pandas
warnings.simplefilter(action='ignore', category=pd.errors.SettingWithCopyWarning)

# Επιλογή συσκευής (GPU/CPU)
device    = "cuda" if torch.cuda.is_available() else "cpu"

# Μονοπάτι προεκπαιδευμένου μοντέλου
load_path = "ibm-granite/granite-timeseries-ttm-r2"

# Δημιουργία φακέλων για αποθήκευση αποτελεσμάτων
os.makedirs('feature_importance_results', exist_ok=True)
os.makedirs('expanded_data', exist_ok=True)
os.makedirs('indicator_results', exist_ok=True)

# Λίστα tickers για επεξεργασία
tickers = ["AAPL","TSLA","XOM","SPY","JNJ","AMD","PG"]

# Προδιαγραφές των στηλών για τον preprocessor
column_specifiers = {
    "timestamp_column": "date",
    "id_columns": [],
    "target_columns": ["close"],
    "control_columns": [
        "open", "high", "low", "volume",
        "sma_20", "ema_20", "wma_20", "tema_20",
        "rsi_14", "stoch_k", "cci_14",
        "bb_upper", "bb_lower", "atr_14",
        "obv", "cmf_20"
    ]
}

# ---------------- Προσθήκη τεχνικών δεικτών και αποθήκευση expanded datasets ---------------- #
for ticker in tickers:
    # Φόρτωση αρχικών δεδομένων
    df = pd.read_csv(f"./processed_data/{ticker}.csv", parse_dates=["date"])
    df.set_index("date", inplace=True)

    # Κινητοί μέσοι και εκθετικοί μέσοι
    df["sma_20"]  = ta.sma(df["close"], length=20)
    df["ema_20"]  = ta.ema(df["close"], length=20)
    df["wma_20"]  = ta.wma(df["close"], length=20)
    df["tema_20"] = ta.tema(df["close"], length=20)

    # Δείκτες ορμής/ταλαντωτές
    df["rsi_14"]  = ta.rsi(df["close"], length=14)
    df["stoch_k"] = ta.stoch(df["high"], df["low"], df["close"])["STOCHk_14_3_3"]
    df["cci_14"]  = ta.cci(df["high"], df["low"], df["close"], length=14)

    # Bollinger Bands και ATR
    bbands         = ta.bbands(df["close"], length=20)
    df["bb_upper"] = bbands["BBU_20_2.0"]
    df["bb_lower"] = bbands["BBL_20_2.0"]
    df["atr_14"]   = ta.atr(df["high"], df["low"], df["close"], length=14)

    # Όγκου: OBV και CMF
    df["obv"]    = ta.obv(df["close"], df["volume"])
    df["cmf_20"] = ta.cmf(df["high"], df["low"], df["close"], df["volume"], length=20)

    # Καθαρισμός NaN που προκύπτουν από τα rollings
    df.dropna(inplace=True)
    df.reset_index(inplace=True)

    # Αποθήκευση εμπλουτισμένου dataset
    df.to_csv(os.path.join("./expanded_data", f"{ticker}.csv"), index=False)

# ---------------- Εκπαίδευση/Πρόβλεψη και συλλογή συνόλων για ερμηνεία ---------------- #
logs      = []  # λίστες για logs εκπαίδευσης (αν χρειαστούν)
test_sets = []  # X καθαρισμένα για κάθε ticker
targets   = []  # y (στόχος) καθαρισμένα για κάθε ticker
imp_dfs   = []  # DataFrames με permutation importances για κάθε ticker

for ticker in tickers:
    # Φόρτωση εμπλουτισμένων δεδομένων
    df = pd.read_csv(f"./expanded_data/{ticker}.csv", parse_dates=["date"])
    
    # Preprocessor για time series
    preprocessor = TimeSeriesPreprocessor(
        **column_specifiers,
        context_length     = 512,
        prediction_length  = 96,
        scaling            = True,
        encode_categorical = False,
        scaler_type        = "standard",
    )
    preprocessor.train(df)

    # Φόρτωση προεκπαιδευμένου TinyTimeMixer
    model = TinyTimeMixerForPrediction.from_pretrained(
        load_path,
        num_input_channels             = preprocessor.num_input_channels,
        prediction_channel_indices     = preprocessor.prediction_channel_indices,
        exogenous_channel_indices      = preprocessor.exogenous_channel_indices,
        fcm_use_mixer                  = False,
        enable_forecast_channel_mixing = False,
        decoder_mode                   = "direct",
    )

    # Split δεδομένων σε train/valid/test
    train_df, valid_df, test_df = prepare_data_splits(
        df,
        context_length=512,
        split_config={"train": 0.6, "test": 0.4}
    )

    # Πάγωμα backbone (μόνο head εκπαιδεύεται)
    for param in model.backbone.parameters():
        param.requires_grad = False

    # Δημιουργία PyTorch Datasets από τον preprocessor
    train_set, valid_set, test_set = get_datasets(
        preprocessor,
        df,
        {"train": 0.6, "test": 0.4},
        fewshot_fraction    = 0.7,              # χρήση 70% του train για fine-tuning
        fewshot_location    = "first",
        use_frequency_token = model.config.resolution_prefix_tuning,
    )

    # Υπερπαράμετροι/ρυθμίσεις εκπαίδευσης
    learning_rate  = 0.004
    num_epochs     = 5
    patience       = 10
    batch_size     = 64

    # Ρυθμίσεις Trainer
    args = TrainingArguments(
        output_dir                  = os.path.join('fewshot_results', "output"),
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
        logging_dir                 = os.path.join('fewshot_results', "logs"),
        load_best_model_at_end      = True,
        metric_for_best_model       = "eval_loss",
        greater_is_better           = False,
        use_cpu                     = device != "cuda",
    )

    # Early stopping και tracking callbacks
    early_stopping_callback = EarlyStoppingCallback(
        early_stopping_patience=patience,
        early_stopping_threshold=1e-5,
    )
    tracking_callback = TrackingCallback()

    # Optimizer/Scheduler
    optimizer = AdamW(model.parameters(), lr=learning_rate)
    scheduler = OneCycleLR(
        optimizer, learning_rate, epochs=num_epochs,
        steps_per_epoch=math.ceil(len(train_set) / batch_size),
    )

    # Δημιουργία Trainer
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
    log      = trainer.state.log_history
    log_df   = pd.DataFrame(log)  # αν θελήσουμε αργότερα να το σώσουμε
    logs.append(log_df)

    # Pipeline πρόβλεψης για test set
    pipeline = TimeSeriesForecastingPipeline(
        model,
        device            = device, 
        feature_extractor = preprocessor,
        batch_size        = batch_size,
    )

    # Υπολογισμός προβλέψεων στο test_df
    forecast = pipeline(test_df)
    forecast["date"]     = pd.to_datetime(forecast["date"])
    forecast["y_true"]   = forecast["close"].str[0]           # εξαγωγή πραγματικών
    forecast["y_pred"]   = forecast["close_prediction"].str[0]# εξαγωγή προβλέψεων
    forecast["residual"] = forecast["y_true"] - forecast["y_pred"]

    # Αποθήκευση αποτελεσμάτων πρόβλεψης
    output_path = os.path.join('indicator_results', f'{ticker}_results.csv')
    forecast.to_csv(output_path, index=False)

    # Επιλογή χαρακτηριστικών από το test_df (ευθυγράμμιση με forecast)
    test = test_df.iloc[511:].copy()  # μετά από context length
    X = test[[
        'open', 'high', 'low', 'volume',
        'sma_20', 'ema_20', 'wma_20', 'tema_20',
        'rsi_14', 'stoch_k', 'cci_14',
        'bb_upper', 'bb_lower', 'atr_14',
        'obv', 'cmf_20'
    ]]

    # Προετοιμασία στόχου/πρόβλεψης
    y_true = forecast["y_true"]
    y_pred = forecast["y_pred"]

    # Καθαρισμός NaN και ευθυγράμμιση δειγμάτων
    X = X.reset_index(drop=True)
    combined = pd.concat([X, pd.Series(y_true, name="target")], axis=1)
    cleaned  = combined.dropna()
    X_clean  = cleaned.drop("target", axis=1)
    y_clean  = cleaned["target"].values

    # Αποθήκευση για χρήση σε SHAP ανά ticker
    test_sets.append(X_clean)
    targets.append(y_clean)

    # Εκπαίδευση απλού surrogate (Ridge) πάνω στις ΠΡΟΒΛΕΨΕΙΣ του μοντέλου
    surrogate = Ridge()
    surrogate.fit(X, y_pred)  # fit στο X και στις προβλέψεις για surrogate mapping

    # Permutation importance του surrogate έναντι του y_clean (MSE-based)
    result = permutation_importance(
        surrogate,
        X_clean,
        y_clean,
        n_repeats=10,
        scoring='neg_mean_squar
