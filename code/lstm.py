
import os, re, glob
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import seaborn as sns

from sklearn.preprocessing import MinMaxScaler, StandardScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error, precision_score, recall_score, f1_score
from scipy.stats import wilcoxon

import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Input, LSTM, Dense
from tensorflow.keras.callbacks import EarlyStopping

DATA_DIR = "./final_data"
OUT_DIR  = "lstm_result"
FINALMODEL_DIR = "finalmodel"
os.makedirs(OUT_DIR, exist_ok=True)

TICKERS = ["AAPL","TSLA","XOM","SPY","JNJ","AMD","PG"]
EVENT_TITLES = [
    "AAPL – Crash & Rebound (2020-03-10)",
    "TSLA – High-Beta Cooling (2021-01-15)",
    "XOM – Oil Cycle Peak (2022-06-01)",
    "SPY – Drawdown Chop (2022-09-15)",
    "JNJ – Low-Volatility Stretch (2019-08-01)",
    "AMD – Tech Selloff (2018-10-10)",
    "PG – Macro-Irrelevant Calm (2015-06-15)"
]
STARTS = ["2019-09-02","2020-07-01","2021-10-01","2022-01-03","2018-11-01","2018-04-02","2014-12-01"]
ENDS   = ["2020-06-01","2021-06-30","2022-09-30","2022-12-30","2020-01-01","2019-03-29","2016-01-01"]

DATE_COL = "date"
TARGET   = "close"
FEATURES = ["open","high","low","sma_20","ema_20","wma_20","tema_20","bb_upper","bb_lower"]

LOOKBACK = 256       
EPOCHS   = 40
BATCH    = 64
SEED     = 42
np.random.seed(SEED); tf.random.set_seed(SEED)

REGIME_MAP = {
    "TSLA": "High-Volatility", "AMD": "High-Volatility",
    "SPY": "Sideways/Chop",
    "PG": "Low-Volatility", "JNJ": "Low-Volatility",
    "AAPL": "Post-Trend Reversal", "XOM": "Post-Trend Reversal",
}
EVENTS = {t: (EVENT_TITLES[i], STARTS[i], ENDS[i]) for i,t in enumerate(TICKERS)}

def slug(s):
    s = re.sub(r"[–—]", "-", s); s = re.sub(r"[^A-Za-z0-9_.-]+", "_", s); return s.strip("_")

def make_train_windows(X_sc, r_sc, lookback, train_end):
    idx = np.arange(lookback+1, train_end)  # +1 because return_t uses price_{t-1}
    if len(idx)==0: return np.array([]), np.array([])
    Xw = np.stack([X_sc[i-lookback:i, :] for i in idx], axis=0)
    yw = r_sc[idx]  # target is return at i
    return Xw, yw

def evaluate(y_true, y_pred):
    a = np.asarray(y_true, dtype=float)
    b = np.asarray(y_pred, dtype=float)

    m = np.isfinite(a) & np.isfinite(b)
    a = a[m]
    b = b[m]

    if a.size == 0 or b.size == 0:
        return np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan

    rmse = np.sqrt(mean_squared_error(a, b))
    mae  = mean_absolute_error(a, b)

    denom = np.where(a == 0.0, np.nan, a)
    mape = np.nanmean(np.abs((a - b) / denom)) * 100

    if a.size >= 2:
        true_dir = np.sign(np.diff(a)) > 0
        pred_dir = np.sign(np.diff(b)) > 0
        hit  = np.mean(true_dir == pred_dir)
        precision = precision_score(true_dir, pred_dir, zero_division=0)
        recall    = recall_score(true_dir, pred_dir, zero_division=0)
        f1        = f1_score(true_dir, pred_dir, zero_division=0)
    else:
        hit = precision = recall = f1 = np.nan

    return mae, rmse, mape, hit, precision, recall, f1


def plot_forecast(dts, y_true, y_pred, title, out_png):
    plt.figure(figsize=(10,4))
    plt.plot(dts, y_true, label="y_true", color="crimson")
    plt.plot(dts, y_pred, label="y_pred", color="green")
    plt.title(title); plt.xlabel("Date"); plt.ylabel("Close")
    plt.legend(); plt.grid(True); plt.tight_layout()
    os.makedirs(os.path.dirname(out_png), exist_ok=True)
    plt.savefig(out_png, dpi=300, bbox_inches="tight")
    plt.show()

def build_lstm(n_features):
    m = Sequential([
        Input(shape=(LOOKBACK, n_features)),
        LSTM(128, return_sequences=True),
        LSTM(64),
        Dense(1)  # outputs next-day log-return (scaled)
    ])
    m.compile(optimizer="adam", loss="mse")
    return m

# ------------------ One-step predictions on RETURNS -> back to PRICES ------------------
def predict_returns_to_prices(model, X_sc, r_sc, prices, indices, lookback, inv_r):
    idx = np.array(indices, dtype=int)
    idx = idx[idx >= lookback+1]  # need prev price for return
    y_pred_price, y_true_price, valid_idx = [], [], []
    for t in idx:
        x_win = X_sc[t-lookback:t, :]
        yhat_r_sc = model.predict(x_win[np.newaxis,...], verbose=0).ravel()[0]
        r_hat = inv_r(np.array([yhat_r_sc]))[0]      # log-return prediction (unscaled)
        p_prev = prices[t-1]
        p_hat  = p_prev * np.exp(r_hat)
        y_pred_price.append(p_hat)
        y_true_price.append(prices[t])
        valid_idx.append(t)
    return np.array(y_pred_price), np.array(y_true_price), np.array(valid_idx)

# ------------------ Run a ticker ------------------
def run_one_ticker(ticker):
    title, start, end = EVENTS[ticker]
    out_dir = os.path.join(OUT_DIR, ticker); os.makedirs(out_dir, exist_ok=True)

    df = pd.read_csv(os.path.join(DATA_DIR, f"{ticker}.csv"), parse_dates=[DATE_COL])
    df.columns = [c.lower() for c in df.columns]
    df = df.sort_values(DATE_COL).set_index(DATE_COL)

    for c in [TARGET] + FEATURES:
        if c not in df.columns: raise ValueError(f"[{ticker}] Missing column: {c}")

    X_all = df[FEATURES].values.astype(float)
    y_all = df[TARGET].values.astype(float)

    # target for the net = log-return of CLOSE
    log_prices = np.log(y_all)
    r_all = np.r_[0.0, np.diff(log_prices)] 
    if isinstance(r_all, pd.Series): r_all = r_all.values
    dates = df.index.values
    N = len(df)

    train_end   = int(0.42 * N)
    test1_start = N - int(0.40 * N)
    mask_event  = (df.index >= pd.Timestamp(start)) & (df.index <= pd.Timestamp(end))

    # Scale: X with MinMax (fit on train), returns with StandardScaler (fit on train)
    scaler_X = MinMaxScaler()
    X_train_sc = scaler_X.fit_transform(X_all[:train_end])
    X_all_sc   = np.vstack([X_train_sc, scaler_X.transform(X_all[train_end:])])
    scaler_r = StandardScaler()
    r_train_sc = scaler_r.fit_transform(r_all[:train_end].reshape(-1,1)).ravel()
    r_all_sc   = scaler_r.transform(r_all.reshape(-1,1)).ravel()
    inv_r = lambda arr: scaler_r.inverse_transform(arr.reshape(-1,1)).ravel()

    # Train windows (targets within train)
    Xtr, ytr = make_train_windows(X_all_sc, r_all_sc, LOOKBACK, train_end)
    if len(Xtr)==0: raise RuntimeError(f"[{ticker}] Train too short for LOOKBACK={LOOKBACK}")

    n_val = max(1, int(0.1*len(Xtr)))
    X_val, y_val = Xtr[-n_val:], ytr[-n_val:]
    X_trn, y_trn = Xtr[:-n_val], ytr[:-n_val]

    model = build_lstm(Xtr.shape[-1])
    batch_sz = min(BATCH, len(X_trn))
    model.fit(X_trn, y_trn,
              validation_data=(X_val, y_val),
              epochs=EPOCHS, batch_size=batch_sz, verbose=0,
              callbacks=[EarlyStopping(monitor="val_loss", patience=6, restore_best_weights=True)])

    # -------- Test set (last 40%) --------
    test_indices = np.arange(test1_start, N, dtype=int)
    yhat_p, ytrue_p, idx1 = predict_returns_to_prices(model, X_all_sc, r_all_sc, y_all, test_indices, LOOKBACK, inv_r)
    dates1 = dates[idx1]
    pd.DataFrame({"Date": dates1, "y_true": ytrue_p, "y_pred": yhat_p}).to_csv(
        os.path.join(out_dir, f"{ticker}_last_40pct_multivar.csv"), index=False
    )
    plot_forecast(dates1, ytrue_p, yhat_p, f"LSTM - {ticker} - test set",
                  os.path.join(out_dir, f"{ticker}_last_40pct_multivar.png"))
    m_last = evaluate(ytrue_p, yhat_p)

    # -------- Event window --------
    event_indices = np.where(mask_event)[0]
    yhat_ev_p, ytrue_ev_p, idxev = predict_returns_to_prices(model, X_all_sc, r_all_sc, y_all, event_indices, LOOKBACK, inv_r)
    datesev = dates[idxev]; ev_slug = slug(title)
    pd.DataFrame({"Date": datesev, "y_true": ytrue_ev_p, "y_pred": yhat_ev_p}).to_csv(
        os.path.join(out_dir, f"{ticker}_{ev_slug}_multivar.csv"), index=False
    )
    plot_forecast(datesev, ytrue_ev_p, yhat_ev_p, f"LSTM - {ticker} - {title}",
                  os.path.join(out_dir, f"{ticker}_{ev_slug}_multivar.png"))
    m_event = evaluate(ytrue_ev_p, yhat_ev_p)

    return m_last, m_event

# ------------------ Run all & summaries ------------------
rows_last, rows_ev = [], []
for t in TICKERS:
    last_m, ev_m = run_one_ticker(t)
    rows_last.append([t, *last_m])
    rows_ev.append([t, *ev_m])

cols = ["ticker","mae","rmse","mape","hit_rate","precision","recall","f1"]
pd.DataFrame(rows_last, columns=cols).to_csv(os.path.join(OUT_DIR, "summary_last40.csv"), index=False)
pd.DataFrame(rows_ev,   columns=cols).to_csv(os.path.join(OUT_DIR, "summary_events.csv"), index=False)
print("Saved per-ticker predictions & summaries to", OUT_DIR)

# ======================= POST =======================

# 1) Error distribution (all csvs)
errs=[]
for t in TICKERS:
    for p in glob.glob(os.path.join(OUT_DIR, t, "*_multivar.csv")):
        df=pd.read_csv(p)
        if {"y_true","y_pred"}.issubset(df.columns):
            e=(df["y_true"]-df["y_pred"]).dropna().values
            if e.size: errs.append(e)
if errs:
    errors=np.concatenate(errs)
    plt.figure(figsize=(10,5)); sns.set(style="whitegrid")
    sns.histplot(errors, bins=60, kde=True, color="darkorange", edgecolor="black", alpha=0.7)
    plt.title("Prediction Error Distribution – LSTM (all tickers)"); plt.xlabel("Prediction Error (y_true − y_pred)")
    plt.ylabel("Density"); plt.grid(True, linestyle="--", alpha=0.6); plt.tight_layout()
    plt.savefig(os.path.join(OUT_DIR,"error_dist_all.png"), dpi=300, bbox_inches="tight"); plt.show()

# 2) Regime matrix (test)
df_last = pd.read_csv(os.path.join(OUT_DIR,"summary_last40.csv")).set_index("ticker")
df_last["regime_type"]=df_last.index.map(REGIME_MAP)
df_last.groupby("regime_type").mean(numeric_only=True).to_csv(os.path.join(OUT_DIR,"regime_matrix.csv"))

# 3) Grouped barplots
def grouped(df, metrics, title, fname):
    df=df.sort_values("ticker").set_index("ticker")
    tickers=df.index.tolist(); x=np.arange(len(tickers)); width=0.15
    fig,ax=plt.subplots(figsize=(10,5), dpi=120); cols=sns.color_palette("Set2", len(metrics))
    for i,m in enumerate(metrics):
        ax.bar(x+i*width, df[m], width, label=m.upper(), color=cols[i], edgecolor="black")
    ax.set_title(title); ax.set_xlabel("Ticker"); ax.set_ylabel("Metric Value")
    ax.set_xticks(x+width*(len(metrics)-1)/2); ax.set_xticklabels(tickers, rotation=45)
    ax.legend(); ax.spines[['top','right']].set_visible(False); ax.yaxis.grid(True, linestyle="--", alpha=0.5)
    plt.tight_layout(); plt.savefig(os.path.join(OUT_DIR,f"{fname}.png"), dpi=300, bbox_inches="tight"); plt.show()
grouped(pd.read_csv(os.path.join(OUT_DIR,"summary_last40.csv")), ["mae","rmse","mape"], "Error Metrics (LSTM)", "error_metrics_per_ticker")
grouped(pd.read_csv(os.path.join(OUT_DIR,"summary_last40.csv")), ["hit_rate","precision","recall","f1"], "Directional Metrics (LSTM)", "directional_metrics_per_ticker")

# 4) Rolling volatility + Pearson (test)
rows=[]
for t in TICKERS:
    p=os.path.join(OUT_DIR,t,f"{t}_last_40pct_multivar.csv")
    if not os.path.exists(p): continue
    df=pd.read_csv(p, parse_dates=["Date"])
    df["log_return"]=np.log(df["y_true"]).diff()
    df["rolling_volatility"]=df["log_return"].rolling(5).std()
    df["abs_error"]=(df["y_true"]-df["y_pred"]).abs()
    d=df.dropna(subset=["rolling_volatility","abs_error"])
    plt.figure(figsize=(8,6), dpi=120)
    plt.scatter(d["rolling_volatility"], d["abs_error"], alpha=0.6, edgecolor="black", label=t)
    plt.title(f"Volatility vs Error Magnitude for {t}")
    plt.xlabel("5-day Rolling Volatility (log return std)"); plt.ylabel("Absolute Prediction Error")
    plt.grid(True); plt.legend(); plt.tight_layout()
    plt.savefig(os.path.join(OUT_DIR,t,f"{t}_rolling_volatility_last40.png"), dpi=300, bbox_inches="tight"); plt.show()
    a=d["rolling_volatility"].values; b=d["abs_error"].values
    a=a-a.mean(); b=b-b.mean(); denom=(np.sqrt((a*a).sum())*np.sqrt((b*b).sum()))
    r=float((a*b).sum()/denom) if denom!=0 else np.nan
    rows.append([t,r,len(d)])
pd.DataFrame(rows, columns=["ticker","pearson_corr","n_points"]).to_csv(os.path.join(OUT_DIR,"vol_error_correlation_last40.csv"), index=False)

# 5) Econ metrics (test)
def econ_metrics(df):
    df=df.copy()
    df["log_return"]=np.log(df["y_true"]).diff()
    df["pred_log_return"]=np.log(df["y_pred"]).diff()
    df["strategy_return"]=df["pred_log_return"].shift(1)*np.sign(df["log_return"])
    df["cum_return"]=df["strategy_return"].cumsum()
    sharpe=df["strategy_return"].mean()/df["strategy_return"].std()
    cum_return=df["cum_return"].iloc[-1]
    max_dd=(df["cum_return"].cummax()-df["cum_return"]).max()
    return {"Sharpe":sharpe,"cum_return":cum_return,"max_drawdown":max_dd,"cum_series":df["cum_return"].reset_index(drop=True)}
econ_rows, series = [], {}
for t in TICKERS:
    p=os.path.join(OUT_DIR,t,f"{t}_last_40pct_multivar.csv")
    if not os.path.exists(p): continue
    em=econ_metrics(pd.read_csv(p)); em["ticker"]=t
    econ_rows.append(em); series[t]=em["cum_series"]
econ_df=pd.DataFrame(econ_rows).set_index("ticker")
econ_df.drop(columns=["cum_series"]).to_csv(os.path.join(OUT_DIR,"econ_metrics_last40.csv"))
# barplots
x=np.arange(len(econ_df)); width=0.6
fig,(ax1,ax2,ax3)=plt.subplots(1,3, figsize=(18,4), dpi=120)
b=ax1.bar(x, econ_df["Sharpe"], width, alpha=0.85)
for r in b:
    h=r.get_height()
    if not np.isnan(h): ax1.text(r.get_x()+r.get_width()/2, h+(0.002 if h>=0 else -0.004), f"{h:.2f}", ha="center", va="bottom" if h>=0 else "top", fontsize=9)
ax1.set_xticks(x); ax1.set_xticklabels(econ_df.index, rotation=30, ha="right", fontsize=9)
ax1.set_title("Sharpe Ratio by Regime"); ax1.set_ylabel("Sharpe Ratio"); ax1.grid(axis="y", linestyle="--", alpha=0.7); [ax1.spines[s].set_visible(False) for s in ["top","right"]]
b=ax2.bar(x, econ_df["cum_return"], width, alpha=0.85, color="yellow")
for r in b:
    h=r.get_height()
    if not np.isnan(h): ax2.text(r.get_x()+r.get_width()/2, h+(0.01 if h>=0 else -0.02), f"{h:.2%}", ha="center", va="bottom" if h>=0 else "top", fontsize=9)
ax2.set_xticks(x); ax2.set_xticklabels(econ_df.index, rotation=30, ha="right", fontsize=9)
ax2.set_title("Total Return by Regime"); ax2.set_ylabel("Cumulative Return"); ax2.grid(axis="y", linestyle="--", alpha=0.7); [ax2.spines[s].set_visible(False) for s in ["top","right"]]
b=ax3.bar(x, econ_df["max_drawdown"], width, alpha=0.85, color="tomato")
for r in b:
    h=r.get_height()
    if not np.isnan(h): ax3.text(r.get_x()+r.get_width()/2, h+0.01, f"{h:.2%}", ha="center", va="bottom", fontsize=9)
ax3.set_xticks(x); ax3.set_xticklabels(econ_df.index, rotation=30, ha="right", fontsize=9)
ax3.set_title("Max Drawdown by Regime"); ax3.set_ylabel("Max Drawdown"); ax3.grid(axis="y", linestyle="--", alpha=0.7); [ax3.spines[s].set_visible(False) for s in ["top","right"]]
plt.tight_layout(); plt.savefig(os.path.join(OUT_DIR,"econ_barplots_last40.png"), dpi=300, bbox_inches="tight"); plt.show()
# cumulative
plt.figure(figsize=(10,5))
for sym,ser in series.items(): plt.plot(np.arange(len(ser)), ser.values, label=sym)
plt.title("Strategy Cumulative Returns"); plt.xlabel("Timestep"); plt.ylabel("Cumulative Return")
plt.grid(True, linestyle="--", alpha=0.7); plt.gca().yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
plt.legend(title="Ticker", fontsize=9, title_fontsize=10, bbox_to_anchor=(1.05,1), loc="upper left")
plt.tight_layout(); plt.savefig(os.path.join(OUT_DIR,"strategy_cumulative_returns_last40.png"), dpi=300, bbox_inches="tight"); plt.show()

# 6) Wilcoxon + Heatmaps vs FinalModel (positive = FinalModel better)
def load_final_summary():

    cand = os.path.join(FINALMODEL_DIR, "summary_last40.csv")
    if os.path.exists(cand):
        df = pd.read_csv(cand)
        # ensure numeric & finite
        for c in ["mae","rmse","mape","hit_rate","precision","recall","f1"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        return df

    rows = []
    for t in TICKERS:
        p1 = os.path.join(FINALMODEL_DIR, f"{t}_final_results.csv")
        p2 = os.path.join(FINALMODEL_DIR, f"{t}.csv")  # fallback name, if any
        path = p1 if os.path.exists(p1) else (p2 if os.path.exists(p2) else None)
        if path is None:
            continue

        df = pd.read_csv(path)
        # try common column names
        if {"y_true","y_pred"}.issubset(df.columns):
            yt = df["y_true"].astype(float).values
            yp = df["y_pred"].astype(float).values
        elif {"actual","pred"}.issubset(df.columns):
            yt = df["actual"].astype(float).values
            yp = df["pred"].astype(float).values
        else:
            # try to guess / skip if not found
            continue

        # drop any rows with NaN/Inf pairs
        mask = np.isfinite(yt) & np.isfinite(yp)
        yt = yt[mask]
        yp = yp[mask]

        m = evaluate(yt, yp)
        rows.append([t, *m])

    return pd.DataFrame(rows, columns=["ticker","mae","rmse","mape","hit_rate","precision","recall","f1"])


df_l = pd.read_csv(os.path.join(OUT_DIR,"summary_last40.csv")).set_index("ticker")
df_f = load_final_summary().set_index("ticker")
common = df_l.index.intersection(df_f.index); df_l=df_l.loc[common]; df_f=df_f.loc[common]

metrics=["mae","rmse","mape","hit_rate","precision","recall","f1"]
pvals={m: wilcoxon(df_f[m], df_l[m]).pvalue for m in metrics}
with open(os.path.join(OUT_DIR,"wilcoxon_final_vs_lstm.txt"),"w") as f:
    f.write("Wilcoxon p-values (FinalModel vs LSTM)\n")
    for k,v in pvals.items(): f.write(f"{k}: {v:.6f}\n")
print("Wilcoxon (FinalModel vs LSTM):", pvals)

# Errors: improvement = LSTM - FinalModel (positive => FinalModel better since errors lower)
err_cols=["mae","rmse","mape"]
diff_err=df_l[err_cols]-df_f[err_cols]
plt.figure(figsize=(6,4))
sns.heatmap(diff_err, annot=True, fmt=".2f", cmap="Blues", vmin=0, linewidths=0.5,
            cbar_kws={"label":"Improvement (LSTM − FinalModel)"})
plt.title("Error Improvement (LSTM − FinalModel)"); plt.ylabel("Ticker")
plt.tight_layout(); plt.savefig(os.path.join(OUT_DIR,"heatmap_error_improvement_LSTM_minus_Final.png"), dpi=300, bbox_inches="tight"); plt.show()

# Directional: higher better → delta = FinalModel - LSTM (positive => FinalModel better)
dir_cols=["hit_rate","precision","recall","f1"]
diff_dir=df_f[dir_cols]-df_l[dir_cols]
plt.figure(figsize=(7,4))
sns.heatmap(diff_dir, annot=True, fmt=".3f", cmap="RdBu", center=0, linewidths=0.5)
plt.title("Directional Delta (FinalModel − LSTM)"); plt.ylabel("Ticker")
plt.tight_layout(); plt.savefig(os.path.join(OUT_DIR,"heatmap_directional_delta_Final_minus_LSTM.png"), dpi=300, bbox_inches="tight"); plt.show()

print("\nAll outputs written to ./lstm_result/")
