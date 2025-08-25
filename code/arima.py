import os, math, warnings
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from sklearn.metrics import mean_squared_error, mean_absolute_error, precision_score, recall_score, f1_score
from statsmodels.tsa.stattools import adfuller
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
from statsmodels.tsa.statespace.sarimax import SARIMAX
from statsmodels.tools.sm_exceptions import ConvergenceWarning

import os, numpy as np, pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import pearsonr

BASE_DATA_DIR = './final_data'  
TICKERS       = ["AAPL","TSLA","XOM","SPY","JNJ","AMD","PG"]

DATE_COL       = 'date'
TARGET_COL     = 'close'
FORECAST_STEPS = 30
SEARCH_P       = range(0, 3)  
SEARCH_Q       = range(0, 3)  
TRAIN_SPLIT    = 0.8 #diagnostics         
SAVE_PLOTS     = True

ROOT_OUTDIR = './arima_output'
os.makedirs(ROOT_OUTDIR, exist_ok=True)
warnings.filterwarnings("ignore", category=ConvergenceWarning)

def load_series(csv_path, date_col, target_col):
    df = pd.read_csv(csv_path)
    df.columns = df.columns.str.strip().str.lower()
    df[date_col.lower()] = pd.to_datetime(df[date_col.lower()], errors='raise')
    df = df.set_index(date_col.lower()).sort_index()
    y = df[target_col.lower()].astype('float64')
    print("Data range:", y.index.min().date(), "→", y.index.max().date())
    print(y.head())
    return y

def adf_test(series, label=''):
    res = adfuller(series.dropna(), autolag='AIC')
    print(f"\nADF Test: {label}")
    print(f"ADF stat: {res[0]:.4f} | p-value: {res[1]:.4g} | lags: {res[2]} | nobs: {res[3]}")
    for k, v in res[4].items():
        print(f"  Critical ({k}): {v:.4f}")
    print("Stationary " if res[1] <= 0.05 else "Not stationary ")
    return res[1]

def make_stationary(y, max_d=3):
    d = 0
    p = adf_test(y, 'Original')
    yd = y.copy()
    while p > 0.05 and d < max_d:
        d += 1
        yd = yd.diff().dropna()
        p = adf_test(yd, f'Differenced (d={d})')
    print(f"\nSuggested differencing order: d = {d}")
    return yd, d

def plot_acf_pacf(series, outdir, fname='acf_pacf.png'):
    fig = plt.figure(figsize=(12,5))
    ax1 = fig.add_subplot(1,2,1)
    plot_acf(series, ax=ax1, lags=30)
    ax1.set_title('ACF (after differencing)')
    ax2 = fig.add_subplot(1,2,2)
    plot_pacf(series, ax=ax2, lags=30, method='ywm')
    ax2.set_title('PACF (after differencing)')
    plt.tight_layout()
    path = os.path.join(outdir, fname)
    if SAVE_PLOTS:
        plt.savefig(path, dpi=150)
    plt.show()
    plt.close()
    print(f"Saved ACF/PACF to {path}")

def fit_arima(y, order, trend='n'):
    model = SARIMAX(y, order=order, trend=trend,
                    enforce_stationarity=False, enforce_invertibility=False)
    return model.fit(disp=False)

def plot_diagnostics(res, outdir, fname='diagnostics.png'):
    res.plot_diagnostics(figsize=(12,8))
    plt.tight_layout()
    path = os.path.join(outdir, fname)
    if SAVE_PLOTS:
        plt.savefig(path, dpi=150)
    plt.show()
    plt.close()
    print(f"Saved diagnostics to {path}")

def forecast_plot(y, res, steps, outdir, fname='forecast_plot.png', title='Forecast'):
    fc = res.get_forecast(steps=steps)
    fc_mean = fc.predicted_mean
    fc_ci = fc.conf_int()
    plt.figure(figsize=(12,5))
    y.tail(250).plot(label='history')
    fc_mean.plot(label='forecast')
    plt.fill_between(fc_ci.index, fc_ci.iloc[:,0], fc_ci.iloc[:,1], alpha=0.2)
    plt.legend()
    plt.title(title)
    plt.tight_layout()
    path = os.path.join(outdir, fname)
    if SAVE_PLOTS:
        plt.savefig(path, dpi=150)
    plt.show()
    plt.close()
    print(f"Saved forecast plot to {path}")
    fdf = pd.DataFrame({'forecast': fc_mean, 'lower': fc_ci.iloc[:,0], 'upper': fc_ci.iloc[:,1]})
    fcsv = os.path.join(outdir, 'forecast.csv')
    fdf.to_csv(fcsv)
    print(f"Saved {steps}-step forecast to {fcsv}")

def metrics(y_true, y_pred):
    y_true, y_pred = np.asarray(y_true), np.asarray(y_pred)
    rmse = math.sqrt(mean_squared_error(y_true, y_pred))
    mae  = mean_absolute_error(y_true, y_pred)
    mape = float(np.mean(np.abs((y_true - y_pred) / y_true)) * 100)
    actual_dir = (np.diff(y_true) > 0)
    pred_dir   = (np.diff(y_pred) > 0)
    hitrate    = float(np.mean(actual_dir == pred_dir) * 100)
    precision  = precision_score(actual_dir, pred_dir, zero_division=0)
    recall     = recall_score(actual_dir, pred_dir, zero_division=0)
    f1         = f1_score(actual_dir, pred_dir, zero_division=0)
    return {'RMSE': rmse, 'MAE': mae, 'MAPE (%)': mape,
            'HitRate (%)': hitrate, 'Precision': precision, 'Recall': recall, 'F1': f1}

def walk_forward(y, order, trend='n', split=0.8):
    n_train = int(len(y) * split)
    train, test = y.iloc[:n_train], y.iloc[n_train:]
    history = list(train)
    preds = []
    for t in range(len(test)):
        res = fit_arima(history, order, trend)
        preds.append(res.forecast(steps=1)[0])
        history.append(test.iloc[t])
    m = metrics(test.values, preds)
    return train, test, np.array(preds), m

def grid_search_walk_forward(y, d, p_values, q_values, trend='n', split=0.8, outdir='.'):
    rows = []
    for p in p_values:
        for q in q_values:
            order = (p, d, q)
            try:
                _, test, preds, m = walk_forward(y, order, trend, split)
                rows.append({'p': p, 'd': d, 'q': q, **m})
                print(f"ARIMA{order} -> RMSE {m['RMSE']:.4f}, MAPE {m['MAPE (%)']:.2f}%, "
                      f"Hit {m['HitRate (%)']:.2f}%, Prec {m['Precision']:.2f}, "
                      f"Rec {m['Recall']:.2f}, F1 {m['F1']:.2f}")
            except Exception as e:
                print(f"ARIMA{order} failed: {e}")
    df = pd.DataFrame(rows).sort_values('RMSE')
    path = os.path.join(outdir, 'walk_forward_results.csv')
    df.to_csv(path, index=False)
    print(f"\nSaved model comparison to {path}")
    return df

def plot_backtest(test_index, y_true, y_pred, title, outdir, fname):
    plt.figure(figsize=(10, 4))
    plt.plot(test_index, y_true, label='y_true', color='crimson')
    plt.plot(test_index, y_pred, label='y_pred', color='green')
    plt.title(title, fontsize=13, weight='bold')
    plt.xlabel("Date")
    plt.ylabel("Close Price")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    path = os.path.join(outdir, fname)
    if SAVE_PLOTS:
        plt.savefig(path, dpi=300, bbox_inches='tight')
    plt.show()
    plt.close()
    print(f"Saved backtest plot to {path}")

# --------------------------- PER-TICKER PIPELINE ---------------------------
def process_ticker(ticker: str):
    csv_path = os.path.join(BASE_DATA_DIR, f'{ticker}.csv')
    outdir   = os.path.join(ROOT_OUTDIR, ticker)
    os.makedirs(outdir, exist_ok=True)
    print("\n" + "="*80)
    print(f"Ticker: {ticker} | CSV: {csv_path} | OUT: {outdir}")
    print("="*80)

    # 1) Load
    y = load_series(csv_path, DATE_COL, TARGET_COL)

    # 2) Stationarity & ACF/PACF
    yd, d = make_stationary(y, max_d=3)
    plot_acf_pacf(yd, outdir)

    # 3) Baseline ARIMA(0,d,0) with optional drift
    use_drift = (abs(yd.mean()) > 1e-6)
    trend = 'c' if (use_drift and d > 0) else 'n'
    print(f"\nFitting baseline ARIMA(0,{d},0) with trend='{trend}' (drift={'yes' if trend=='c' else 'no'})")
    res = fit_arima(y, (0, d, 0), trend)
    print(res.summary())
    plot_diagnostics(res, outdir)

    # 4) Forecast
    forecast_plot(y, res, FORECAST_STEPS, outdir,
                  title=f"{ticker} — ARIMA(0,{d},0) {'with drift' if trend=='c' else ''} forecast")

    # 5) Walk-forward grid on small (p,q)
    results_df = grid_search_walk_forward(y, d, SEARCH_P, SEARCH_Q, trend, TRAIN_SPLIT, outdir)
    best = results_df.iloc[0].to_dict()

    # 6) Backtest plot for best (walk-forward predictions already computed inside grid;
    #    recompute once to get the actual preds and plot them)
    order_best = (int(best['p']), int(best['d']), int(best['q']))
    _, test, preds, _ = walk_forward(y, order_best, trend, TRAIN_SPLIT)
    plot_backtest(test.index, test.values, preds,
                  title=f"{ticker} — Walk-forward ARIMA{order_best}",
                  outdir=outdir, fname='backtest_best.png')

    best.update({
        'ticker': ticker,
        'order': f"{order_best}",
        'trend': trend
    })
    pd.DataFrame([best]).to_csv(os.path.join(outdir, 'best_model_summary.csv'), index=False)
    return best

# --------------------------- RUN ALL TICKERS ---------------------------
all_summaries = []
for tk in TICKERS:
    try:
        best = process_ticker(tk)
        all_summaries.append(best)
    except FileNotFoundError:
        print(f"!! Skipping {tk}: {os.path.join(BASE_DATA_DIR, tk + '.csv')} not found")
    except Exception as e:
        print(f"!! Error on {tk}: {e}")

if all_summaries:
    summary_df = pd.DataFrame(all_summaries)[
        ['ticker','order','trend','RMSE','MAE','MAPE (%)','HitRate (%)','Precision','Recall','F1','p','d','q']
    ]
    summary_path = os.path.join(ROOT_OUTDIR, '_summary.csv')
    summary_df.to_csv(summary_path, index=False)
    print(f"\nWrote summary for {len(summary_df)} tickers to {summary_path}")
else:
    print("\nNo summaries produced.")


#  error dist + tables + regime table + rolling vol + Pearson 


ROOT_OUTDIR = './arima_output'
tickers = ["AAPL", "TSLA", "XOM", "SPY", "JNJ", "AMD", "PG"]

event_titles = [
    "AAPL – Crash & Rebound (2020-03-10)",
    "TSLA – High-Beta Cooling (2021-01-15)",
    "XOM – Oil Cycle Peak (2022-06-01)",
    "SPY – Drawdown Chop (2022-09-15)",
    "JNJ – Low-Volatility Stretch (2019-08-01)",
    "AMD – Tech Selloff (2018-10-10)",
    "PG – Macro-Irrelevant Calm (2015-06-15)"
]

starts = ["2019-09-02", "2020-07-01", "2021-10-01", "2022-01-03", "2018-11-01", "2018-04-02", "2014-12-01"]
ends   = ["2020-06-01", "2021-06-30", "2022-09-30", "2022-12-30", "2020-01-01", "2019-03-29", "2016-01-01"]

regime_map = {
    "TSLA": "High-Volatility",
    "AMD": "High-Volatility",
    "SPY": "Sideways/Chop",
    "PG": "Low-Volatility",
    "JNJ": "Low-Volatility",
    "AAPL": "Post-Trend Reversal",
    "XOM": "Post-Trend Reversal"
}

# ---------- 1) Error distribution plot ----------
all_errors = []
for tk in tickers:
    path = os.path.join(ROOT_OUTDIR, tk, 'test40_predictions.csv')
    df = pd.read_csv(path)
    err = (df['actual'] - df['pred']).dropna().values
    all_errors.extend(err)

plt.figure(figsize=(9, 5), dpi=120)
sns.set(style="whitegrid")
sns.histplot(all_errors, bins=50, kde=True, color="darkorange", edgecolor='black', alpha=0.7)
plt.title("Prediction Error Distribution – ARIMA", fontsize=14, weight='bold')
plt.xlabel("Prediction Error (y_true - y_pred)", fontsize=12)
plt.ylabel("Density", fontsize=12)
plt.grid(True, linestyle='--', alpha=0.6)
plt.tight_layout()
plt.savefig(os.path.join(ROOT_OUTDIR, 'error_dist_ARIMA.png'), dpi=300, bbox_inches='tight')
plt.show()

test_metrics_path  = os.path.join(ROOT_OUTDIR, 'test40_metrics_all.csv')
event_metrics_path = os.path.join(ROOT_OUTDIR, 'event_metrics_all.csv')

test_df  = pd.read_csv(test_metrics_path)
event_df = pd.read_csv(event_metrics_path)

test_df = test_df.sort_values('ticker').reset_index(drop=True)
event_df = event_df.sort_values('ticker').reset_index(drop=True)

print("\n=== Test set metrics (last 40%) ===")
print(test_df.to_string(index=False))

print("\n=== Event window metrics ===")
print(event_df.to_string(index=False))

# ---------- 5) Metrics by regime type (from test set) ----------
tmp = test_df.copy()
tmp['regime_type'] = tmp['ticker'].map(regime_map)
regime_table = (tmp.groupby('regime_type')
                  .mean(numeric_only=True)
                  .loc[:, ['mae','rmse','mape','hit_rate','precision','recall','f1']]
                  .sort_index())

print("\n=== Test metrics by regime type ===")
print(regime_table.to_string())

regime_table.to_csv(os.path.join(ROOT_OUTDIR, 'test_metrics_by_regime.csv'))

# ---------- 6) Rolling volatility plots per ticker + Pearson ----------
pear = []
pear_txt_lines = ["Pearson Correlation (Volatility vs Error) – ARIMA\n"]
for tk in tickers:
    path = os.path.join(ROOT_OUTDIR, tk, 'test40_predictions.csv')
    df = pd.read_csv(path)
    # log returns on y_true (actual)
    df['log_return'] = np.log(df['actual']).diff()
    df['rolling_volatility'] = df['log_return'].rolling(window=5).std()
    df['abs_error'] = np.abs(df['actual'] - df['pred'])
    df_clean = df.dropna(subset=['rolling_volatility','abs_error'])

    
    plt.figure(figsize=(6, 4), dpi=120)
    plt.scatter(df_clean['rolling_volatility'], df_clean['abs_error'],
                alpha=0.6, color='slateblue', edgecolor='black')
    plt.title(f"Volatility vs Error Magnitude for {tk}")
    plt.xlabel("5-day Rolling Volatility (log return std)")
    plt.ylabel("Absolute Prediction Error")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(os.path.join(ROOT_OUTDIR, tk, f'{tk}_rolling_volatility.png'),
                dpi=300, bbox_inches='tight')
    plt.show()

    corr, pval = pearsonr(df_clean['rolling_volatility'], df_clean['abs_error'])
    pear.append(corr)
    line = f"{tk}: {corr:.4f}"
    print(line)
    pear_txt_lines.append(line)

# Save Pearson summary
with open(os.path.join(ROOT_OUTDIR, 'pearson_correlations_ARIMA.txt'), 'w') as f:
    f.write("\n".join(pear_txt_lines))

ROOT_OUTDIR = './arima_output'

# Load test set metrics
test_df = pd.read_csv(f"{ROOT_OUTDIR}/test40_metrics_all.csv")
test_df = test_df.sort_values('ticker').reset_index(drop=True)
df = test_df.set_index('ticker')

def plot_grouped_metrics(df, metrics, colors, title_suffix, filename):
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
    plt.savefig(f"{ROOT_OUTDIR}/{filename}.png", dpi=300, bbox_inches='tight')
    plt.show()

# 1) Error metrics
error_metrics = ["mae", "rmse", "mape"]
error_colors = sns.color_palette("Set2", len(error_metrics))
plot_grouped_metrics(df, error_metrics, error_colors, "Error Metrics", "error_metrics_per_ticker")

# 2) Directional metrics
directional_metrics = ["hit_rate", "precision", "recall", "f1"]
directional_colors = sns.color_palette("Set2", len(directional_metrics))
plot_grouped_metrics(df, directional_metrics, directional_colors, "Directional Metrics", "directional_metrics_per_ticker")


# ===== Multi-ticker: test 40% + event windows using best models from _summary.csv =====
# Helpers for these two evaluation modes
def walk_forward_by_index(y, order, trend, train_end_idx, eval_start_idx, eval_end_idx):
    n = len(y)
    train_end_idx = max(1, min(train_end_idx, n-2))
    eval_start_idx = max(train_end_idx+1, min(eval_start_idx, n-1))
    eval_end_idx   = max(eval_start_idx, min(eval_end_idx, n-1))

    history = list(y.iloc[:train_end_idx])  # seed history
    preds, actuals, idxs = [], [], []

    for t in range(train_end_idx, eval_end_idx + 1):
        res = fit_arima(history, order, trend)
        yhat = res.forecast(steps=1)[0]
        if t >= eval_start_idx:
            preds.append(yhat)
            actuals.append(y.iloc[t])
            idxs.append(y.index[t])
        history.append(y.iloc[t])

    m = metrics(np.array(actuals), np.array(preds))
    return pd.Index(idxs), np.array(actuals), np.array(preds), m

def walk_forward_by_dates(y, order, trend, eval_start_date, eval_end_date):
    if isinstance(eval_start_date, str):
        eval_start_date = pd.to_datetime(eval_start_date)
    if isinstance(eval_end_date, str):
        eval_end_date = pd.to_datetime(eval_end_date)

    train_mask = y.index < eval_start_date
    test_mask  = (y.index >= eval_start_date) & (y.index <= eval_end_date)

    history = list(y.loc[train_mask].values)
    preds, actuals, idxs = [], [], []

    test_vals = y.loc[test_mask]
    for dt, true_val in test_vals.items():
        res = fit_arima(history, order, trend)
        yhat = res.forecast(steps=1)[0]
        preds.append(yhat)
        actuals.append(true_val)
        idxs.append(dt)
        history.append(true_val)

    m = metrics(np.array(actuals), np.array(preds))
    return pd.Index(idxs), np.array(actuals), np.array(preds), m


tickers = ["AAPL", "TSLA", "XOM", "SPY", "JNJ", "AMD", "PG"]

event_titles = [
    "AAPL – Crash & Rebound (2020-03-10)",
    "TSLA – High-Beta Cooling (2021-01-15)",
    "XOM – Oil Cycle Peak (2022-06-01)",
    "SPY – Drawdown Chop (2022-09-15)",
    "JNJ – Low-Volatility Stretch (2019-08-01)",
    "AMD – Tech Selloff (2018-10-10)",
    "PG – Macro-Irrelevant Calm (2015-06-15)"
]

starts = ["2019-09-02", "2020-07-01", "2021-10-01", "2022-01-03", "2018-11-01", "2018-04-02", "2014-12-01"]
ends   = ["2020-06-01", "2021-06-30", "2022-09-30", "2022-12-30", "2020-01-01", "2019-03-29", "2016-01-01"]

# ---------------- Load best models from summary ----------------
summary_path = os.path.join(ROOT_OUTDIR, '_summary.csv')
summary_df = pd.read_csv(summary_path)

def get_best_order_and_trend(row, y_series):
    p = row['p'] if 'p' in row else None
    d = row['d'] if 'd' in row else None
    q = row['q'] if 'q' in row else None
    trend = row['trend'] if 'trend' in row else 'n'
    if pd.isna(p) or pd.isna(d) or pd.isna(q):
        if 'order' in row and isinstance(row['order'], str) and row['order'].startswith('('):
            p, d, q = [int(x.strip()) for x in row['order'].strip('()').split(',')]
        else:
            _, d = make_stationary(y_series, max_d=3)
            p, q = 0, 0
    return (int(p), int(d), int(q)), str(trend)

test40_rows = []
event_rows  = []

for i, tk in enumerate(tickers):
    outdir = os.path.join(ROOT_OUTDIR, tk)
    csv_path = os.path.join(BASE_DATA_DIR, f'{tk}.csv')
    if not os.path.exists(csv_path):
        print(f"Skipping {tk}: {csv_path} not found")
        continue

    print("\n" + "-"*70)
    print(f"Evaluating {tk}")
    y = load_series(csv_path, DATE_COL, TARGET_COL)

    row = summary_df.loc[summary_df['ticker'].str.upper() == tk]
    if row.empty:
        _, d_tmp = make_stationary(y, max_d=3)
        order = (0, d_tmp, 0)
        trend = 'n'
    else:
        order, trend = get_best_order_and_trend(row.iloc[0], y)

    model_str = f"ARIMA{order}"
    print(f"Using {model_str} with trend='{trend}'")

    # ---------- Test: last 40% (train first 42%, start scoring at 60%) ----------
    n = len(y)
    train_end_idx  = int(n * 0.42)
    eval_start_idx = int(n * 0.60)
    eval_end_idx   = n - 1

    idxs_A, actuals_A, preds_A, m_A = walk_forward_by_index(
        y, order, trend, train_end_idx, eval_start_idx, eval_end_idx
    )

    plot_backtest(
        idxs_A, actuals_A, preds_A,
        title=f'{model_str} -> {tk} - test set',
        outdir=outdir, fname='test40_backtest.png'
    )
    pd.DataFrame({'date': idxs_A, 'actual': actuals_A, 'pred': preds_A}).to_csv(
        os.path.join(outdir, 'test40_predictions.csv'), index=False
    )

    test40_rows.append({
        'ticker': tk,
        'mae': m_A['MAE'],
        'rmse': m_A['RMSE'],
        'mape': m_A['MAPE (%)'],
        'hit_rate': m_A['HitRate (%)'] / 100.0,
        'precision': m_A['Precision'],
        'recall': m_A['Recall'],
        'f1': m_A['F1'],
    })

    # ---------- Event window ----------
    start_i, end_i, title_i = starts[i], ends[i], event_titles[i]
    idxs_B, actuals_B, preds_B, m_B = walk_forward_by_dates(
        y, order, trend, eval_start_date=start_i, eval_end_date=end_i
    )

    plot_backtest(
        idxs_B, actuals_B, preds_B,
        title=f'{model_str} -> {title_i}',
        outdir=outdir, fname='event_backtest.png'
    )
    pd.DataFrame({'date': idxs_B, 'actual': actuals_B, 'pred': preds_B}).to_csv(
        os.path.join(outdir, 'event_predictions.csv'), index=False
    )

    event_rows.append({
        'ticker': tk,
        'mae': m_B['MAE'],
        'rmse': m_B['RMSE'],
        'mape': m_B['MAPE (%)'],
        'hit_rate': m_B['HitRate (%)'] / 100.0,
        'precision': m_B['Precision'],
        'recall': m_B['Recall'],
        'f1': m_B['F1'],
    })

# ---------------- Write combined metrics ----------------
test40_df = pd.DataFrame(test40_rows)
event_df  = pd.DataFrame(event_rows)

test40_path = os.path.join(ROOT_OUTDIR, 'test40_metrics_all.csv')
event_path  = os.path.join(ROOT_OUTDIR, 'event_metrics_all.csv')

test40_df.to_csv(test40_path, index=False)
event_df.to_csv(event_path, index=False)

print(f"\nSaved test metrics to {test40_path}")
print(f"Saved event metrics to {event_path}")


os.makedirs("eval_results", exist_ok=True)

tickers = ["AAPL", "TSLA", "XOM", "SPY", "JNJ", "AMD", "PG"]

# Color/marker style
PALETTE = {"ARIMA": "slateblue", "FinalModel": "darkorange"}
MARKERS = {"ARIMA": "o", "FinalModel": "o"}
SCATTER_KW = dict(alpha=0.6, edgecolor="black", linewidths=0.5)

def load_arima_test(ticker):
    df = pd.read_csv(f'./arima_output/{ticker}/test40_predictions.csv')
    df = df.rename(columns={'actual':'y_true', 'pred':'y_pred'})
    df['date'] = pd.to_datetime(df['date'])
    return df[['date','y_true','y_pred']]

def load_finalmodel_test(ticker):
    df = pd.read_csv(f'./finalmodel/{ticker}_final_results.csv')
    if 'y_true' not in df.columns and 'actual' in df.columns:
        df = df.rename(columns={'actual':'y_true'})
    if 'y_pred' not in df.columns and 'pred' in df.columns:
        df = df.rename(columns={'pred':'y_pred'})
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
    else:
        df['date'] = pd.RangeIndex(start=0, stop=len(df))
    return df[['date','y_true','y_pred']]

pear_lines = ["Pearson Correlation (Volatility vs Error): ARIMA vs FinalModel\n"]

for tk in tickers:
    ar_df = load_arima_test(tk)
    fm_df = load_finalmodel_test(tk)

    merged = pd.merge(ar_df, fm_df, on='date', how='inner', suffixes=('_arima', '_final'))
    if merged.empty:
        print(f"[{tk}] No overlapping dates; skipping.")
        continue

    base = merged.rename(columns={'y_true_arima':'y_true'}).copy()
    if 'y_true_final' in base.columns:
        base['y_true'] = base['y_true'].fillna(base['y_true_final'])

    base = base[['date','y_true','y_pred_arima','y_pred_final']].sort_values('date')
    base['log_return'] = np.log(base['y_true']).diff()
    base['rolling_volatility'] = base['log_return'].rolling(window=5).std()
    base['abs_error_arima'] = np.abs(base['y_true'] - base['y_pred_arima'])
    base['abs_error_final'] = np.abs(base['y_true'] - base['y_pred_final'])
    clean = base.dropna(subset=['rolling_volatility','abs_error_arima','abs_error_final'])

    # --- Styled scatter
    plt.figure(figsize=(8, 6), dpi=120)
    plt.scatter(clean['rolling_volatility'], clean['abs_error_arima'],
                color=PALETTE["ARIMA"], marker=MARKERS["ARIMA"],
                label="ARIMA", **SCATTER_KW)
    plt.scatter(clean['rolling_volatility'], clean['abs_error_final'],
                color=PALETTE["FinalModel"], marker=MARKERS["FinalModel"],
                label="FinalModel", **SCATTER_KW)

    plt.title(f"Volatility vs Error Magnitude for {tk}")
    plt.xlabel("5-day Rolling Volatility (log return std)")
    plt.ylabel("Absolute Prediction Error")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.savefig(f'eval_results/{tk}_combined_rolling_volatility.png', dpi=300, bbox_inches='tight')
    plt.show()

    # --- Pearson correlations
    corr_arima, _ = pearsonr(clean['rolling_volatility'], clean['abs_error_arima'])
    corr_final, _ = pearsonr(clean['rolling_volatility'], clean['abs_error_final'])
    line = f"{tk}:  ARIMA={corr_arima:.4f}   FinalModel={corr_final:.4f}"
    print(line)
    pear_lines.append(line)

with open('eval_results/pearson_correlations_ARIMA_vs_FinalModel.txt', 'w') as f:
    f.write("\n".join(pear_lines))

# ===== ARIMA econ metrics + Wilcoxon vs FinalModel + heatmaps =====
import os, numpy as np, pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import seaborn as sns
from scipy.stats import wilcoxon

sns.set_style("whitegrid")
os.makedirs("eval_results", exist_ok=True)

TICKERS = ["AAPL","TSLA","XOM","SPY","JNJ","AMD","PG"]
ROOT_ARIMA = "./arima_output"

def econ_metrics(df):
    df = df.copy()
    # expects columns: y_true, y_pred
    df["log_return"] = np.log(df["y_true"]).diff()
    df["pred_log_return"] = np.log(df["y_pred"]).diff()
    # same definition you used: use previous step’s predicted log-return * sign(actual)
    df["strategy_return"] = df["pred_log_return"].shift(1) * np.sign(df["log_return"])
    df["cum_return"] = df["strategy_return"].cumsum()
    df["cum_series"] = df["cum_return"].copy()
    sr_denom = df["strategy_return"].std()
    sharpe = df["strategy_return"].mean() / sr_denom if sr_denom and not np.isclose(sr_denom, 0) else np.nan
    cum_return = df["cum_return"].iloc[-1]
    max_drawdown = (df["cum_return"].cummax() - df["cum_return"]).max()
    return {
        "Sharpe": sharpe,
        "cum_return": cum_return,
        "max_drawdown": max_drawdown,
        "cum_series": df["cum_series"]
    }

# ---------- 1) Compute econ metrics from ARIMA test predictions ----------
econ = []
series = {}
for tk in TICKERS:
    p = f"{ROOT_ARIMA}/{tk}/test40_predictions.csv"
    df = pd.read_csv(p)
    df = df.rename(columns={"actual":"y_true","pred":"y_pred"})
    df = df.dropna(subset=["y_true","y_pred"])
    em = econ_metrics(df)
    em["ticker"] = tk
    econ.append(em)
    series[tk] = em["cum_series"]

econ_df = pd.DataFrame(econ).set_index("ticker").drop(columns=["cum_series"])
econ_df.to_csv("eval_results/arima_econ_metrics.csv")
print("\n=== ARIMA econ metrics (test set) ===")
print(econ_df)

# ---------- 2) Bar plots (Sharpe, Cumulative Return, Max Drawdown) ----------
x = np.arange(len(TICKERS))
width = 0.6

fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(18, 4), dpi=120, sharey=False)

bars_sh = ax1.bar(x, econ_df["Sharpe"], width, alpha=0.85)
for r in bars_sh:
    h = r.get_height()
    if np.isnan(h): continue
    ax1.text(r.get_x()+r.get_width()/2, h + (0.002 if h>=0 else -0.004),
             f"{h:.2f}", ha="center", va="bottom" if h>=0 else "top", fontsize=9)
ax1.set_xticks(x); ax1.set_xticklabels(TICKERS, rotation=30, ha="right", fontsize=9)
ax1.set_title("Sharpe Ratio by Regime", fontsize=12, weight="bold", pad=10)
ax1.set_ylabel("Sharpe Ratio", fontsize=10); ax1.grid(axis="y", linestyle="--", linewidth=0.5, alpha=0.7)
ax1.spines[['top','right']].set_visible(False)

bars_cr = ax2.bar(x, econ_df["cum_return"], width, alpha=0.85, color='yellow')
for r in bars_cr:
    h = r.get_height()
    if np.isnan(h): continue
    ax2.text(r.get_x()+r.get_width()/2, h + (0.01 if h>=0 else -0.02),
             f"{h:.2%}", ha="center", va="bottom" if h>=0 else "top", fontsize=9)
ax2.set_xticks(x); ax2.set_xticklabels(TICKERS, rotation=30, ha="right", fontsize=9)
ax2.set_title("Total Return by Regime", fontsize=12, weight="bold", pad=10)
ax2.set_ylabel("Cumulative Return", fontsize=10); ax2.grid(axis="y", linestyle="--", linewidth=0.5, alpha=0.7)
ax2.spines[['top','right']].set_visible(False)

bars_md = ax3.bar(x, econ_df["max_drawdown"], width, alpha=0.85, color='tomato')
for r in bars_md:
    h = r.get_height()
    if np.isnan(h): continue
    ax3.text(r.get_x()+r.get_width()/2, h + 0.01, f"{h:.2%}", ha="center", va="bottom", fontsize=9)
ax3.set_xticks(x); ax3.set_xticklabels(TICKERS, rotation=30, ha="right", fontsize=9)
ax3.set_title("Max Drawdown by Regime", fontsize=12, weight="bold", pad=10)
ax3.set_ylabel("Max Drawdown", fontsize=10); ax3.grid(axis="y", linestyle="--", linewidth=0.5, alpha=0.7)
ax3.spines[['top','right']].set_visible(False)

fig.tight_layout()
plt.savefig("arima_output/arima_econ_barplots.png", dpi=300, bbox_inches='tight')
plt.show()

# ---------- 3) Combined cumulative-return curves ----------
plt.figure(figsize=(10,5))
for sym, ser in series.items():
    plt.plot(ser.index, ser.values, label=sym)
plt.title("Strategy Cumulative Returns (ARIMA)", fontsize=13, weight="bold", pad=10)
plt.xlabel("Timestep", fontsize=11); plt.ylabel("Cumulative Return", fontsize=11)
plt.grid(True, linestyle="--", linewidth=0.5, alpha=0.7)
plt.gca().yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
plt.legend(title="Ticker", fontsize=9, title_fontsize=10, bbox_to_anchor=(1.05,1), loc='upper left')
plt.tight_layout()
plt.savefig("arima_output/arima_strategy_cumulative_returns.png", dpi=300, bbox_inches='tight')
plt.show()

# ---------- 4) Wilcoxon paired test: ARIMA vs FinalModel metrics ----------
df_arima  = pd.read_csv(f"{ROOT_ARIMA}/test40_metrics_all.csv").set_index("ticker")
df_final  = pd.read_csv("./finalmodel/output.csv").set_index("ticker")

common = df_arima.index.intersection(df_final.index)
df_arima = df_arima.loc[common]
df_final = df_final.loc[common]

def wilc_compare(df_base, df_tuned, label="ARIMA_vs_FinalModel"):
    metrics = ["mae","rmse","mape","hit_rate","precision","recall","f1"]
    pvals = {}
    for m in metrics:
        stat, p = wilcoxon(df_base[m], df_tuned[m])
        pvals[m] = p
    print(f"\nWilcoxon p-values ({label}):")
    for k,v in pvals.items():
        print(f"  {k}: {v:.6f}")
    with open(f"eval_results/wilcoxon_{label.lower()}.txt","w") as f:
        f.write(f"Wilcoxon p-values ({label}):\n")
        for k,v in pvals.items():
            f.write(f"{k}: {v:.6f}\n")
    return pvals

_ = wilc_compare(df_arima, df_final, label="ARIMA_vs_FinalModel")

# ---------- 5) Heatmaps
# Errors: show improvement = ARIMA − FinalModel (positive means FinalModel better because errors are lower)
err_cols = ["mae","rmse","mape"]
diff_err = df_arima[err_cols] - df_final[err_cols]
diff_err.to_csv("arima_output/heatmap_error_improvement_ARIMA_minus_Final.csv")

plt.figure(figsize=(6,4))
sns.heatmap(diff_err, annot=True, fmt=".2f", cmap="RdBu_r", center=0, linewidths=0.5)
plt.title("Error Improvement (ARIMA − FinalModel)")
plt.ylabel("Ticker")
plt.tight_layout()
plt.savefig("arima_output/heatmap_error_improvement_ARIMA_minus_Final.png", dpi=300, bbox_inches='tight')
plt.show()

# Directional: higher is better, so show delta = FinalModel − ARIMA (positive means FinalModel better)
dir_cols = ["hit_rate","precision","recall","f1"]
diff_dir = df_final[dir_cols] - df_arima[dir_cols]
diff_dir.to_csv("arima_output/heatmap_directional_delta_Final_minus_ARIMA.csv")

plt.figure(figsize=(7,4))
sns.heatmap(diff_dir, annot=True, fmt=".3f", cmap="RdBu", center=0, linewidths=0.5)
plt.title("Directional Delta (ARIMA − FinalModel)")
plt.ylabel("Ticker")
plt.tight_layout()
plt.savefig("arima_output/heatmap_directional_delta_Final_minus_ARIMA.png", dpi=300, bbox_inches='tight')
plt.show()

