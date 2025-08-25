import glob
import pathlib
import warnings
import pandas as pd
import numpy as np

RAW_DIR = pathlib.Path("./data")
OUT_DIR = pathlib.Path("./processed")
OUT_DIR.mkdir(exist_ok=True)

def load_raw(path):
    reader = pd.read_excel if path.suffix.lower() in {".xls", ".xlsx"} else pd.read_csv
    df = reader(path)
    if "Date" not in df.columns:
        raise ValueError(f"'Date' column missing in {path.name}")

    df["Date"] = pd.to_datetime(df["Date"])
    df = (df.set_index("Date")
            .sort_index()
            .loc[~df.index.duplicated(keep="first")])
    return df


def adjust_ohlc(df):
    factor = df["Adj Close"] / df["Close"]
    for col in ["Open", "High", "Low", "Close"]:
        df[col] = df[col] * factor

    df = df.rename(columns={
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume",
    })
    return df[["open", "high", "low", "close", "adj_close", "volume"]]


def reindex_and_fill(df, max_gap = 2):
    full_idx = pd.bdate_range(df.index.min(), df.index.max(), freq="C")
    df = df.reindex(full_idx)

    df["volume"] = df["volume"].replace(0, np.nan)

    df.ffill(limit=max_gap, inplace=True)
    df.bfill(limit=max_gap, inplace=True)

    core = ["open", "high", "low", "close", "adj_close", "volume"]
    df.dropna(subset=core, inplace=True)
    return df


def add_log(df):
    df["log_return_1d"] = np.log(df["adj_close"]).diff().astype("float32")

    float_cols = ["open", "high", "low", "close", "adj_close"]
    df[float_cols] = df[float_cols].astype("float32")
    df["volume"] = df["volume"].astype("int64")
    return df


def process_file(path):
    ticker = path.name.split("_")[0].upper()
    try:
        df = load_raw(path)
        df = adjust_ohlc(df)
        df = reindex_and_fill(df)
        df = add_log(df)

        out_path = OUT_DIR / f"{ticker}.csv"
        df.index.name = "date"

        df.to_csv(out_path,
                  index=True,
                  float_format="%.6f")
                  
        print(f"✅  {ticker}: saved -> {out_path}")
    except Exception as exc:
        warnings.warn(f"⚠️  {ticker}: {exc}")


def main():
    raw_files = glob.glob(str(RAW_DIR / "*_yahoo*"))
    if not raw_files:
        print("No raw files found in ./data/")
        return

    for f in sorted(raw_files):
        process_file(pathlib.Path(f))


if __name__ == "__main__":
    main()
