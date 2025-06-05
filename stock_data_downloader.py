import argparse
import json
import os
import time

import yfinance as yf
import pandas as pd
from pathlib import Path


def _download_from_yahoo(ticker, start, end, interval):    
    df = yf.download( ticker, start=start, end=end, interval=interval, progress=False, auto_adjust=False,)
    if df.empty:
        raise ValueError(f"No data returned from Yahoo Finance for {ticker}.")
    return df

def _standardise_columns(df):
    rename_map = {
        "open"     : "Open",
        "high"     : "High",
        "low"      : "Low",
        "close"    : "Close",
        "adj close": "Adj Close",
        "adj_close": "Adj Close",
        "volume"   : "Volume",
    }

    if isinstance(df.columns, pd.MultiIndex):
        flattened = []
        for col in df.columns:
            name: str = next((p for p in col if isinstance(p, str) and p.strip()), str(col[0]))
            flattened.append(name)
        df.columns = flattened

    df.columns = [rename_map.get(str(c).lower(), str(c)) for c in df.columns]
    return df

def download_stock_data(ticker, start_date, end_date, api_key, token, interval = "1d", source = "yahoo",):
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    
    if end < start:
        raise ValueError("end_date must be after start_date.")

    handlers = {"yahoo": lambda: _download_from_yahoo(ticker, start, end, interval)}
    if source not in handlers:
        raise ValueError(f"Unknown source '{source}'. Available: {list(handlers.keys())}.")

    df = handlers[source]()
    df = _standardise_columns(df)
    return df

def save_to_csv(df, filepath):
    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
    df.to_csv(filepath, float_format="%.6f", date_format="%Y-%m-%d")


def load_jobs(path):
    if path.suffix.lower() in {".json", ".jsonl"}:
        jobs = json.loads(path.read_text())
        if isinstance(jobs, dict):
            jobs = [jobs]
    else:
        raise ValueError("Unsupported file type; use .json")
    return jobs


def run_jobs(jobs, out_dir):
    for idx, spec in enumerate(jobs, 1):
        ticker = spec.get("ticker")
        try:
            df = download_stock_data(
                ticker     = ticker,
                start_date = spec["start_date"],
                end_date   = spec["end_date"],
                interval   = spec.get("interval", "1d"),
                source     = spec.get("source", "yahoo"),
                api_key    = spec.get("api_key"),
                token      = spec.get("token"),
            )
            out_name = spec.get("out") or f"{ticker}_{spec['start_date']}_{spec['end_date']}_{spec.get('source', 'yahoo')}.csv"
            save_to_csv(df, out_dir / out_name)
            print(f"[{idx}/{len(jobs)}] ✔️  Saved {len(df)} rows -> {out_name}")
            time.sleep(1)
        except Exception as exc:
            print(f"[{idx}/{len(jobs)}] ⚠️  Job failed for {ticker}: {exc}")


if __name__ == "__main__":  
    parser = argparse.ArgumentParser(description="Batch stock downloader from a JSON spec file.")
    parser.add_argument("spec_file", help="Path jobs.json")
    parser.add_argument("--out_dir", default="data", help="Directory to save CSVs (created if needed)")
    ns = parser.parse_args()

    jobs = load_jobs(Path(ns.spec_file))
    if not jobs:
        raise ValueError("Specification file contains no jobs.")


    out_dir = Path(ns.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    run_jobs(jobs, out_dir)
