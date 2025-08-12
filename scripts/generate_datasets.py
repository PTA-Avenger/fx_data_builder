#!/usr/bin/env python3
import os, yaml, pandas as pd
import numpy as np
from datetime import datetime, timedelta
from tqdm import tqdm
import json

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
CONF_PATH = os.path.join(BASE_DIR, "config", "config.yaml")
with open(CONF_PATH) as f:
    CONF = yaml.safe_load(f)

PAIRS = CONF['general']['fx_pairs']
PROC_DIR = os.path.join(BASE_DIR, "data", "processed")
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
OUT_DIR = os.path.join(BASE_DIR, "data", "model_ready")
os.makedirs(OUT_DIR, exist_ok=True)

SEQ_LEN = 60  # e.g., 60 days or 60 hourly bars for LSTM
FORECAST_HORIZON = 1  # predict next bar direction for simplicity

def build_trend_dataset(pair_proc_path):
    df = pd.read_csv(pair_proc_path, index_col=0, parse_dates=True)
    # We'll produce sliding windows: X = seq_len rows of features, y = next return direction
    features = ['close','rsi_14','ema12','ema26','macd','atr_14','bb_percent','z_score_20','return_1']
    rows_X = []
    rows_y = []
    timestamps = []
    for i in range(SEQ_LEN, len(df)-FORECAST_HORIZON):
        window = df.iloc[i-SEQ_LEN:i]
        X = window[features].values  # shape (SEQ_LEN, n_feat)
        next_close = df['close'].iloc[i+FORECAST_HORIZON]
        cur_close = df['close'].iloc[i]
        ret = (next_close - cur_close) / cur_close
        label = 1 if ret > 0 else 0  # simple binary: up vs down/flat
        rows_X.append(X.flatten())  # flatten to one row (Watsonx can accept nested too if supported)
        rows_y.append(label)
        timestamps.append(df.index[i])
    X_arr = np.array(rows_X)
    y_arr = np.array(rows_y)
    cols = []
    nfeat = len(features)
    for t in range(SEQ_LEN):
        for f in features:
            cols.append(f"t{t}_{f}")
    df_out = pd.DataFrame(X_arr, columns=cols)
    df_out['label'] = y_arr
    df_out['timestamp'] = timestamps
    return df_out

def build_meanrev_dataset(pair_proc_path):
    df = pd.read_csv(pair_proc_path, index_col=0, parse_dates=True)
    # For mean reversion, create per-row features and a label whether price reverts in next N bars
    features = ['close','rsi_14','bb_percent','z_score_20','atr_14','return_1','return_5']
    X = df[features].copy()
    # label: next 5-bar return negative? The label logic can be tuned
    df['future_return_5'] = df['close'].shift(-5) / df['close'] - 1
    df['label'] = df['future_return_5'].apply(lambda x: 1 if x < 0 else 0)  # revert_down example
    X['label'] = df['label']
    X['timestamp'] = df.index
    X = X.dropna()
    return X

def build_sentiment_dataset(news_csv, pair_daily_proc_path):
    # Align news to price movement in next 1,2,5 days
    news = pd.read_csv(news_csv, parse_dates=['publishedAt'])
    df = pd.read_csv(pair_daily_proc_path, index_col=0, parse_dates=True)
    out_rows = []
    for _, row in news.iterrows():
        pair = row['pair']
        t = row['publishedAt']
        # find nearest price time >= publishedAt
        price_idx = df.index.searchsorted(pd.to_datetime(t))
        if price_idx >= len(df): 
            continue
        price_time = df.index[price_idx]
        # compute forward returns
        def forward_return(hours=24):
            # find index after 'hours' hours or days depending on timeframe
            # here use days for daily
            future_idx = price_idx + 1  # next bar
            if future_idx >= len(df):
                return None
            future_close = df['close'].iloc[future_idx]
            cur_close = df['close'].iloc[price_idx]
            return (future_close - cur_close) / cur_close
        f1 = forward_return()
        if f1 is None:
            continue
        out_rows.append({
            'pair': pair,
            'publishedAt': t,
            'title': row.get('title'),
            'description': row.get('description'),
            'url': row.get('url'),
            'forward_return_1': f1
        })
    res = pd.DataFrame(out_rows)
    # compute a label: up/down
    res['label'] = res['forward_return_1'].apply(lambda x: 1 if x>0 else 0)
    return res

def main():
    # Build per-pair datasets and then concat into master CSVs
    trend_frames = []
    mean_frames = []
    sentiment_files = [f for f in os.listdir(RAW_DIR) if f.startswith("news_")]
    news_file = os.path.join(RAW_DIR, sentiment_files[-1]) if sentiment_files else None

    for pair in PAIRS:
        proc_daily = os.path.join(PROC_DIR, f"{pair}_daily_processed.csv")
        if not os.path.exists(proc_daily):
            print("Processed daily missing for", pair)
            continue
        print("Building trend dataset for", pair)
        try:
            tdf = build_trend_dataset(proc_daily)
            tdf['pair'] = pair
            trend_frames.append(tdf)
        except Exception as e:
            print("Trend build error:", e)
        print("Building meanrev dataset for", pair)
        try:
            mdf = build_meanrev_dataset(proc_daily)
            mdf['pair'] = pair
            mean_frames.append(mdf)
        except Exception as e:
            print("Meanrev build error:", e)
        # sentiment
        if news_file:
            print("Building sentiment alignment for", pair)
            try:
                sdf = build_sentiment_dataset(news_file, proc_daily)
                s_out = os.path.join(OUT_DIR, f"{pair}_sentiment_aligned.csv")
                sdf.to_csv(s_out, index=False)
                print("Saved sentiment aligned for", pair, s_out)
            except Exception as e:
                print("Sentiment build error", e)

    if trend_frames:
        trend_all = pd.concat(trend_frames, ignore_index=True)
        trend_out = os.path.join(OUT_DIR, "trend_dataset.csv")
        trend_all.to_csv(trend_out, index=False)
        print("Saved trend dataset:", trend_out)
    if mean_frames:
        mean_all = pd.concat(mean_frames, ignore_index=True)
        mean_out = os.path.join(OUT_DIR, "meanrev_dataset.csv")
        mean_all.to_csv(mean_out, index=False)
        print("Saved meanrev dataset:", mean_out)

if __name__ == "__main__":
    main()
