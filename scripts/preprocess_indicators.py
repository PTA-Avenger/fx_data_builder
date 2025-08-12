#!/usr/bin/env python3
import os, yaml
import pandas as pd
import pandas_ta as ta
from tqdm import tqdm
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
CONF_PATH = os.path.join(BASE_DIR, "config", "config.yaml")
with open(CONF_PATH) as f:
    CONF = yaml.safe_load(f)

PAIRS = CONF['general']['fx_pairs']
IN_DIR = os.path.join(BASE_DIR, "data", "raw")
OUT_DIR = os.path.join(BASE_DIR, "data", "processed")
os.makedirs(OUT_DIR, exist_ok=True)

def add_indicators(df):
    # Ensure standard OHLC columns: Open, High, Low, Close, Volume
    cols = df.columns.str.lower()
    # rename common columns if needed
    rename_map = {}
    for c in df.columns:
        if "open" in c.lower(): rename_map[c] = "open"
        if "high" in c.lower(): rename_map[c] = "high"
        if "low" in c.lower(): rename_map[c] = "low"
        if "close" in c.lower(): rename_map[c] = "close"
        if "volume" in c.lower(): rename_map[c] = "volume"
    df = df.rename(columns=rename_map)
    df = df[['open','high','low','close']+([ 'volume'] if 'volume' in df.columns else [])]
    # Basic indicators
    df['rsi_14'] = ta.rsi(df['close'], length=14)
    ema12 = ta.ema(df['close'], length=12)
    ema26 = ta.ema(df['close'], length=26)
    df['ema12'] = ema12
    df['ema26'] = ema26
    df['macd'] = ta.macd(df['close'])['MACD_12_26_9']
    df['macd_signal'] = ta.macd(df['close'])['MACDs_12_26_9']
    bb = ta.bbands(df['close'])
    df['bb_upper'] = bb['BBU_20_2.0']
    df['bb_lower'] = bb['BBL_20_2.0']
    df['bb_percent'] = (df['close'] - df['bb_lower']) / (df['bb_upper'] - df['bb_lower'])
    df['atr_14'] = ta.atr(df['high'], df['low'], df['close'], length=14)
    # z-score vs rolling mean
    df['rolling_mean_20'] = df['close'].rolling(window=20).mean()
    df['rolling_std_20'] = df['close'].rolling(window=20).std()
    df['z_score_20'] = (df['close'] - df['rolling_mean_20']) / df['rolling_std_20']
    # returns
    df['return_1'] = df['close'].pct_change(1)
    df['return_5'] = df['close'].pct_change(5)
    df = df.dropna()
    return df

def main():
    for pair in PAIRS:
        daily_path = os.path.join(IN_DIR, f"{pair}_daily.csv")
        if not os.path.exists(daily_path):
            print("Daily not found:", daily_path)
            continue
        df = pd.read_csv(daily_path, index_col=0, parse_dates=True)
        df_proc = add_indicators(df)
        out_path = os.path.join(OUT_DIR, f"{pair}_daily_processed.csv")
        df_proc.to_csv(out_path)
        print("Processed saved:", out_path)
        # If intraday exists, also process intraday
        intraday_path = os.path.join(IN_DIR, f"{pair}_intraday_{CONF['general']['intraday_interval']}.csv")
        if os.path.exists(intraday_path):
            df2 = pd.read_csv(intraday_path, index_col=0, parse_dates=True)
            df2_proc = add_indicators(df2)
            out_path2 = os.path.join(OUT_DIR, f"{pair}_intraday_processed_{CONF['general']['intraday_interval']}.csv")
            df2_proc.to_csv(out_path2)
            print("Intraday processed saved:", out_path2)

if __name__ == "__main__":
    main()
