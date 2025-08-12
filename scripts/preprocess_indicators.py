#!/usr/bin/env python3
import os, yaml
import pandas as pd
import numpy as np

# NumPy 2.x compatibility: pandas_ta imports `NaN` symbol removed in NumPy 2
if not hasattr(np, "NaN"):
    np.NaN = np.nan  # type: ignore[attr-defined]

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

def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    # Ensure standard OHLC columns: Open, High, Low, Close, Volume
    rename_map = {}
    for c in df.columns:
        cl = str(c).lower()
        if "open" in cl: rename_map[c] = "open"
        if "high" in cl: rename_map[c] = "high"
        if "low" in cl: rename_map[c] = "low"
        if "close" in cl: rename_map[c] = "close"
        if "volume" in cl: rename_map[c] = "volume"
    df = df.rename(columns=rename_map)
    # Drop duplicate columns after renaming (keep first)
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()]

    # Keep only known columns
    keep_cols = [c for c in ["open","high","low","close","volume"] if c in df.columns]
    df = df[keep_cols]

    # Coerce numeric types
    for c in ["open","high","low","close","volume"]:
        if c in df.columns:
            series_or_df = df[c]
            if isinstance(series_or_df, pd.DataFrame):
                series_or_df = series_or_df.iloc[:, 0]
            df[c] = pd.to_numeric(series_or_df, errors='coerce')

    # Drop rows missing OHLC
    df = df.dropna(subset=[col for col in ["open","high","low","close"] if col in df.columns])

    # Basic indicators
    df['rsi_14'] = ta.rsi(df['close'], length=14)

    ema12 = ta.ema(df['close'], length=12)
    ema26 = ta.ema(df['close'], length=26)
    if ema12 is None:
        ema12 = _ema(df['close'], 12)
    if ema26 is None:
        ema26 = _ema(df['close'], 26)
    df['ema12'] = ema12
    df['ema26'] = ema26

    macd_df = ta.macd(df['close'])
    if macd_df is not None and not macd_df.empty:
        # Use first two columns as MACD and signal to avoid relying on exact names
        df['macd'] = macd_df.iloc[:, 0]
        if macd_df.shape[1] > 1:
            df['macd_signal'] = macd_df.iloc[:, 1]
        else:
            # Fallback signal EMA(9) of MACD line
            df['macd_signal'] = _ema(df['macd'], 9)
    else:
        # Manual MACD
        macd_line = df['ema12'] - df['ema26']
        signal_line = _ema(macd_line, 9)
        df['macd'] = macd_line
        df['macd_signal'] = signal_line

    bb = ta.bbands(df['close'])
    if bb is not None and not bb.empty:
        # Detect columns by position to avoid name differences
        # Typically: [lower, mid, upper] or named BBL, BBM, BBU
        lower = bb.iloc[:, 0]
        upper = bb.iloc[:, -1]
        df['bb_upper'] = upper
        df['bb_lower'] = lower
    else:
        # Fallback Bollinger Bands 20, 2.0
        rolling_mean = df['close'].rolling(window=20).mean()
        rolling_std = df['close'].rolling(window=20).std()
        df['bb_upper'] = rolling_mean + 2.0 * rolling_std
        df['bb_lower'] = rolling_mean - 2.0 * rolling_std

    denom = (df['bb_upper'] - df['bb_lower'])
    df['bb_percent'] = (df['close'] - df['bb_lower']) / denom.replace(0, np.nan)

    # ATR
    atr = ta.atr(df['high'], df['low'], df['close'], length=14)
    if atr is None or (hasattr(atr, 'empty') and atr.empty):
        tr1 = (df['high'] - df['low']).abs()
        tr2 = (df['high'] - df['close'].shift(1)).abs()
        tr3 = (df['low'] - df['close'].shift(1)).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(window=14).mean()
    df['atr_14'] = atr

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
        intraday_path = os.path.join(IN_DIR, f"{pair}_intraday_{CONF['general']['intraday_interval']}m.csv")
        if os.path.exists(intraday_path):
            df2 = pd.read_csv(intraday_path, index_col=0, parse_dates=True)
            df2_proc = add_indicators(df2)
            out_path2 = os.path.join(OUT_DIR, f"{pair}_intraday_processed_{CONF['general']['intraday_interval']}m.csv")
            df2_proc.to_csv(out_path2)
            print("Intraday processed saved:", out_path2)

if __name__ == "__main__":
    main()
