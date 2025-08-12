#!/usr/bin/env python3
"""
fetch_fx_data.py (Hybrid edition)

Fetches daily and intraday forex candles:
- Daily: Finnhub (full range), fallback to Yahoo Finance.
- Intraday: Finnhub (capped range for free tier), fallback to Alpha Vantage, then Yahoo Finance intraday.
"""

import os
import time
import yaml
import requests
import pandas as pd
from datetime import datetime, timedelta, UTC
from pathlib import Path
from tqdm import tqdm
from dotenv import load_dotenv
from typing import Optional

try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
except Exception:  # tenacity is optional; degrade gracefully
    def retry(*args, **kwargs):
        def deco(f):
            return f
        return deco
    def stop_after_attempt(n):
        return None
    def wait_exponential(*args, **kwargs):
        return None
    def retry_if_exception_type(*args, **kwargs):
        return None

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[1]
CONF_PATH = BASE_DIR / "config" / "config.yaml"
with open(CONF_PATH, "r") as f:
    CONF = yaml.safe_load(f)

FINNHUB_KEY = os.getenv("FINNHUB_API_KEY") or CONF.get("finnhub", {}).get("api_key")
ALPHAV_KEY = os.getenv("ALPHAV_API_KEY") or CONF.get("alpha_vantage", {}).get("api_key")
def is_placeholder(key: Optional[str]) -> bool:
    if not key:
        return True
    return key.lower().startswith("your_") or "replace" in key.lower()

PAIRS = CONF['general']['fx_pairs']
OUT_RAW = BASE_DIR / "data" / "raw"
OUT_RAW.mkdir(parents=True, exist_ok=True)

RESOLUTION = CONF['general'].get('intraday_interval', '60')
INTRADAY_DAYS = min(CONF['general'].get('intraday_outputsize_days', 30), 30)  # cap at 30 for free tier
MAX_CALLS_PER_MIN = 55
SLEEP_BETWEEN_CALLS = 60.0 / MAX_CALLS_PER_MIN

FINNHUB_CANDLES = "https://finnhub.io/api/v1/forex/candle"
ALPHAV_INTRADAY = "https://www.alphavantage.co/query"

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8), reraise=False)
def unix_ts(dt: datetime):
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)
    return int(dt.timestamp())

def fetch_candles_finnhub(symbol: str, resolution: str, _from_ts: int, _to_ts: int):
    params = {
        "symbol": symbol,
        "resolution": resolution,
        "from": _from_ts,
        "to": _to_ts,
        "token": FINNHUB_KEY
    }
    try:
        r = requests.get(FINNHUB_CANDLES, params=params, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[ERROR] Finnhub fetch {symbol} {resolution}: {e}")
        return None

def fetch_candles_alphavantage(pair: str, interval: str):
    symbol = f"{pair[:3]}/{pair[3:]}"
    params = {
        "function": "FX_INTRADAY",
        "from_symbol": pair[:3],
        "to_symbol": pair[3:],
        "interval": f"{interval}min",
        "apikey": ALPHAV_KEY,
        "outputsize": "full"
    }
    try:
        r = requests.get(ALPHAV_INTRADAY, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        key = list(data.keys())[1] if len(data) > 1 else None
        if not key:
            return None
        df = pd.DataFrame.from_dict(data[key], orient="index")
        df.index = pd.to_datetime(df.index)
        df.columns = ["open", "high", "low", "close"]
        df.sort_index(inplace=True)
        return df
    except Exception as e:
        print(f"[ERROR] Alpha Vantage fetch {pair}: {e}")
        return None

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8), reraise=False)
def yf_download_with_retry(ticker: str, *, start=None, end=None, period=None, interval=None) -> pd.DataFrame:
    import yfinance as yf
    return yf.download(
        ticker,
        start=start,
        end=end,
        period=period,
        interval=interval,
        auto_adjust=False,
        progress=False,
    )

def save_candles_json_to_df(j, out_path):
    if j is None or j.get("s") != "ok":
        return False
    df = pd.DataFrame({
        "timestamp": pd.to_datetime(j['t'], unit='s'),
        "open": j['o'],
        "high": j['h'],
        "low": j['l'],
        "close": j['c'],
        "volume": j.get('v', [None]*len(j['t']))
    })
    df = df.set_index('timestamp').sort_index()
    df.to_csv(out_path)
    return True

def fetch_daily(symbol, pair):
    start_date = CONF['general'].get('start_date')
    start_dt = datetime.fromisoformat(start_date)
    end_dt = datetime.now(UTC) if CONF['general'].get('end_date') is None else datetime.fromisoformat(CONF['general'].get('end_date'))
    from_ts = unix_ts(start_dt)
    to_ts = unix_ts(end_dt)
    print(f"Fetching DAILY for {pair} ({symbol}) from {start_dt.date()} to {end_dt.date()}")
    finnhub_tried = False
    if not is_placeholder(FINNHUB_KEY):
        finnhub_tried = True
        j = fetch_candles_finnhub(symbol, "D", from_ts, to_ts)
        ok = save_candles_json_to_df(j, out_path)
        if ok:
            print("Saved daily:", out_path)
            return
    # Yahoo fallback (with retry)
    try:
        ticker = f"{pair[:3]}{pair[3:]}=X"
        if finnhub_tried:
            print(f"Finnhub daily failed, falling back to Yahoo {ticker}")
        else:
            print(f"No Finnhub key, using Yahoo {ticker}")
        df = yf_download_with_retry(ticker, start=start_dt.date(), end=end_dt.date(), interval='1d')
        if df is not None and not df.empty:
            df.index = pd.to_datetime(df.index)
            df.to_csv(out_path)
            print("Saved fallback daily:", out_path)
        else:
            print("Yahoo fallback returned empty.")
    except Exception as e:
        print("Yahoo fallback failed:", e)

def fetch_intraday(symbol, pair):
    now = datetime.now(UTC)
    from_dt = now - timedelta(days=INTRADAY_DAYS)
    from_ts = unix_ts(from_dt)
    to_ts = unix_ts(now)
    print(f"Fetching INTRADAY ({RESOLUTION}m) for {pair} from {from_dt.date()} to {now.date()}")
    # Finnhub first if key available
    finnhub_tried = False
    if not is_placeholder(FINNHUB_KEY):
        finnhub_tried = True
        j = fetch_candles_finnhub(symbol, RESOLUTION, from_ts, to_ts)
        ok = save_candles_json_to_df(j, out_path)
        if ok:
            print("Saved intraday:", out_path)
            return
    # Alpha Vantage fallback if key available
    if not is_placeholder(ALPHAV_KEY):
        print(f"Finnhub intraday failed for {pair}, trying Alpha Vantage...")
        df = fetch_candles_alphavantage(pair, RESOLUTION)
        if df is not None and not df.empty:
            df.to_csv(out_path)
            print("Saved Alpha Vantage intraday:", out_path)
            return
    # Yahoo intraday fallback (hourly)
    try:
        print(f"Trying Yahoo intraday for {pair} at {RESOLUTION}m...")
        ticker = f"{pair[:3]}{pair[3:]}=X"
        # Yahoo supports limited lookback for intraday; use period
        period_days = INTRADAY_DAYS
        df = yf_download_with_retry(ticker, period=f"{period_days}d", interval=f"{RESOLUTION}m")
        if df is not None and not df.empty:
            df.index = pd.to_datetime(df.index)
            # yfinance provides OHLCV columns capitalized; normalize
            df = df.rename(columns={
                'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Adj Close': 'adj_close', 'Volume': 'volume'
            })
            df.to_csv(out_path)
            print("Saved Yahoo intraday:", out_path)
        else:
            print("Yahoo intraday returned empty.")
    except Exception as e:
        print("Yahoo intraday failed:", e)

def main():
    print("Starting Hybrid FX fetch...")
    for pair in tqdm(PAIRS):
        symbol = f"OANDA:{pair[:3]}_{pair[3:]}"
        fetch_daily(symbol, pair)
        time.sleep(SLEEP_BETWEEN_CALLS)
        fetch_intraday(symbol, pair)
        time.sleep(SLEEP_BETWEEN_CALLS)
    print("Done.")

if __name__ == "__main__":
    main()
