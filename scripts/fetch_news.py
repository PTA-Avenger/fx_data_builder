#!/usr/bin/env python3
"""
fetch_news.py (Hybrid edition)
Fetches:
- Recent news (<= 30 days) from NewsAPI
- Logs older date ranges for manual/scraping
"""

import os, yaml, time
from newsapi import NewsApiClient
import pandas as pd
from tqdm import tqdm
from datetime import datetime, timedelta, UTC
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
CONF_PATH = os.path.join(BASE_DIR, "config", "config.yaml")
with open(CONF_PATH) as f:
    CONF = yaml.safe_load(f)

NEWS_KEY = os.getenv("NEWSAPI_KEY") or CONF.get('newsapi', {}).get('api_key')
PAIRS = CONF['general']['fx_pairs']
OUT_RAW = os.path.join(BASE_DIR, "data", "raw")
os.makedirs(OUT_RAW, exist_ok=True)

PAIR_QUERIES = {
    "EURUSD": "EUR USD OR Euro Dollar OR ECB OR European Central Bank",
    "GBPUSD": "GBP USD OR Pound Dollar OR Bank of England",
    "USDJPY": "USD JPY OR Japan Bank OR BoJ OR Yen",
    "AUDUSD": "AUD USD OR AUDUSD OR Reserve Bank of Australia",
    "USDCAD": "USD CAD OR Bank of Canada OR CAD",
    "USDCHF": "USD CHF OR Swiss National Bank OR SNB OR Franc"
}

MAX_NEWSAPI_DAYS = 30

def fetch_news(query, from_dt, to_dt, page=1, page_size=100):
    api = NewsApiClient(api_key=NEWS_KEY)
    try:
        return api.get_everything(
            q=query,
            from_param=from_dt.isoformat(),
            to=to_dt.isoformat(),
            language='en',
            sort_by='relevancy',
            page=page,
            page_size=page_size
        )
    except Exception as e:
        print("NewsAPI error:", e)
        return None

def main():
    start_config = datetime.fromisoformat(CONF['general']['start_date'])
    end = datetime.now(UTC) if CONF['general']['end_date'] is None else datetime.fromisoformat(CONF['general']['end_date'])
    period_days = 7
    current = start_config
    all_rows = []
    print("Fetching news...")
    while current < end:
        next_dt = min(end, current + timedelta(days=period_days))
        if (datetime.now(UTC) - next_dt).days > MAX_NEWSAPI_DAYS:
            print(f"[SKIP] {current.date()} to {next_dt.date()} (outside NewsAPI free-tier range)")
            current = next_dt
            continue
        for pair, query in PAIR_QUERIES.items():
            res = fetch_news(query, current, next_dt, page=1)
            if res and res.get('articles'):
                for a in res['articles']:
                    row = {
                        'pair': pair,
                        'publishedAt': a.get('publishedAt'),
                        'source': a.get('source', {}).get('name'),
                        'title': a.get('title'),
                        'description': a.get('description'),
                        'url': a.get('url')
                    }
                    all_rows.append(row)
            time.sleep(1)
        current = next_dt
    df = pd.DataFrame(all_rows)
    out_file = os.path.join(OUT_RAW, f"news_{datetime.now(UTC).strftime('%Y%m%d')}.csv")
    df.to_csv(out_file, index=False)
    print("News saved to:", out_file)

if __name__ == "__main__":
    main()
