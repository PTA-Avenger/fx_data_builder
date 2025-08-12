#!/usr/bin/env python3
import os
import re
import time
import yaml
import json
import math
import random
import logging
from typing import List, Dict, Optional
from datetime import datetime, timedelta, UTC

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser

from dotenv import load_dotenv

load_dotenv()

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
CONF_PATH = os.path.join(BASE_DIR, "config", "config.yaml")
OUT_RAW = os.path.join(BASE_DIR, "data", "raw")
os.makedirs(OUT_RAW, exist_ok=True)

with open(CONF_PATH) as f:
    CONF = yaml.safe_load(f)

PAIRS: List[str] = CONF['general']['fx_pairs']
START_DT = datetime.fromisoformat(CONF['general']['start_date'])
END_DT = datetime.now(UTC) if CONF['general']['end_date'] is None else datetime.fromisoformat(CONF['general']['end_date'])

# Pair keyword queries used for simple assignment
PAIR_QUERIES: Dict[str, str] = {
    "EURUSD": "EUR USD|Euro|ECB|European Central Bank|EURUSD|EUR-USD|EUR/USD",
    "GBPUSD": "GBP USD|Pound|Bank of England|BoE|GBPUSD|GBP-USD|GBP/USD",
    "USDJPY": "USD JPY|Yen|Bank of Japan|BoJ|USDJPY|USD-JPY|USD/JPY",
    "AUDUSD": "AUD USD|Australian Dollar|RBA|AUDUSD|AUD-USD|AUD/USD",
    "USDCAD": "USD CAD|Canadian Dollar|BoC|Bank of Canada|USDCAD|USD-CAD|USD/CAD",
    "USDCHF": "USD CHF|Swiss Franc|SNB|Swiss National Bank|USDCHF|USD-CHF|USD/CHF",
}

USER_AGENTS = [
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
]

HEADERS_BASE = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

REQUEST_TIMEOUT = 20
REQUEST_DELAY_SEC = 1.0  # politeness delay between requests
MAX_PAGES_PER_SOURCE = 10  # prevent runaway scraping

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger("scraper")


def http_get(url: str, params: Optional[dict] = None) -> Optional[requests.Response]:
    headers = HEADERS_BASE.copy()
    headers["User-Agent"] = random.choice(USER_AGENTS)
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 403:
            logger.warning(f"403 Forbidden for {url}. Site may be blocking automated requests.")
        resp.raise_for_status()
        return resp
    except Exception as e:
        logger.error(f"GET failed {url}: {e}")
        return None


def normalize_dt(text: str, now: Optional[datetime] = None) -> Optional[datetime]:
    if not text:
        return None
    if now is None:
        now = datetime.now(UTC)
    t = text.strip()

    # Handle relative times like "2 hours ago", "10 minutes ago", "Yesterday"
    lower = t.lower()
    rel_match = re.match(r"^(\d+)\s+(minute|minutes|hour|hours|day|days)\s+ago$", lower)
    if rel_match:
        num = int(rel_match.group(1))
        unit = rel_match.group(2)
        delta = None
        if unit.startswith("minute"):
            delta = timedelta(minutes=num)
        elif unit.startswith("hour"):
            delta = timedelta(hours=num)
        elif unit.startswith("day"):
            delta = timedelta(days=num)
        if delta is not None:
            return now - delta

    if lower == "yesterday":
        # Best effort: assume noon UTC yesterday
        y = (now - timedelta(days=1)).date()
        return datetime(y.year, y.month, y.day, 12, 0, tzinfo=UTC)

    try:
        dt = dateparser.parse(t)
        if dt is None:
            return None
        if dt.tzinfo is None:
            return dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except Exception:
        return None


def assign_pairs(title: str, desc: str) -> List[str]:
    hay = f"{title or ''} {desc or ''}".lower()
    matched: List[str] = []
    for pair, pattern in PAIR_QUERIES.items():
        if re.search(pattern, hay, flags=re.IGNORECASE):
            matched.append(pair)
    return matched


# ---------------- Reuters ----------------
REUTERS_BASE = "https://www.reuters.com"
REUTERS_CURRENCIES = f"{REUTERS_BASE}/markets/currencies/"


def scrape_reuters(start_dt: datetime, end_dt: datetime) -> List[Dict]:
    logger.info("Scraping Reuters currencies news ...")
    items: List[Dict] = []
    page = 1
    while page <= MAX_PAGES_PER_SOURCE:
        url = REUTERS_CURRENCIES if page == 1 else f"{REUTERS_CURRENCIES}?page={page}"
        resp = http_get(url)
        time.sleep(REQUEST_DELAY_SEC)
        if not resp:
            break
        soup = BeautifulSoup(resp.text, "lxml")

        # Try to find article cards; Reuters structure changes frequently.
        articles = soup.find_all("article")
        if not articles:
            # Fallback: look for generic links under markets/currencies
            articles = soup.select('a[href*="/markets/currencies/"]')

        found_any = False
        for art in articles:
            title = None
            link = None
            desc = None
            published = None

            # Common headline patterns
            h = art.find(["h2", "h3"]) if hasattr(art, 'find') else None
            if h and h.get_text(strip=True):
                title = h.get_text(strip=True)
                a = h.find("a")
                if a and a.get("href"):
                    href = a.get("href")
                    link = href if href.startswith("http") else f"{REUTERS_BASE}{href}"
            elif hasattr(art, 'get') and art.get('href'):
                title = art.get_text(strip=True)
                href = art.get('href')
                link = href if href.startswith("http") else f"{REUTERS_BASE}{href}"

            # Description/snippet
            p = art.find("p") if hasattr(art, 'find') else None
            if p:
                desc = p.get_text(strip=True)

            # Time
            time_tag = art.find("time") if hasattr(art, 'find') else None
            if time_tag and (time_tag.get("datetime") or time_tag.get_text(strip=True)):
                published = normalize_dt(time_tag.get("datetime") or time_tag.get_text(strip=True))

            if not title or not link:
                continue
            found_any = True

            # If no time found, skip or set None
            if published is None:
                # Try to infer from JSON-LD if present
                json_ld = art.find("script", type="application/ld+json") if hasattr(art, 'find') else None
                if json_ld and json_ld.string:
                    try:
                        data = json.loads(json_ld.string)
                        date_str = data.get("datePublished") or data.get("dateCreated")
                        published = normalize_dt(date_str)
                    except Exception:
                        pass

            if published is None:
                # As a last resort, skip items without time
                continue

            if published < start_dt.replace(tzinfo=UTC) or published > end_dt.astimezone(UTC):
                continue

            pairs = assign_pairs(title, desc or "")
            if not pairs:
                # Skip if we cannot associate to any FX pair
                continue

            for pair in pairs:
                items.append({
                    "pair": pair,
                    "source": "Reuters",
                    "title": title,
                    "description": desc,
                    "url": link,
                    "publishedAt": published.isoformat(),
                })
        if not found_any:
            break
        page += 1
    return items


# ---------------- Investing.com ----------------
INVESTING_BASE = "https://www.investing.com"
INVESTING_FOREX_NEWS = f"{INVESTING_BASE}/news/forex-news"


def scrape_investing(start_dt: datetime, end_dt: datetime) -> List[Dict]:
    logger.info("Scraping Investing.com forex news ...")
    items: List[Dict] = []
    page = 1
    while page <= MAX_PAGES_PER_SOURCE:
        url = INVESTING_FOREX_NEWS if page == 1 else f"{INVESTING_FOREX_NEWS}/{page}"
        resp = http_get(url)
        time.sleep(REQUEST_DELAY_SEC)
        if not resp:
            break
        soup = BeautifulSoup(resp.text, "lxml")

        # Article containers vary; try common selectors
        article_containers = soup.select("article, div.textDiv, div.largeTitle article, div.mediumTitle article")
        if not article_containers:
            # Fallback: anchor links under forex-news
            article_containers = soup.select('a[href*="/news/forex-news/"]')

        found_any = False
        for art in article_containers:
            title = None
            link = None
            desc = None
            published = None

            # Title and link
            h = None
            if hasattr(art, 'find'):
                h = art.find(["h1","h2","h3","a"])  # sometimes link is the title
            if h and h.get_text(strip=True):
                title = h.get_text(strip=True)
                a = h if h.name == "a" else h.find("a")
                if a and a.get("href"):
                    href = a.get("href")
                    link = href if href.startswith("http") else f"{INVESTING_BASE}{href}"
            elif hasattr(art, 'get') and art.get('href'):
                title = art.get_text(strip=True)
                href = art.get('href')
                link = href if href.startswith("http") else f"{INVESTING_BASE}{href}"

            # Description
            if hasattr(art, 'find'):
                p = art.find("p")
                if p:
                    desc = p.get_text(strip=True)

            # Time text appears in small tags or span with class indicating time
            time_el = None
            if hasattr(art, 'find'):
                time_el = art.find("time") or art.find("span", string=re.compile(r"ago|\d{4}"))
            if time_el:
                time_text = time_el.get("datetime") or time_el.get_text(strip=True)
                published = normalize_dt(time_text)

            if not title or not link or not published:
                continue

            if published < start_dt.replace(tzinfo=UTC) or published > end_dt.astimezone(UTC):
                continue

            pairs = assign_pairs(title, desc or "")
            if not pairs:
                continue

            found_any = True
            for pair in pairs:
                items.append({
                    "pair": pair,
                    "source": "Investing.com",
                    "title": title,
                    "description": desc,
                    "url": link,
                    "publishedAt": published.isoformat(),
                })
        if not found_any:
            break
        page += 1
    return items


def main():
    # Force UTC for boundaries
    start_dt_utc = START_DT if START_DT.tzinfo else START_DT.replace(tzinfo=UTC)
    end_dt_utc = END_DT if END_DT.tzinfo else END_DT.replace(tzinfo=UTC)

    all_rows: List[Dict] = []
    try:
        all_rows.extend(scrape_reuters(start_dt_utc, end_dt_utc))
    except Exception as e:
        logger.error(f"Reuters scrape failed: {e}")
    try:
        all_rows.extend(scrape_investing(start_dt_utc, end_dt_utc))
    except Exception as e:
        logger.error(f"Investing.com scrape failed: {e}")

    if not all_rows:
        logger.warning("No articles scraped. Sites may be blocking requests or structure changed.")

    import pandas as pd
    df = pd.DataFrame(all_rows)
    if not df.empty:
        df.sort_values(by=["publishedAt", "source"], inplace=True)
        out_file = os.path.join(OUT_RAW, f"news_scraped_{datetime.now(UTC).strftime('%Y%m%d')}.csv")
        df.to_csv(out_file, index=False)
        logger.info(f"Saved scraped news to: {out_file} ({len(df)} rows)")
    else:
        logger.info("Nothing to save.")


if __name__ == "__main__":
    main()