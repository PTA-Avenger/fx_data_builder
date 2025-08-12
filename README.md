# FX Data Pipeline

A small pipeline to fetch FX market data and news, compute indicators, and generate model-ready datasets.

## Quick start

1. Create and activate a virtual environment (recommended).
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Create a `.env` file from the template and add your API keys:

```bash
cp .env.example .env
# Edit .env and set FINNHUB_API_KEY, ALPHAV_API_KEY, NEWSAPI_KEY
```

4. Configure pairs and date ranges in `config/config.yaml`.

5. Run the pipeline:

```bash
make all
# or step-by-step
make fetch
make news
make preprocess
make datasets
```

## Notes
- API keys can be provided via environment variables (preferred) or in `config/config.yaml`.
- Intraday fetch is limited to the past 30 days for the free Finnhub tier; falls back to Alpha Vantage when needed.
- Output folders:
  - `data/raw`: raw candles and news
  - `data/processed`: candles with indicators
  - `data/model_ready`: aggregated datasets for modeling
- If building `ta-lib` is problematic, you can rely solely on `pandas_ta` (already used here).

## Security
- Do not commit real credentials. `.env` is ignored by git. If credentials were previously committed, rotate them immediately.