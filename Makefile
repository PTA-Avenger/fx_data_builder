PY?=python

.PHONY: fetch preprocess news datasets scrape all

fetch:
	$(PY) scripts/fetch_fx_data.py

news:
	$(PY) scripts/fetch_news.py

scrape:
	$(PY) scripts/scrape_news.py

preprocess:
	$(PY) scripts/preprocess_indicators.py

datasets:
	$(PY) scripts/generate_datasets.py

all: fetch news preprocess datasets