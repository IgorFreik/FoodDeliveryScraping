# Food Delivery Scraper

A production-grade web scraping pipeline that collects restaurant listings from **UberEats**, **DoorDash**, **Grubhub**, and **JustEatTakeaway (Thuisbezorgd)**. The pipeline is orchestrated by Apache Airflow, stores data in PostgreSQL + PostGIS, archives raw HTML and parsed JSON in MinIO (S3-compatible), streams events through Kafka, and monitors everything with Prometheus and Grafana.

---

## Demo

### Live headed scraping

The scraper uses a Playwright-controlled Chromium browser. When run locally, the browser is visible — here it is scraping UberEats Amsterdam in real time, extracting 1,000+ merchant listings per run.

![Live scraping demo](docs/demo_scraping.gif)
### Airflow, Grafana, MinIO, Prometheus
A walkthrough of the full infrastructure: Airflow DAG scheduling per platform, the Grafana merchant coverage dashboard, MinIO object storage buckets, and Prometheus service health monitoring.

![Airflow, Grafana, MinIO, Prometheus stack](docs/demo_services.gif)
---

## Architecture

```
Playwright (headless / headed)
        |
        v
  BaseScraper (retry, rate-limit, stealth, proxy)
        |
        +---> listings scrape
        |         |
        |         v
        |   parse HTML / embedded JSON  ---> MerchantListing model
        |         |
        +---> detail scrape (address, menu, geo)
                  |
                  v
         Kafka topic: raw-html-scraped
                  |
                  v
         MinIO: raw-html / parsed-json buckets
                  |
                  v
         PostgreSQL + PostGIS (upsert)
                  |
                  v
         Prometheus metrics --> Grafana dashboards
```

Airflow DAGs run on a weekly schedule and can be triggered manually per platform and market.

---

## Platforms and markets

| Platform | Markets |
|---|---|
| UberEats | Amsterdam, New York City |
| JustEatTakeaway | Amsterdam |
| DoorDash | New York City |
| Grubhub | New York City |

Markets and their geo-coordinates are configured in `config/markets.yaml`.

---

## Stack

| Component | Technology |
|---|---|
| Orchestration | Apache Airflow 2.x |
| Browser automation | Playwright (Chromium) |
| Proxy / browser | Bright Data (optional) |
| Database | PostgreSQL 16 + PostGIS |
| Object storage | MinIO (S3-compatible) |
| Event streaming | Apache Kafka (KRaft mode) |
| Monitoring | Prometheus + Grafana |
| Language | Python 3.9+ |

---

## Getting started

### Prerequisites

- Docker and Docker Compose
- Python 3.9+

### 1. Clone and configure

```bash
git clone <repo-url>
cd ScrapingUber
cp .env.example .env
# Edit .env to set passwords and optional API keys
```

### 2. Start all services

```bash
docker compose up -d
```

This starts PostgreSQL, Kafka, MinIO, Airflow (webserver + scheduler), parser-consumer (Kafka → parse → DB), Prometheus, and Grafana.

| Service | URL | Default credentials |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| Grafana | http://localhost:3001 | admin / admin |
| MinIO | http://localhost:9001 | minioadmin / minioadmin123 |
| Prometheus | http://localhost:9090 | — |

### 3. Install Python dependencies (for local runs)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
# Or: pip install -r requirements.txt
playwright install chromium
```

### 4. Run a scraper locally (non-headless)

```bash
python scripts/run_local_ubereats.py        # UberEats Amsterdam, browser visible
python scripts/run_local_justeattakeaway.py # JustEatTakeaway Amsterdam
```

Results are saved to `local_storage/`.

### 5. Trigger a DAG manually

Log in to Airflow at http://localhost:8080, find the DAG you want (e.g. `scrape_ubereats`), and click the play button, or use the CLI:

```bash
docker compose exec airflow-webserver airflow dags trigger scrape_ubereats
```

---

## Project structure

```
.
├── scrapers/
│   ├── base.py                  # Abstract base: retry, proxy, rate-limit, archival
│   ├── ubereats/listing.py      # UberEats listing + detail scraper
│   ├── justeattakeaway/         # JustEatTakeaway scraper
│   ├── doordash/                # DoorDash scraper
│   └── grubhub/                 # Grubhub scraper
├── processing/
│   ├── parser.py                # HTML/JSON parsers for each platform
│   ├── normalizer.py            # Normalizes raw listings into a unified model
│   └── models.py                # Pydantic data models
├── dags/
│   ├── scrape_ubereats.py       # Airflow DAG for UberEats
│   ├── scrape_doordash.py       # Airflow DAG for DoorDash
│   ├── scrape_grubhub.py        # Airflow DAG for Grubhub
│   ├── scrape_justeattakeaway.py
│   ├── resolve_entities.py     # Entity resolution DAG
│   └── snapshot_merchant_counts.py  # Daily merchant count snapshots for Grafana
├── storage/
│   ├── db.py                    # SQLAlchemy models and session management
│   ├── minio_client.py          # MinIO upload helpers
│   ├── init_db.sql              # PostgreSQL + PostGIS schema
│   └── migrations/              # SQL migrations (e.g. platform_merchant_daily_counts)
├── streaming/
│   ├── producer.py              # Kafka event publisher
│   └── parser_consumer.py       # Kafka consumer that parses raw HTML
├── analytics/
│   ├── coverage_report.py       # Per-market, per-platform coverage report
│   └── data_quality.py          # Data quality metrics (completeness, freshness, geo)
├── config/
│   ├── markets.yaml             # Market slugs, coordinates, and zip codes
│   ├── platforms.yaml           # Per-platform scraping configuration
│   ├── prometheus.yml           # Prometheus scrape config
│   └── grafana/                 # Grafana dashboard provisioning
├── tests/
│   ├── test_parser.py            # DoorDash, Grubhub parsers
│   ├── test_parser_ubereats.py   # Uber Eats parser
│   ├── test_parser_justeattakeaway.py  # JustEatTakeaway parser
│   ├── test_entity_resolution.py
│   └── test_data_quality.py
├── examples/                     # Integration scripts (require DB/MinIO/network)
├── docs/                         # Media assets for README
├── docker-compose.yml
├── Dockerfile.airflow
└── requirements.txt
```

---

## Configuration

### Markets (`config/markets.yaml`)

Add or remove markets by editing the `markets` list. Each entry requires a `slug`, human-readable `name`, center coordinates (`lat`, `lng`), and an optional list of `zips`.

### Proxy (`config/platforms.yaml` and `.env`)

Set `BRIGHTDATA_PROXY_HOST`, `BRIGHTDATA_PROXY_USER`, and `BRIGHTDATA_PROXY_PASS` in `.env` to route traffic through Bright Data. Alternatively, set `BRIGHTDATA_BROWSER_URL` to use the Scraping Browser (remote Playwright via CDP). Both are optional — the scraper runs direct if these variables are blank.

---

## Data model

Each scraped merchant is normalized into a `MerchantListing` and upserted into the `platform_merchants` table:

| Field | Type | Description |
|---|---|---|
| `platform` | text | `ubereats`, `doordash`, etc. |
| `platform_id` | text | Platform-native merchant ID |
| `name` | text | Restaurant name |
| `address` | text | Street address |
| `geom` | geometry | PostGIS point (SRID 4326) |
| `cuisine_tags` | text[] | Cuisine categories |
| `rating` | float | Average star rating |
| `review_count` | int | Number of reviews |
| `price_bucket` | text | `$`, `$$`, `$$$` |
| `delivery_fee` | float | Delivery fee in local currency |
| `estimated_delivery_min` | int | Estimated delivery minutes |
| `is_promoted` | bool | Sponsored / promoted listing flag |
| `market` | text | Market slug (e.g. `amsterdam`) |
| `raw_url` | text | Original listing URL |
| `scraped_at` | timestamp | Last scrape timestamp |
