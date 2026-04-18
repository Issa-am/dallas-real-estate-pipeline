# Dallas Real Estate Data Pipeline

An end-to-end data engineering pipeline that scrapes, cleans, transforms, and loads Dallas-area apartment listings into a star schema database. Built with Python, PySpark, SQL, and Airflow, containerized with Docker.

## Architecture

```
Craigslist → Scraper (Beautiful Soup) → Raw CSV
    → Cleaner (Pandas) → Clean CSV
        → PySpark Transforms → Enriched CSV + Location Stats
        → SQL Loader → SQLite Star Schema Database
            → Quality Checks (pytest)

Orchestrated by: Apache Airflow DAG (daily schedule)
Containerized by: Docker
```

## Tech Stack

- **Python** — core language
- **Beautiful Soup** — web scraping from Craigslist Dallas
- **Pandas** — data cleaning, deduplication, feature extraction
- **PySpark** — distributed transformations, window functions, aggregations
- **SQL (SQLite)** — star schema data warehouse with fact/dimension tables
- **Apache Airflow** — pipeline orchestration with 5-task DAG
- **Docker** — containerization for reproducible execution
- **pytest** — 26 unit tests covering scraper, cleaner, and data quality

## Project Structure

```
dallas-real-estate-pipeline/
├── scrapers/
│   └── scraper.py              # Craigslist scraper (Beautiful Soup)
├── transformations/
│   ├── clean_data.py           # Pandas cleaning + feature extraction
│   └── spark_transforms.py     # PySpark window functions + aggregations
├── sql/
│   ├── schema.sql              # Star schema DDL (fact + 3 dimension tables)
│   └── load_data.py            # Database loader with foreign key lookups
├── dags/
│   └── dallas_pipeline_dag.py  # Airflow DAG with 5 tasks
├── tests/
│   └── test_pipeline.py        # 26 unit tests
├── data/                       # Raw + clean CSVs, SQLite database
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

## Pipeline Steps

**1. Scraping** — Collects 1,000+ apartment listings from Craigslist Dallas using Beautiful Soup. Extracts title, price, location, and link from each listing. Respects rate limits with 2-second delays between pages.

**2. Cleaning** — Removes 60-70% duplicate listings. Extracts bedroom and bathroom counts from titles using regex. Standardizes location names (strips state codes, zip codes, normalizes casing). Categorizes listings into Budget, Mid-Range, Premium, and Luxury tiers.

**3. PySpark Transforms** — Ranks listings by price within each location using window functions. Computes location-level statistics (avg price, std deviation, listing count). Identifies best deals by comparing each listing's price to the average for its bedroom count.

**4. SQL Star Schema** — Loads clean data into a star schema with one fact table (fact_listings) and three dimension tables (dim_location, dim_property_type, dim_price_category). Includes 4 analytical views for common queries.

**5. Quality Checks** — 26 unit tests validate price cleaning, bedroom extraction, location standardization, and overall data quality (no nulls, valid ranges, required columns).

## Key Findings

- Average rent in Dallas area: ~$1,300/month
- Price range: $269 - $4,242/month
- Most affordable areas: North DFW, Fort Worth
- Most expensive areas: Prosper, Turtle Creek, Uptown
- 44% of listings fall in the Mid-Range ($800-$1,200) category

## Quick Start

```bash
# Clone the repo
git clone https://github.com/Issa-am/dallas-real-estate-pipeline.git
cd dallas-real-estate-pipeline

# Run with Docker (recommended)
docker-compose up --build

# Or run manually
pip install -r requirements.txt
python scrapers/scraper.py
python transformations/clean_data.py
python transformations/spark_transforms.py
python sql/load_data.py

# Run tests
python -m pytest tests/test_pipeline.py -v
```

## Airflow DAG

The pipeline is orchestrated as a 5-task DAG that runs daily:

```
scrape_listings → clean_data → [spark_transforms, load_database] → quality_checks
```

Spark transforms and database loading run in parallel since they both depend on clean data but not on each other.

## Author

**Issa Amjadi** — Data Engineer based in Dallas, TX

- GitHub: [github.com/Issa-am](https://github.com/Issa-am)
- LinkedIn: [linkedin.com/in/issa-amjadi](https://linkedin.com/in/issa-amjadi)
