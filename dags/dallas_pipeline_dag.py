"""
Dallas Real Estate Pipeline - Airflow DAG
Orchestrates the full pipeline: scrape → clean → spark → load DB
Author: Issa Amjadi
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys
import os

# Add project root to path so we can import our modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ---- TASK FUNCTIONS ----

def run_scraper(**context):
    """Task 1: Scrape listings from Craigslist."""
    from scrapers.scraper import scrape_dallas_housing, save_data
    
    df = scrape_dallas_housing(pages=3)
    filepath = save_data(df)
    
    # Pass filepath to next task using XCom
    context['ti'].xcom_push(key='raw_filepath', value=filepath)
    print(f"Scraper complete: {len(df)} listings saved to {filepath}")


def run_cleaner(**context):
    """Task 2: Clean and transform raw data."""
    from transformations.clean_data import load_raw_data, clean_data, save_clean_data
    
    df = load_raw_data()
    df = clean_data(df)
    filepath = save_clean_data(df)
    
    context['ti'].xcom_push(key='clean_filepath', value=filepath)
    print(f"Cleaner complete: {len(df)} clean rows saved to {filepath}")


def run_spark_transforms(**context):
    """Task 3: Run PySpark transformations."""
    from transformations.spark_transforms import (
        create_spark_session, load_data, add_price_rank_by_location,
        compute_location_stats, compute_price_segments, 
        find_best_deals, save_spark_output
    )
    
    spark = create_spark_session()
    df = load_data(spark)
    df = add_price_rank_by_location(df)
    location_stats = compute_location_stats(df)
    df = compute_price_segments(df)
    df_deals = find_best_deals(df)
    save_spark_output(df, location_stats)
    spark.stop()
    print("Spark transforms complete")


def run_db_loader(**context):
    """Task 4: Load data into SQLite database."""
    from sql.load_data import create_database, load_dimension_tables, load_fact_table, run_sample_queries
    import pandas as pd
    
    df = pd.read_csv("data/dallas_listings_clean.csv")
    conn = create_database()
    load_dimension_tables(conn, df)
    load_fact_table(conn, df)
    run_sample_queries(conn)
    conn.close()
    print("Database loader complete")


def run_quality_checks(**context):
    """Task 5: Validate data quality."""
    import pandas as pd
    import sqlite3
    
    # Check 1: Clean CSV exists and has rows
    df = pd.read_csv("data/dallas_listings_clean.csv")
    assert len(df) > 0, "Clean CSV is empty!"
    print(f"✓ Clean CSV has {len(df)} rows")
    
    # Check 2: No null prices
    null_prices = df["price"].isna().sum()
    assert null_prices == 0, f"Found {null_prices} null prices!"
    print(f"✓ No null prices")
    
    # Check 3: Prices in valid range
    bad_prices = df[(df["price"] < 100) | (df["price"] > 20000)]
    assert len(bad_prices) == 0, f"Found {len(bad_prices)} out-of-range prices!"
    print(f"✓ All prices in valid range ($100 - $20,000)")
    
    # Check 4: Database has data
    conn = sqlite3.connect("data/dallas_real_estate.db")
    result = pd.read_sql_query("SELECT COUNT(*) as cnt FROM fact_listings", conn)
    assert result["cnt"][0] > 0, "Database fact table is empty!"
    print(f"✓ Database has {result['cnt'][0]} rows in fact_listings")
    conn.close()
    
    # Check 5: Spark output exists
    assert os.path.exists("data/dallas_listings_spark.csv"), "Spark output missing!"
    print(f"✓ Spark output exists")
    
    print("\nAll quality checks passed!")


# ---- DAG DEFINITION ----

default_args = {
    'owner': 'issa_amjadi',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dallas_real_estate_pipeline',
    default_args=default_args,
    description='End-to-end Dallas real estate data pipeline',
    schedule_interval='@daily',
    catchup=False,
    tags=['real-estate', 'dallas', 'etl'],
)

# ---- TASKS ----

task_scrape = PythonOperator(
    task_id='scrape_listings',
    python_callable=run_scraper,
    dag=dag,
)

task_clean = PythonOperator(
    task_id='clean_data',
    python_callable=run_cleaner,
    dag=dag,
)

task_spark = PythonOperator(
    task_id='spark_transforms',
    python_callable=run_spark_transforms,
    dag=dag,
)

task_load_db = PythonOperator(
    task_id='load_database',
    python_callable=run_db_loader,
    dag=dag,
)

task_quality = PythonOperator(
    task_id='quality_checks',
    python_callable=run_quality_checks,
    dag=dag,
)

# ---- PIPELINE ORDER ----
# scrape → clean → [spark, load_db] → quality_checks

task_scrape >> task_clean >> [task_spark, task_load_db] >> task_quality