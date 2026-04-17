"""
Dallas Real Estate Pipeline - Database Loader
Loads clean CSV data into SQLite database with star schema.
Author: Issa Amjadi
"""

import sqlite3
import pandas as pd
import os


def create_database(db_path="data/dallas_real_estate.db"):
    """Create the database and tables from schema.sql."""
    
    conn = sqlite3.connect(db_path)
    
    # Read and execute schema
    with open("sql/schema.sql", "r") as f:
        schema = f.read()
    
    conn.executescript(schema)
    conn.commit()
    print(f"Database created at: {db_path}")
    return conn


def load_dimension_tables(conn, df):
    """Populate dimension tables from clean data."""
    
    cursor = conn.cursor()
    
    # --- dim_location ---
    locations = df[["location", "clean_location"]].drop_duplicates()
    for _, row in locations.iterrows():
        # Extract city (first part before comma)
        city = row["clean_location"].split(",")[0].strip() if pd.notna(row["clean_location"]) else "Unknown"
        
        cursor.execute("""
            INSERT OR IGNORE INTO dim_location (raw_location, clean_location, city)
            VALUES (?, ?, ?)
        """, (row["location"], row["clean_location"], city))
    
    print(f"Loaded {len(locations)} locations")
    
    # --- dim_property_type ---
    property_types = df[["bedrooms", "bathrooms"]].drop_duplicates()
    for _, row in property_types.iterrows():
        beds = int(row["bedrooms"]) if pd.notna(row["bedrooms"]) else None
        baths = int(row["bathrooms"]) if pd.notna(row["bathrooms"]) else None
        
        if beds == 0:
            label = "Studio"
        elif beds is not None and baths is not None:
            label = f"{beds}BR/{baths}BA"
        elif beds is not None:
            label = f"{beds}BR"
        else:
            label = "Unknown"
        
        cursor.execute("""
            INSERT OR IGNORE INTO dim_property_type (bedrooms, bathrooms, property_label)
            VALUES (?, ?, ?)
        """, (beds, baths, label))
    
    print(f"Loaded {len(property_types)} property types")
    
    # --- dim_price_category ---
    categories = [
        ("Budget", 0, 799),
        ("Mid-Range", 800, 1199),
        ("Premium", 1200, 1999),
        ("Luxury", 2000, 10000)
    ]
    for name, min_p, max_p in categories:
        cursor.execute("""
            INSERT OR IGNORE INTO dim_price_category (category_name, min_price, max_price)
            VALUES (?, ?, ?)
        """, (name, min_p, max_p))
    
    print(f"Loaded {len(categories)} price categories")
    
    conn.commit()


def load_fact_table(conn, df):
    """Load listings into the fact table with foreign key lookups."""
    
    cursor = conn.cursor()
    loaded = 0
    
    for _, row in df.iterrows():
        # Look up location_id
        cursor.execute(
            "SELECT location_id FROM dim_location WHERE raw_location = ?",
            (row["location"],)
        )
        loc_result = cursor.fetchone()
        location_id = loc_result[0] if loc_result else None
        
        # Look up property_type_id
        beds = int(row["bedrooms"]) if pd.notna(row["bedrooms"]) else None
        baths = int(row["bathrooms"]) if pd.notna(row["bathrooms"]) else None
        cursor.execute(
            "SELECT property_type_id FROM dim_property_type WHERE bedrooms IS ? AND bathrooms IS ?",
            (beds, baths)
        )
        prop_result = cursor.fetchone()
        property_type_id = prop_result[0] if prop_result else None
        
        # Look up price_category_id
        cursor.execute(
            "SELECT price_category_id FROM dim_price_category WHERE ? >= min_price AND ? <= max_price",
            (row["price"], row["price"])
        )
        cat_result = cursor.fetchone()
        price_category_id = cat_result[0] if cat_result else None
        
        # Insert into fact table
        cursor.execute("""
            INSERT INTO fact_listings 
            (title, price, price_raw, location_id, property_type_id, 
             price_category_id, price_per_bedroom, link, source, scraped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row["title"], row["price"], row["price_raw"],
            location_id, property_type_id, price_category_id,
            row.get("price_per_bedroom"), row["link"], 
            row["source"], row["scraped_at"]
        ))
        loaded += 1
    
    conn.commit()
    print(f"Loaded {loaded} listings into fact_listings")


def run_sample_queries(conn):
    """Run the analytical views to show the data works."""
    
    print("\n" + "=" * 50)
    print("SAMPLE QUERIES")
    print("=" * 50)
    
    # Query 1: Average rent by location
    print("\n--- Top 10 Locations by Average Rent ---")
    df = pd.read_sql_query("SELECT * FROM v_avg_rent_by_location LIMIT 10", conn)
    print(df.to_string(index=False))
    
    # Query 2: Rent by property type
    print("\n--- Average Rent by Property Type ---")
    df = pd.read_sql_query("SELECT * FROM v_avg_rent_by_type", conn)
    print(df.to_string(index=False))
    
    # Query 3: Price distribution
    print("\n--- Price Distribution ---")
    df = pd.read_sql_query("SELECT * FROM v_price_distribution", conn)
    print(df.to_string(index=False))
    
    # Query 4: Custom query - cheapest 2BR apartments
    print("\n--- Top 5 Cheapest 2-Bedroom Apartments ---")
    query = """
        SELECT fl.title, fl.price, dl.clean_location
        FROM fact_listings fl
        JOIN dim_location dl ON fl.location_id = dl.location_id
        JOIN dim_property_type dpt ON fl.property_type_id = dpt.property_type_id
        WHERE dpt.bedrooms = 2
        ORDER BY fl.price ASC
        LIMIT 5
    """
    df = pd.read_sql_query(query, conn)
    print(df.to_string(index=False))


# ---- MAIN ----
if __name__ == "__main__":
    print("=" * 50)
    print("Dallas Real Estate - Database Loader")
    print("=" * 50)
    
    # Load clean data
    df = pd.read_csv("data/dallas_listings_clean.csv")
    print(f"Read {len(df)} rows from clean CSV")
    
    # Create database and load
    conn = create_database()
    load_dimension_tables(conn, df)
    load_fact_table(conn, df)
    
    # Run sample queries
    run_sample_queries(conn)
    
    conn.close()
    print("\nDone! Database saved to data/dallas_real_estate.db")