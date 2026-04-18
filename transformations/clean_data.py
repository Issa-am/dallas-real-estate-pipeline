"""
Dallas Real Estate Pipeline - Data Cleaning
Cleans and enriches raw scraped listings.
Author: Issa Amjadi
"""

import pandas as pd
import re
import os
from datetime import datetime


def load_raw_data(data_dir="data"):
    """Load the most recent CSV from the data folder."""
    
    files = [f for f in os.listdir(data_dir) if f.startswith("dallas_listings_2") and f.endswith(".csv")]
    if not files:
        print("No CSV files found in data folder.")
        return None
    
    # Get the most recent file
    latest = sorted(files)[-1]
    filepath = os.path.join(data_dir, latest)
    
    df = pd.read_csv(filepath)
    print(f"Loaded {len(df)} rows from {latest}")
    return df


def remove_duplicates(df):
    """Remove duplicate listings based on title and price."""
    
    before = len(df)
    df = df.drop_duplicates(subset=["title", "price", "location"], keep="first").copy()
    after = len(df)
    print(f"Removed {before - after} duplicates. {after} rows remaining.")
    return df


def extract_bedrooms(title):
    """Pull bedroom count from listing title."""
    
    if not isinstance(title, str):
        return None
    
    match = re.search(r"(\d)\s*(bed|bd|br|bedroom)", title.lower())
    if match:
        return int(match.group(1))
    
    # Check for studio
    if "studio" in title.lower():
        return 0
    
    return None


def extract_bathrooms(title):
    """Pull bathroom count from listing title."""
    
    if not isinstance(title, str):
        return None
    
    match = re.search(r"(\d)\s*(bath|ba|bth|bathroom)", title.lower())
    if match:
        return int(match.group(1))
    return None


def clean_location(location):
    """Standardize location names."""
    
    if not isinstance(location, str):
        return "Unknown"
    
    # Remove extra whitespace
    location = location.strip()
    
    # Remove state abbreviation and zip if present
    location = re.sub(r",?\s*TX\s*\d*$", "", location)
    
    # Remove common suffixes
    location = location.strip(", ")
    
    if not location:
        return "Unknown"
    
    return location.title()


def add_price_category(price):
    """Categorize rent into budget tiers."""
    
    if pd.isna(price):
        return "Unknown"
    if price < 800:
        return "Budget"
    if price < 1200:
        return "Mid-Range"
    if price < 2000:
        return "Premium"
    return "Luxury"


def clean_data(df):
    """Master cleaning function - runs all cleaning steps."""
    
    print("\n--- Starting Data Cleaning ---")
    
    # Step 1: Remove duplicates
    df = remove_duplicates(df)
    
    # Step 2: Extract bedrooms and bathrooms from title
    df["bedrooms"] = df["title"].apply(extract_bedrooms)
    df["bathrooms"] = df["title"].apply(extract_bathrooms)
    print(f"Extracted bedrooms for {df['bedrooms'].notna().sum()} listings")
    print(f"Extracted bathrooms for {df['bathrooms'].notna().sum()} listings")
    
    # Step 3: Clean location names
    df["clean_location"] = df["location"].apply(clean_location)
    print(f"Standardized {df['clean_location'].nunique()} unique locations")
    
    # Step 4: Add price categories
    df["price_category"] = df["price"].apply(add_price_category)
    print(f"Price categories: {df['price_category'].value_counts().to_dict()}")
    
    # Step 5: Remove outliers (likely errors)
    before = len(df)
    df = df[(df["price"] >= 200) & (df["price"] <= 10000)]
    print(f"Removed {before - len(df)} price outliers")
    
    # Step 6: Add price per bedroom
    df["price_per_bedroom"] = df.apply(
        lambda row: round(row["price"] / row["bedrooms"]) 
        if row["bedrooms"] and row["bedrooms"] > 0 
        else None, axis=1
    )
    
    print(f"\n--- Cleaning Complete: {len(df)} rows ---")
    return df


def save_clean_data(df, output_dir="data"):
    """Save cleaned data."""
    
    filepath = os.path.join(output_dir, "dallas_listings_clean.csv")
    df.to_csv(filepath, index=False)
    print(f"Clean data saved to: {filepath}")
    return filepath


# ---- MAIN ----
if __name__ == "__main__":
    print("=" * 50)
    print("Dallas Real Estate - Data Cleaning")
    print("=" * 50)
    
    df = load_raw_data()
    
    if df is not None:
        df = clean_data(df)
        
        print("\n--- Preview ---")
        print(df[["title", "price", "bedrooms", "bathrooms", 
                   "clean_location", "price_category"]].head(10).to_string())
        
        print("\n--- Summary ---")
        print(f"Average rent by category:")
        print(df.groupby("price_category")["price"].mean().round(0))
        
        save_clean_data(df)