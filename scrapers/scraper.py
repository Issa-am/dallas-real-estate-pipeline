"""
Dallas Real Estate Pipeline - Craigslist Housing Scraper
Scrapes Dallas-area housing listings from Craigslist.
Author: Issa Amjadi
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
from datetime import datetime


def scrape_dallas_housing(pages=3):
    """
    Scrape Dallas Craigslist housing listings.
    
    Args:
        pages: Number of pages to scrape (each page has ~120 listings)
    
    Returns:
        pandas DataFrame with all scraped listings
    """
    
    base_url = "https://dallas.craigslist.org/search/apa"
    all_listings = []
    
    for page in range(pages):
        offset = page * 120
        params = {"s": offset}
        
        print(f"Scraping page {page + 1} of {pages}...")
        
        try:
            # Send request with a realistic header
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
            response = requests.get(base_url, params=params, headers=headers, timeout=10)
            response.raise_for_status()
            
        except requests.RequestException as e:
            print(f"Error fetching page {page + 1}: {e}")
            continue
        
        # Parse the HTML
        soup = BeautifulSoup(response.text, "lxml")
        
        # Find all listing items
        listings = soup.find_all("li", class_="cl-static-search-result")
        
        if not listings:
            print(f"No listings found on page {page + 1}. Structure may have changed.")
            # Try alternate structure
            listings = soup.find_all("div", class_="result-info")
        
        for listing in listings:
            record = extract_listing(listing)
            if record:
                all_listings.append(record)
        
        print(f"  Found {len(listings)} listings on page {page + 1}")
        
        # Be respectful - wait between requests
        time.sleep(2)
    
    # Convert to DataFrame
    df = pd.DataFrame(all_listings)
    print(f"\nTotal listings scraped: {len(df)}")
    
    return df


def extract_listing(listing):
    """
    Extract data from a single listing element.
    
    Args:
        listing: BeautifulSoup element for one listing
    
    Returns:
        dict with listing data, or None if extraction fails
    """
    
    try:
        # Title
        title_tag = listing.find("div", class_="title") or listing.find("a", class_="titlestring")
        title = title_tag.get_text(strip=True) if title_tag else None
        
        # Price
        price_tag = listing.find("div", class_="price") or listing.find("span", class_="priceinfo")
        price_text = price_tag.get_text(strip=True) if price_tag else None
        price = clean_price(price_text)
        
        # Location
        location_tag = listing.find("div", class_="location") or listing.find("span", class_="meta")
        location = location_tag.get_text(strip=True) if location_tag else None
        
        # Link
        link_tag = listing.find("a", href=True)
        link = link_tag["href"] if link_tag else None
        
        return {
            "title": title,
            "price": price,
            "price_raw": price_text,
            "location": location,
            "link": link,
            "scraped_at": datetime.now().isoformat(),
            "source": "craigslist"
        }
        
    except Exception as e:
        print(f"Error extracting listing: {e}")
        return None


def clean_price(price_text):
    """
    Convert price string like '$1,500' to integer 1500.
    
    Args:
        price_text: raw price string from HTML
    
    Returns:
        int price or None if parsing fails
    """
    
    if not price_text:
        return None
    
    try:
        # Remove $, commas, and whitespace
        cleaned = price_text.replace("$", "").replace(",", "").strip()
        return int(cleaned)
    except ValueError:
        return None


def save_data(df, output_dir="data"):
    """
    Save scraped data to CSV with timestamp in filename.
    
    Args:
        df: pandas DataFrame to save
        output_dir: folder to save CSVs in
    """
    
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"dallas_listings_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)
    
    df.to_csv(filepath, index=False)
    print(f"Data saved to: {filepath}")
    
    return filepath


# ---- MAIN ----
if __name__ == "__main__":
    print("=" * 50)
    print("Dallas Real Estate Scraper")
    print("=" * 50)
    
    # Scrape listings
    df = scrape_dallas_housing(pages=3)
    
    if not df.empty:
        # Show preview
        print("\n--- Preview ---")
        print(df.head(10).to_string())
        
        # Basic stats
        print("\n--- Stats ---")
        print(f"Total listings: {len(df)}")
        print(f"Price range: ${df['price'].min():,.0f} - ${df['price'].max():,.0f}")
        print(f"Average price: ${df['price'].mean():,.0f}")
        print(f"Unique locations: {df['location'].nunique()}")
        
        # Save to CSV
        save_data(df)
    else:
        print("No data scraped. Check the website structure.")