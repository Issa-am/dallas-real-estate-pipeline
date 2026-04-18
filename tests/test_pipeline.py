"""
Dallas Real Estate Pipeline - Unit Tests
Tests for scraper, cleaner, and data quality.
Author: Issa Amjadi
"""

import unittest
import pandas as pd
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scrapers.scraper import clean_price, extract_listing
from transformations.clean_data import (
    extract_bedrooms, extract_bathrooms, 
    clean_location, add_price_category
)


class TestCleanPrice(unittest.TestCase):
    """Test the price cleaning function."""
    
    def test_normal_price(self):
        self.assertEqual(clean_price("$1,500"), 1500)
    
    def test_no_comma(self):
        self.assertEqual(clean_price("$900"), 900)
    
    def test_none_input(self):
        self.assertIsNone(clean_price(None))
    
    def test_empty_string(self):
        self.assertIsNone(clean_price(""))
    
    def test_invalid_price(self):
        self.assertIsNone(clean_price("free"))


class TestExtractBedrooms(unittest.TestCase):
    """Test bedroom extraction from titles."""
    
    def test_two_bed(self):
        self.assertEqual(extract_bedrooms("2 bed 2 bath apartment"), 2)
    
    def test_one_br(self):
        self.assertEqual(extract_bedrooms("Nice 1br apartment"), 1)
    
    def test_three_bedroom(self):
        self.assertEqual(extract_bedrooms("3 bedroom house"), 3)
    
    def test_studio(self):
        self.assertEqual(extract_bedrooms("Studio apartment downtown"), 0)
    
    def test_no_bedroom_info(self):
        self.assertIsNone(extract_bedrooms("Great location near park"))
    
    def test_none_input(self):
        self.assertIsNone(extract_bedrooms(None))


class TestExtractBathrooms(unittest.TestCase):
    """Test bathroom extraction from titles."""
    
    def test_two_bath(self):
        self.assertEqual(extract_bathrooms("2 bed 2 bath apartment"), 2)
    
    def test_one_ba(self):
        self.assertEqual(extract_bathrooms("1bd 1ba near downtown"), 1)
    
    def test_no_bath_info(self):
        self.assertIsNone(extract_bathrooms("Nice apartment"))


class TestCleanLocation(unittest.TestCase):
    """Test location standardization."""
    
    def test_with_state(self):
        self.assertEqual(clean_location("Fort Worth, TX"), "Fort Worth")
    
    def test_with_zip(self):
        self.assertEqual(clean_location("Dallas, TX 75201"), "Dallas")
    
    def test_empty(self):
        self.assertEqual(clean_location(""), "Unknown")
    
    def test_none(self):
        self.assertEqual(clean_location(None), "Unknown")


class TestPriceCategory(unittest.TestCase):
    """Test price categorization."""
    
    def test_budget(self):
        self.assertEqual(add_price_category(500), "Budget")
    
    def test_mid_range(self):
        self.assertEqual(add_price_category(1000), "Mid-Range")
    
    def test_premium(self):
        self.assertEqual(add_price_category(1500), "Premium")
    
    def test_luxury(self):
        self.assertEqual(add_price_category(3000), "Luxury")


class TestDataQuality(unittest.TestCase):
    """Test that clean data meets quality standards."""
    
    def setUp(self):
        """Load clean data before each test."""
        self.df = pd.read_csv("data/dallas_listings_clean.csv")
    
    def test_not_empty(self):
        self.assertGreater(len(self.df), 0)
    
    def test_no_null_prices(self):
        self.assertEqual(self.df["price"].isna().sum(), 0)
    
    def test_prices_in_range(self):
        self.assertTrue((self.df["price"] >= 100).all())
        self.assertTrue((self.df["price"] <= 20000).all())
    
    def test_has_required_columns(self):
        required = ["title", "price", "location", "link", "source"]
        for col in required:
            self.assertIn(col, self.df.columns)


if __name__ == "__main__":
    unittest.main()