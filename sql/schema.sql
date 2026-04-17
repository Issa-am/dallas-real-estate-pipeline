-- Dallas Real Estate Pipeline - Database Schema
-- Author: Issa Amjadi
-- Creates a star schema for apartment listings analysis

-- ============================================
-- DIMENSION TABLES
-- ============================================

CREATE TABLE IF NOT EXISTS dim_location (
    location_id INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_location TEXT,
    clean_location TEXT,
    city TEXT,
    state TEXT DEFAULT 'TX'
);

CREATE TABLE IF NOT EXISTS dim_property_type (
    property_type_id INTEGER PRIMARY KEY AUTOINCREMENT,
    bedrooms INTEGER,
    bathrooms INTEGER,
    property_label TEXT  -- 'Studio', '1BR/1BA', '2BR/2BA', etc.
);

CREATE TABLE IF NOT EXISTS dim_price_category (
    price_category_id INTEGER PRIMARY KEY AUTOINCREMENT,
    category_name TEXT,      -- Budget, Mid-Range, Premium, Luxury
    min_price INTEGER,
    max_price INTEGER
);

-- ============================================
-- FACT TABLE
-- ============================================

CREATE TABLE IF NOT EXISTS fact_listings (
    listing_id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT,
    price INTEGER,
    price_raw TEXT,
    location_id INTEGER,
    property_type_id INTEGER,
    price_category_id INTEGER,
    price_per_bedroom REAL,
    link TEXT,
    source TEXT,
    scraped_at TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
    FOREIGN KEY (property_type_id) REFERENCES dim_property_type(property_type_id),
    FOREIGN KEY (price_category_id) REFERENCES dim_price_category(price_category_id)
);

-- ============================================
-- ANALYTICAL VIEWS
-- ============================================

-- Average rent by location
CREATE VIEW IF NOT EXISTS v_avg_rent_by_location AS
SELECT 
    dl.clean_location,
    COUNT(*) as listing_count,
    ROUND(AVG(fl.price), 0) as avg_rent,
    MIN(fl.price) as min_rent,
    MAX(fl.price) as max_rent
FROM fact_listings fl
JOIN dim_location dl ON fl.location_id = dl.location_id
GROUP BY dl.clean_location
HAVING listing_count >= 3
ORDER BY avg_rent DESC;

-- Average rent by property type
CREATE VIEW IF NOT EXISTS v_avg_rent_by_type AS
SELECT
    dpt.property_label,
    dpt.bedrooms,
    COUNT(*) as listing_count,
    ROUND(AVG(fl.price), 0) as avg_rent
FROM fact_listings fl
JOIN dim_property_type dpt ON fl.property_type_id = dpt.property_type_id
GROUP BY dpt.property_label, dpt.bedrooms
ORDER BY dpt.bedrooms;

-- Price category distribution
CREATE VIEW IF NOT EXISTS v_price_distribution AS
SELECT
    dpc.category_name,
    dpc.min_price,
    dpc.max_price,
    COUNT(*) as listing_count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM fact_listings), 1) as percentage
FROM fact_listings fl
JOIN dim_price_category dpc ON fl.price_category_id = dpc.price_category_id
GROUP BY dpc.category_name
ORDER BY dpc.min_price;