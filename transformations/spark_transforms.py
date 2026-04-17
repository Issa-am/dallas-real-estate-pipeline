"""
Dallas Real Estate Pipeline - PySpark Transformations
Demonstrates distributed data processing on listing data.
Author: Issa Amjadi
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def create_spark_session():
    """Create a local Spark session."""
    
    spark = SparkSession.builder \
        .appName("DallasRealEstate") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("Spark session created")
    return spark


def load_data(spark, filepath="data/dallas_listings_clean.csv"):
    """Load clean CSV into a Spark DataFrame."""
    
    df = spark.read.csv(filepath, header=True, inferSchema=True)
    print(f"Loaded {df.count()} rows with {len(df.columns)} columns")
    df.printSchema()
    return df


def add_price_rank_by_location(df):
    """Rank listings by price within each location using window functions."""
    
    window = Window.partitionBy("clean_location").orderBy("price")
    
    df = df.withColumn("price_rank_in_location", F.rank().over(window))
    df = df.withColumn("listings_in_location", F.count("*").over(
        Window.partitionBy("clean_location")
    ))
    
    print("Added price rank and listing count per location")
    return df


def compute_location_stats(df):
    """Aggregate statistics by location."""
    
    location_stats = df.groupBy("clean_location").agg(
        F.count("*").alias("total_listings"),
        F.round(F.avg("price"), 0).alias("avg_price"),
        F.min("price").alias("min_price"),
        F.max("price").alias("max_price"),
        F.round(F.stddev("price"), 0).alias("price_stddev"),
        F.round(F.avg("bedrooms"), 1).alias("avg_bedrooms")
    ).orderBy(F.desc("total_listings"))
    
    print("\n--- Location Statistics ---")
    location_stats.show(15, truncate=False)
    return location_stats


def compute_price_segments(df):
    """Create price segments using PySpark SQL expressions."""
    
    df = df.withColumn("price_segment",
        F.when(F.col("price") < 800, "Budget")
         .when(F.col("price") < 1200, "Mid-Range")
         .when(F.col("price") < 2000, "Premium")
         .otherwise("Luxury")
    )
    
    segment_summary = df.groupBy("price_segment").agg(
        F.count("*").alias("count"),
        F.round(F.avg("price"), 0).alias("avg_price"),
        F.round(F.avg("bedrooms"), 1).alias("avg_bedrooms")
    ).orderBy("avg_price")
    
    print("\n--- Price Segments ---")
    segment_summary.show()
    return df


def find_best_deals(df):
    """Find listings with below-average price for their bedroom count."""
    
    avg_by_beds = df.groupBy("bedrooms").agg(
        F.avg("price").alias("avg_price_for_beds")
    )
    
    df_with_avg = df.join(avg_by_beds, on="bedrooms", how="left")
    
    df_with_avg = df_with_avg.withColumn("price_vs_avg",
        F.round(F.col("price") - F.col("avg_price_for_beds"), 0)
    )
    
    deals = df_with_avg.filter(F.col("price_vs_avg") < 0) \
        .select("title", "price", "bedrooms", "clean_location", "price_vs_avg") \
        .orderBy("price_vs_avg")
    
    print("\n--- Best Deals (Below Average for Bedroom Count) ---")
    deals.show(10, truncate=50)
    return df_with_avg


def save_spark_output(df, location_stats, output_dir="data"):
    """Save PySpark results to CSV."""
    
    # Save enriched listings
    df.toPandas().to_csv(f"{output_dir}/dallas_listings_spark.csv", index=False)
    print(f"Enriched listings saved to {output_dir}/dallas_listings_spark.csv")
    
    # Save location stats
    location_stats.toPandas().to_csv(f"{output_dir}/location_stats.csv", index=False)
    print(f"Location stats saved to {output_dir}/location_stats.csv")


# ---- MAIN ----
if __name__ == "__main__":
    print("=" * 50)
    print("Dallas Real Estate - PySpark Transformations")
    print("=" * 50)
    
    # Create Spark session
    spark = create_spark_session()
    
    # Load data
    df = load_data(spark)
    
    # Transform
    df = add_price_rank_by_location(df)
    location_stats = compute_location_stats(df)
    df = compute_price_segments(df)
    df_deals = find_best_deals(df)
    
    # Save
    save_spark_output(df, location_stats)
    
    # Stop Spark
    spark.stop()
    print("\nDone! Spark session closed.")