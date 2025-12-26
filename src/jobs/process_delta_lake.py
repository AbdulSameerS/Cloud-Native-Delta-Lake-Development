import sys
import os

# Conditional imports for AWS Glue (Cloud) vs Local Spark (Local)
try:
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.job import Job
    GLUE_AVAILABLE = True
except ImportError:
    GLUE_AVAILABLE = False
    print("AWS Glue libraries not found. Running in Local/Compat mode.")

from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, schema_of_json

# -------------------------------------------------------------------------
# SETUP: Context & Session
# -------------------------------------------------------------------------
if GLUE_AVAILABLE:
    # --- AWS GLUE ENV ---
    try:
        args = getResolvedOptions(sys.argv, ['JOB_NAME', 'raw_bucket', 'silver_bucket'])
        raw_bucket = args['raw_bucket']
        silver_bucket = args['silver_bucket']
        job_name = args['JOB_NAME']
    except Exception as e:
        # Fallback if arguments aren't passed even if glue lib is present
        raw_bucket = "data"
        silver_bucket = "data/silver"
        job_name = "local_test_with_glue_lib"

    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(job_name, args if 'args' in locals() else {})
else:
    # --- LOCAL ENV ---
    raw_bucket = "data"
    silver_bucket = "data/silver"
    job_name = "local_delta_demo"
    
    # Initialize Spark with Delta support
    spark = SparkSession.builder \
        .appName("LocalDeltaLake") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()
    
    # Validating Delta installation
    print("Spark Version:", spark.version)

print(f"Starting Job: {job_name}")
print(f"Reading from: {raw_bucket}, Writing to: {silver_bucket}")

# -------------------------------------------------------------------------
# 1. READ RAW DATA (Bronze Layer)
# -------------------------------------------------------------------------

# Read Orders (Structured CSV)
if raw_bucket.startswith("s3://") or raw_bucket.startswith("data"):
    # If S3, construct path. If local 'data', use relative.
    orders_path = f"s3://{raw_bucket}/orders.csv" if raw_bucket.startswith("s3") else f"{raw_bucket}/orders.csv"
    reviews_path = f"s3://{raw_bucket}/reviews.json" if raw_bucket.startswith("s3") else f"{raw_bucket}/reviews.json"
else:
    # Just in case bucket is just the name
    orders_path = f"s3://{raw_bucket}/orders.csv"
    reviews_path = f"s3://{raw_bucket}/reviews.json"

print(f"Reading orders from {orders_path}")
try:
    df_orders = spark.read.option("header", "true").option("inferSchema", "true").csv(orders_path)
    df_orders.show(5)
except Exception as e:
    print(f"Error reading orders: {e}")
    # Create empty DF to prevent crash if file missing in dev
    df_orders = spark.createDataFrame([], schema="order_id string, user_id string, product_id string, quantity int, order_date string, total_amount double")

print(f"Reading reviews from {reviews_path}")
try:
    df_reviews = spark.read.json(reviews_path)
    df_reviews.show(5)
except Exception as e:
    print(f"Error reading reviews: {e}")
    df_reviews = spark.createDataFrame([], schema="review_id string, product_id string, user_id string, rating int, review_text string, timestamp string")


# -------------------------------------------------------------------------
# 2. TRANSFORM (Silver Layer)
# -------------------------------------------------------------------------

# Enrich Orders: Add ingestion timestamp
df_orders_silver = df_orders.withColumn("ingestion_timestamp", current_timestamp())

# Clean Reviews: Filter out bad ratings? Or just standardize.
# Let's say we only want valid ratings 1-5
df_reviews_silver = df_reviews.filter((col("rating") >= 1) & (col("rating") <= 5)) \
                              .withColumn("ingestion_timestamp", current_timestamp())

# -------------------------------------------------------------------------
# 3. WRITE TO DELTA LAKE (ACID Compliance)
# -------------------------------------------------------------------------
# Enable Delta support if available (Glue 4.0 has it native)

orders_delta_path = f"s3://{silver_bucket}/orders_delta" if "s3" in silver_bucket else f"{silver_bucket}/orders_delta"
reviews_delta_path = f"s3://{silver_bucket}/reviews_delta" if "s3" in silver_bucket else f"{silver_bucket}/reviews_delta"

# Check if Delta exists (Upsert logic)
# Note: For simple demo we usually overwrite or append. 
# Here is the MERGE pattern (Hypothetical if table exists)

# Writing Orders
print(f"Writing Orders to Delta Table at {orders_delta_path}")
df_orders_silver.write.format("delta").mode("overwrite").save(orders_delta_path)

# Writing Reviews
print(f"Writing Reviews to Delta Table at {reviews_delta_path}")
df_reviews_silver.write.format("delta").mode("overwrite").save(reviews_delta_path)

# Example: Simple Aggregation for Gold (Product Sentiment)
# Join orders and reviews on product_id
print("Creating Gold Layer Aggregates...")
df_joined = df_orders_silver.join(df_reviews_silver, "product_id", "inner")

df_gold = df_joined.groupBy("product_id") \
    .agg({"rating": "avg", "total_amount": "sum"}) \
    .withColumnRenamed("avg(rating)", "average_rating") \
    .withColumnRenamed("sum(total_amount)", "total_sales")

df_gold.show(5)

# Write Gold
gold_path = orders_delta_path.replace("silver", "gold").replace("orders_delta", "product_stats")
if "gold" not in gold_path and "silver" in silver_bucket: 
    # fix path hacking for local
    gold_path = silver_bucket.replace("silver", "gold") + "/product_stats"

print(f"Writing Gold Analytics to {gold_path}")
# In a real scenario, this might write to Redshift via JDBC
# df_gold.write.format("jdbc").option("url", redshift_url)...
df_gold.write.format("delta").mode("overwrite").save(gold_path)

if GLUE_AVAILABLE:
    job.commit()
else:
    print("Local job Run Complete")
